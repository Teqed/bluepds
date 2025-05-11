//! Repository transactor for the actor store.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use rsky_syntax::aturi::AtUri;
use std::sync::Arc;

use crate::{
    SigningKey,
    actor_store::{
        ActorDb,
        blob::{BackgroundQueue, BlobTransactor},
        record::RecordTransactor,
        repo::{reader::RepoReader, sql_repo_transactor::SqlRepoTransactor},
        resources::ActorStoreResources,
    },
    repo::{
        Repo,
        block_map::BlockMap,
        types::{
            BlobStore, CommitAction, CommitDataWithOps, CommitOp, PreparedCreate, PreparedWrite,
            WriteOpAction, format_data_key,
        },
    },
};

/// Repository transactor for the actor store.
pub(crate) struct RepoTransactor {
    /// The inner reader.
    pub reader: RepoReader,
    /// BlobTransactor for handling blobs.
    pub blob: BlobTransactor,
    /// RecordTransactor for handling records.
    pub record: RecordTransactor,
    /// SQL repository transactor.
    pub storage: SqlRepoTransactor,
}

impl RepoTransactor {
    /// Create a new repository transactor.
    pub(crate) fn new(
        db: ActorDb,
        blobstore: BlobStore,
        did: String,
        signing_key: Arc<SigningKey>,
        background_queue: BackgroundQueue,
        now: Option<String>,
    ) -> Self {
        // Create a new RepoReader
        let reader = RepoReader::new(db.clone(), blobstore.clone(), did.clone(), signing_key);

        // Create a new BlobTransactor
        let blob = BlobTransactor::new(db.clone(), blobstore.clone(), background_queue);

        // Create a new RecordTransactor
        let record = RecordTransactor::new(db.clone(), blobstore);

        // Create a new SQL repository transactor
        let storage = SqlRepoTransactor::new(db, did.clone(), now);

        Self {
            reader,
            blob,
            record,
            storage,
        }
    }

    /// Try to load a repository.
    pub(crate) async fn maybe_load_repo(&self) -> Result<Option<Repo>> {
        // Query the repository root
        let root = sqlx::query!("SELECT cid FROM repo_root LIMIT 1")
            .fetch_optional(&self.db.pool)
            .await
            .context("failed to query repo root")?;

        // If found, load the repo
        if let Some(row) = root {
            let cid = Cid::try_from(&row.cid)?;
            let repo = Repo::load(&self.storage, cid).await?;
            Ok(Some(repo))
        } else {
            Ok(None)
        }
    }

    /// Create a new repository with the given writes.
    pub(crate) async fn create_repo(
        &self,
        writes: Vec<PreparedCreate>,
    ) -> Result<CommitDataWithOps> {
        // Assert we're in a transaction
        self.db.assert_transaction()?;

        // Convert writes to operations
        let ops = writes.iter().map(|w| create_write_to_op(w)).collect();

        // Format the initial commit
        let commit =
            Repo::format_init_commit(&self.storage, &self.did, &self.signing_key, ops).await?;

        // Apply the commit, index the writes, and process blobs in parallel
        let results = futures::future::join3(
            self.storage.apply_commit(&commit, true),
            self.index_writes(&writes, &commit.rev),
            self.blob.process_write_blobs(&commit.rev, &writes),
        )
        .await;

        // Check for errors in each parallel task
        results.0?;
        results.1?;
        results.2?;

        // Create commit operations
        let ops = writes
            .iter()
            .map(|w| CommitOp {
                action: CommitAction::Create,
                path: format_data_key(&w.uri.collection, &w.uri.rkey),
                cid: Some(w.cid),
                prev: None,
            })
            .collect();

        // Return the commit data with operations
        Ok(CommitDataWithOps {
            commit_data: commit,
            ops,
            prev_data: None,
        })
    }

    /// Process writes to the repository.
    pub(crate) async fn process_writes(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Assert we're in a transaction
        self.db.assert_transaction()?;

        // Check write limit
        if writes.len() > 200 {
            return Err(anyhow::anyhow!("Too many writes. Max: 200"));
        }

        // Format the commit
        let commit = self.format_commit(writes, swap_commit_cid).await?;

        // Check commit size limit (2MB)
        if commit.commit_data.relevant_blocks.byte_size()? > 2_000_000 {
            return Err(anyhow::anyhow!("Too many writes. Max event size: 2MB"));
        }

        // Apply the commit, index the writes, and process blobs in parallel
        let results = futures::future::join3(
            self.storage.apply_commit(&commit.commit_data, false),
            self.index_writes(writes, &commit.commit_data.rev),
            self.blob
                .process_write_blobs(&commit.commit_data.rev, writes),
        )
        .await;

        // Check for errors in each parallel task
        results.0?;
        results.1?;
        results.2?;

        Ok(commit)
    }

    /// Format a commit for the given writes.
    pub(crate) async fn format_commit(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Get the current root
        let curr_root = self.storage.get_root_detailed().await?;
        if curr_root.is_none() {
            return Err(anyhow::anyhow!("No repo root found for {}", self.did));
        }

        let curr_root = curr_root.unwrap();

        // Check commit swap if requested
        if let Some(swap) = swap_commit {
            if curr_root.cid != swap {
                return Err(anyhow::anyhow!(
                    "Bad commit swap: current={}, expected={}",
                    curr_root.cid,
                    swap
                ));
            }
        }

        // Cache the revision for better performance
        self.storage.cache_rev(&curr_root.rev).await?;

        // Prepare collections for tracking changes
        let mut new_record_cids = Vec::new();
        let mut del_and_update_uris = Vec::new();
        let mut commit_ops = Vec::new();

        // Process each write
        for write in writes {
            let action = &write.action;
            let uri = &write.uri;
            let swap_cid = write.swap_cid;

            // Track new record CIDs
            if *action != WriteOpAction::Delete {
                new_record_cids.push(write.cid);
            }

            // Track deleted/updated URIs
            if *action != WriteOpAction::Create {
                del_and_update_uris.push(uri.clone());
            }

            // Get the current record if it exists
            let record = self.record.get_record(uri, None, true).await?;
            let curr_record = record.as_ref().map(|r| Cid::try_from(&r.cid).unwrap());

            // Create commit operation
            let mut op = CommitOp {
                action: action.clone(),
                path: format_data_key(&uri.collection, &uri.rkey),
                cid: if *action == WriteOpAction::Delete {
                    None
                } else {
                    Some(write.cid)
                },
                prev: curr_record,
            };

            commit_ops.push(op);

            // Validate swap_cid conditions
            if swap_cid.is_some() {
                match action {
                    WriteOpAction::Create if swap_cid != Some(None) => {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: there should be no current record for a create"
                        ));
                    }
                    WriteOpAction::Update if swap_cid == Some(None) => {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: there should be a current record for an update"
                        ));
                    }
                    WriteOpAction::Delete if swap_cid == Some(None) => {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: there should be a current record for a delete"
                        ));
                    }
                    _ => {}
                }

                if let Some(Some(swap)) = swap_cid {
                    if curr_record.is_some() && curr_record != Some(swap) {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: current={:?}, expected={}",
                            curr_record,
                            swap
                        ));
                    }
                }
            }
        }

        // Load the repo
        let repo = Repo::load(&self.storage, curr_root.cid).await?;
        let prev_data = repo.commit.data.clone();

        // Convert writes to ops
        let write_ops = writes.iter().map(|w| write_to_op(w)).collect();

        // Format the commit
        let commit = repo.format_commit(write_ops, &self.signing_key).await?;

        // Find blocks that would be deleted but are referenced by another record
        let dupe_record_cids = self
            .get_duplicate_record_cids(&commit.removed_cids.to_list(), &del_and_update_uris)
            .await?;

        // Remove duplicates from removed_cids
        for cid in &dupe_record_cids {
            commit.removed_cids.delete(*cid);
        }

        // Find blocks that are relevant to ops but not included in diff
        let new_record_blocks = commit.relevant_blocks.get_many(&new_record_cids)?;
        if !new_record_blocks.missing.is_empty() {
            let missing_blocks = self
                .storage
                .reader
                .get_blocks(&new_record_blocks.missing)
                .await?;
            commit.relevant_blocks.add_map(missing_blocks.blocks)?;
        }

        Ok(CommitDataWithOps {
            commit_data: commit,
            ops: commit_ops,
            prev_data: Some(prev_data),
        })
    }

    /// Index writes to the database.
    pub(crate) async fn index_writes(&self, writes: &[PreparedWrite], rev: &str) -> Result<()> {
        // Assert we're in a transaction
        self.db.assert_transaction()?;

        // Process each write in parallel
        let futures = writes.iter().map(|write| async move {
            match write.action {
                WriteOpAction::Create | WriteOpAction::Update => {
                    self.record
                        .index_record(
                            &write.uri,
                            &write.cid,
                            &write.record,
                            &write.action,
                            rev,
                            &self.now,
                        )
                        .await
                }
                WriteOpAction::Delete => self.record.delete_record(&write.uri).await,
            }
        });

        // Wait for all indexing operations to complete
        futures::future::try_join_all(futures).await?;

        Ok(())
    }

    /// Get record CIDs that are duplicated elsewhere in the repository.
    pub(crate) async fn get_duplicate_record_cids(
        &self,
        cids: &[Cid],
        touched_uris: &[AtUri],
    ) -> Result<Vec<Cid>> {
        if touched_uris.is_empty() || cids.is_empty() {
            return Ok(Vec::new());
        }

        // Convert CIDs and URIs to strings
        let cid_strs: Vec<String> = cids.iter().map(|c| c.to_string()).collect();
        let uri_strs: Vec<String> = touched_uris.iter().map(|u| u.to_string()).collect();

        // Query the database for duplicates
        let rows = sqlx::query!(
            "SELECT cid FROM record WHERE cid IN (?) AND uri NOT IN (?)",
            cid_strs.join(","),
            uri_strs.join(",")
        )
        .fetch_all(&self.db.pool)
        .await
        .context("failed to query duplicate record CIDs")?;

        // Convert to CIDs
        let result = rows
            .into_iter()
            .map(|row| Cid::try_from(&row.cid))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(result)
    }
}

// Helper functions

/// Convert a PreparedCreate to an operation.
fn create_write_to_op(write: &PreparedCreate) -> WriteOp {
    WriteOp {
        action: WriteOpAction::Create,
        collection: write.uri.collection.clone(),
        rkey: write.uri.rkey.clone(),
        record: write.record.clone(),
    }
}

/// Convert a PreparedWrite to an operation.
fn write_to_op(write: &PreparedWrite) -> WriteOp {
    match write.action {
        WriteOpAction::Create => WriteOp {
            action: WriteOpAction::Create,
            collection: write.uri.collection.clone(),
            rkey: write.uri.rkey.clone(),
            record: write.record.clone(),
        },
        WriteOpAction::Update => WriteOp {
            action: WriteOpAction::Update,
            collection: write.uri.collection.clone(),
            rkey: write.uri.rkey.clone(),
            record: write.record.clone(),
        },
        WriteOpAction::Delete => WriteOp {
            action: WriteOpAction::Delete,
            collection: write.uri.collection.clone(),
            rkey: write.uri.rkey.clone(),
            record: None,
        },
    }
}
