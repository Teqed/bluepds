//! Repository transactor for the actor store.

use std::str::FromStr;

use anyhow::Result;
use atrium_repo::Cid;
use rsky_syntax::aturi::AtUri;

use crate::{
    actor_store::ActorDb, repo::types::{CommitAction, CommitDataWithOps, CommitOp, PreparedWrite, WriteOpAction}, SigningKey
};

use super::{reader::RepoReader, sql_repo_transactor::SqlRepoTransactor};

/// Transactor for repository operations.
pub(crate) struct RepoTransactor {
    /// The inner reader.
    pub(crate) reader: RepoReader,
    ///
}


impl RepoTransactor {
    /// Create a new repository transactor.
    pub(crate) fn new(
        db: ActorDb,
        did: String,
        signing_key: SigningKey,
        blob_config: crate::config::BlobConfig,
    ) -> Self {
        Self {
            reader: RepoReader::new(db.clone(), did.clone(), blob_config),
            storage: SqlRepoTransactor::new(db, did.clone()),
            did,
            signing_key,
        }
    }

    /// Load the repository if it exists.
    // pub async fn maybe_load_repo(
    //     &self,
    // ) -> Result<Option<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>>> {
    //     todo!("Implement maybe_load_repo")
    // }

    /// Create a new repository.
    pub(crate) async fn create_repo(
        &self,
        writes: Vec<PreparedWrite>,
    ) -> Result<CommitDataWithOps> {
        todo!("Implement create_repo")
    }

    /// Process writes to the repository.
    pub(crate) async fn process_writes(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Validate parameters
        if writes.len() > 200 {
            return Err(anyhow::anyhow!("Too many writes. Max: 200").into());
        }

        // Format the commit (creates blocks and structures the operations)
        let commit = self.format_commit(writes.clone(), swap_commit_cid).await?;

        // Check commit size (prevent large commits)
        if commit.commit_data.relevant_blocks.byte_size()? > 2000000 {
            return Err(anyhow::anyhow!("Too many writes. Max event size: 2MB").into());
        }

        // Execute these operations in parallel for better performance
        tokio::try_join!(
            // Persist the commit to repo storage
            self.storage.apply_commit(commit.commit_data.clone(), true),
            // Send to indexing
            self.index_writes(writes.clone(), &commit.commit_data.rev),
            // Process blobs from writes
            self.process_write_blobs(&commit.commit_data.rev, &writes),
        )?;

        Ok(commit)
    }

    /// Format a commit for writing.
    pub(crate) async fn format_commit(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Get current repository root
        let curr_root = self.storage.get_root_detailed().await?;
        if curr_root.cid.is_nil() {
            return Err(anyhow::anyhow!("No repo root found for {}", self.did).into());
        }

        // Check swap commit if provided
        if let Some(swap_cid) = swap_commit {
            if !curr_root.cid.equals(swap_cid) {
                return Err(anyhow::anyhow!(
                    "Bad commit swap: expected {}, got {}",
                    swap_cid,
                    curr_root.cid
                )
                .into());
            }
        }

        // Cache last commit for performance
        self.storage.cache_rev(&curr_root.rev).await?;

        let mut new_record_cids = Vec::new();
        let mut del_and_update_uris = Vec::new();
        let mut commit_ops = Vec::new();

        // Process each write to create commit operations
        for write in &writes {
            let uri_str = write.uri().clone();
            let uri = AtUri::try_from(uri_str.as_str())?;

            match write.action() {
                WriteOpAction::Create | WriteOpAction::Update => {
                    if let Some(cid) = write.cid() {
                        new_record_cids.push(cid);
                    }
                }
                _ => {}
            }

            if write.action() != &WriteOpAction::Create {
                del_and_update_uris.push(uri.clone());
            }

            // Get current record if it exists
            let record = self.record.get_record(&uri.to_string(), None, true).await?;
            let curr_record = record.map(|r| Cid::from_str(&r.cid).ok()).flatten();

            // Create the operation
            let path = format!("{}/{}", uri.get_collection(), uri.get_rkey());
            let mut op = CommitOp {
                action: match write.action() {
                    WriteOpAction::Create => CommitAction::Create,
                    WriteOpAction::Update => CommitAction::Update,
                    WriteOpAction::Delete => CommitAction::Delete,
                },
                path: path.clone(),
                cid: match write.action() {
                    WriteOpAction::Delete => None,
                    _ => write.cid(),
                },
                prev: curr_record.clone(),
            };

            // Validate swap consistency
            if let Some(swap_cid) = write.swap_cid() {
                match write.action() {
                    WriteOpAction::Create if swap_cid.is_some() => {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: cannot provide swap CID for create"
                        )
                        .into());
                    }
                    WriteOpAction::Update | WriteOpAction::Delete if swap_cid.is_none() => {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: must provide swap CID for update/delete"
                        )
                        .into());
                    }
                    _ => {}
                }

                if swap_cid.is_some()
                    && curr_record.is_some()
                    && !curr_record.unwrap().equals(swap_cid.unwrap())
                {
                    return Err(anyhow::anyhow!(
                        "Bad record swap: expected {}, got {:?}",
                        swap_cid.unwrap(),
                        curr_record
                    )
                    .into());
                }
            }

            commit_ops.push(op);
        }

        // Load the current repository and prepare for modification
        let repo = crate::storage::open_repo(&self.storage.reader.config, &self.did, curr_root.cid)
            .await?;
        let prev_data = repo.commit().data;

        // Convert PreparedWrites to RecordWriteOps
        let write_ops = writes
            .iter()
            .map(|w| write_to_op(w.clone()))
            .collect::<Result<Vec<_>>>()?;

        // Format the new commit
        let commit = repo.format_commit(write_ops, &self.signing_key).await?;

        // Find blocks referenced by other records
        let dupe_record_cids = self
            .get_duplicate_record_cids(&commit.removed_cids.to_list(), &del_and_update_uris)
            .await?;

        // Remove duplicated CIDs from the removal list
        for cid in dupe_record_cids {
            commit.removed_cids.delete(cid);
        }

        // Ensure all necessary blocks are included
        let new_record_blocks = commit.relevant_blocks.get_many(new_record_cids)?;
        if !new_record_blocks.missing.is_empty() {
            let missing_blocks = self.storage.get_blocks(new_record_blocks.missing).await?;
            commit.relevant_blocks.add_map(missing_blocks.blocks)?;
        }

        Ok(CommitDataWithOps {
            commit_data: commit,
            ops: commit_ops,
            prev_data: Some(prev_data),
        })
    }

    /// Index writes in the repository.
    pub(crate) async fn index_writes(&self, writes: Vec<PreparedWrite>, rev: &str) -> Result<()> {
        // Process each write for indexing
        for write in writes {
            let uri_str = write.uri().clone();
            let uri = AtUri::try_from(uri_str.as_str())?;

            match write.action() {
                WriteOpAction::Create | WriteOpAction::Update => {
                    if let PreparedWrite::Create(w) | PreparedWrite::Update(w) = write {
                        self.record
                            .index_record(
                                uri,
                                w.cid,
                                Some(w.record),
                                write.action().clone(),
                                rev,
                                None, // Use current timestamp
                            )
                            .await?;
                    }
                }
                WriteOpAction::Delete => {
                    self.record.delete_record(&uri).await?;
                }
            }
        }

        Ok(())
    }

    /// Get duplicate record CIDs.
    pub(crate) async fn get_duplicate_record_cids(
        &self,
        cids: &[Cid],
        touched_uris: &[AtUri],
    ) -> Result<Vec<Cid>> {
        if touched_uris.is_empty() || cids.is_empty() {
            return Ok(Vec::new());
        }

        let cid_strs: Vec<String> = cids.iter().map(|c| c.to_string()).collect();
        let uri_strs: Vec<String> = touched_uris.iter().map(|u| u.to_string()).collect();

        // Find records that have the same CIDs but weren't touched in this operation
        let duplicates = sqlx::query_scalar!(
            r#"
            SELECT cid FROM record
            WHERE cid IN (SELECT unnest($1::text[]))
            AND uri NOT IN (SELECT unnest($2::text[]))
            "#,
            &cid_strs,
            &uri_strs
        )
        .fetch_all(&self.reader.db)
        .await?;

        // Convert string CIDs back to Cid objects
        let dupe_cids = duplicates
            .into_iter()
            .filter_map(|cid_str| Cid::from_str(&cid_str).ok())
            .collect();

        Ok(dupe_cids)
    }

    pub(crate) async fn process_write_blobs(
        &self,
        rev: &str,
        writes: &[PreparedWrite],
    ) -> Result<()> {
        // First handle deletions or updates
        let uris: Vec<String> = writes
            .iter()
            .filter(|w| matches!(w.action(), WriteOpAction::Delete | WriteOpAction::Update))
            .map(|w| w.uri().clone())
            .collect();

        if !uris.is_empty() {
            // Remove blob references for deleted/updated records
            self.blob.delete_dereferenced_blobs(&uris).await?;
        }

        // Process each blob in each write
        for write in writes {
            if let PreparedWrite::Create(w) | PreparedWrite::Update(w) = write {
                for blob in &w.blobs {
                    // Verify and make permanent
                    self.blob.verify_blob_and_make_permanent(blob).await?;

                    // Associate blob with record
                    self.blob
                        .associate_blob(&blob.cid.to_string(), write.uri())
                        .await?;
                }
            }
        }

        Ok(())
    }
}
