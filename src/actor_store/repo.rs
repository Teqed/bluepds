//! Repository operations for actor store.

use std::str::FromStr as _;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use cidv10::Cid as CidV10;
use diesel::prelude::*;
use rsky_repo::{
    block_map::BlockMap,
    cid_set::CidSet,
    repo::Repo,
    storage::{readable_blockstore::ReadableBlockstore as _, types::RepoStorage},
    types::{
        CommitAction, CommitData, CommitDataWithOps, CommitOp, PreparedBlobRef, PreparedWrite,
        WriteOpAction, write_to_op,
    },
    util::format_data_key,
};
use rsky_syntax::aturi::AtUri;
use tokio::sync::RwLock;

use super::{
    ActorDb,
    blob::{BackgroundQueue, BlobHandler, BlobStorePlaceholder},
    record::RecordHandler,
};
use crate::SigningKey;

use crate::actor_store::sql_repo::SqlRepoStorage;

/// Data for sync events.
pub(crate) struct SyncEventData {
    /// The CID of the repository root.
    pub cid: Cid,
    /// The revision of the repository.
    pub rev: String,
    /// The blocks in the repository.
    pub blocks: BlockMap,
}

/// Unified repository handler for the actor store with both read and write capabilities.
pub(crate) struct RepoHandler {
    /// Actor DID
    pub did: String,
    /// Backend storage
    pub storage: Arc<RwLock<dyn RepoStorage>>,
    /// BlobReader for handling blob operations
    pub blob: BlobHandler,
    /// RecordHandler for handling record operations
    pub record: RecordHandler,
    /// BlobTransactor for handling blob writes
    pub blob_transactor: BlobHandler,
    /// RecordHandler for handling record writes
    pub record_transactor: RecordHandler,
    /// Signing keypair
    pub signing_key: Option<Arc<SigningKey>>,
    /// Background queue for async operations
    pub background_queue: BackgroundQueue,
}

impl RepoHandler {
    /// Create a new repository handler with read/write capabilities.
    pub(crate) fn new(
        db: ActorDb,
        blobstore: BlobStorePlaceholder,
        did: String,
        signing_key: Arc<SigningKey>,
        background_queue: BackgroundQueue,
    ) -> Self {
        // Create readers
        let blob = BlobHandler::new(db.clone(), blobstore.clone());
        let record = RecordHandler::new(db.clone(), did.clone());

        // Create storage backend with current timestamp
        let now = chrono::Utc::now().to_rfc3339();
        let storage = SqlRepoStorage::new(did.clone(), db.clone(), Some(now));

        // Create transactors
        let blob_transactor =
            BlobHandler::new(db.clone(), blobstore.clone(), background_queue.clone());
        let record_transactor = RecordHandler::new(db.clone(), blobstore);

        Self {
            did,
            storage,
            blob,
            record,
            blob_transactor,
            record_transactor,
            signing_key: Some(signing_key),
            background_queue,
        }
    }

    /// Get event data for synchronization.
    pub(crate) async fn get_sync_event_data(&self) -> Result<SyncEventData> {
        let root = self.storage.get_root_detailed().await?;
        let blocks = self
            .storage
            .get_blocks(vec![CidV10::from_str(&root.cid.to_string()).unwrap()])
            .await?;

        Ok(SyncEventData {
            cid: root.cid,
            rev: root.rev,
            blocks: blocks.blocks,
        })
    }

    /// Try to load repository
    pub(crate) async fn maybe_load_repo(&self) -> Result<Option<Repo>> {
        match self.storage.get_root().await {
            Some(cid) => {
                let repo = Repo::load(&self.storage, cid).await?;
                Ok(Some(repo))
            }
            None => Ok(None),
        }
    }

    /// Create a new repository with prepared writes
    pub(crate) async fn create_repo(
        &self,
        writes: Vec<PreparedWrite>,
    ) -> Result<CommitDataWithOps> {
        let signing_key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No signing key available for write operations"))?;

        // Convert writes to operations
        let ops = writes
            .iter()
            .map(|w| write_to_op(w))
            .collect::<Result<Vec<_>>>()?;

        // Format the initial commit
        let commit = Repo::format_init_commit(&self.storage, &self.did, signing_key, ops).await?;

        // Apply the commit, index the writes, and process blobs in parallel
        let results = futures::future::join3(
            self.storage.apply_commit(commit.clone(), Some(true)),
            self.index_writes(&writes, &commit.rev),
            self.blob_transactor
                .process_write_blobs(&commit.rev, writes.clone()),
        )
        .await;

        // Check for errors
        results.0.context("Failed to apply commit")?;
        results.1.context("Failed to index writes")?;
        results.2.context("Failed to process blobs")?;

        // Create commit operations
        let ops = writes
            .iter()
            .filter_map(|w| match w {
                PreparedWrite::Create(c) | PreparedWrite::Update(c) => {
                    let uri = AtUri::from_str(&c.uri).ok()?;
                    Some(CommitOp {
                        action: CommitAction::Create,
                        path: format_data_key(uri.get_collection(), uri.get_rkey()),
                        cid: Some(c.cid),
                        prev: None,
                    })
                }
                PreparedWrite::Delete(_) => None,
            })
            .collect();

        Ok(CommitDataWithOps {
            commit_data: commit,
            ops,
            prev_data: None,
        })
    }

    /// Process writes to the repository
    pub(crate) async fn process_writes(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Check write limit
        if writes.len() > 200 {
            return Err(anyhow::anyhow!("Too many writes. Max: 200"));
        }

        // Format the commit
        let commit = self.format_commit(writes.clone(), swap_commit_cid).await?;

        // Check commit size limit (2MB)
        if commit.commit_data.relevant_blocks.byte_size()? > 2_000_000 {
            return Err(anyhow::anyhow!("Too many writes. Max event size: 2MB"));
        }

        // Apply the commit, index the writes, and process blobs in parallel
        let results = futures::future::join3(
            self.storage.apply_commit(commit.commit_data.clone(), None),
            self.index_writes(&writes, &commit.commit_data.rev),
            self.blob_transactor
                .process_write_blobs(&commit.commit_data.rev, writes),
        )
        .await;

        // Check for errors
        results.0.context("Failed to apply commit")?;
        results.1.context("Failed to index writes")?;
        results.2.context("Failed to process blobs")?;

        Ok(commit)
    }

    /// Format a commit for writes
    pub(crate) async fn format_commit(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        // Ensure we have a signing key
        let signing_key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No signing key available for write operations"))?;

        // Get current root
        let curr_root = self
            .storage
            .get_root_detailed()
            .await
            .context("Failed to get repository root")?;

        // Check commit swap if requested
        if let Some(swap) = swap_commit_cid {
            if curr_root.cid != swap {
                return Err(anyhow::anyhow!(
                    "Bad commit swap: current={}, expected={}",
                    curr_root.cid,
                    swap
                ));
            }
        }

        // Cache the current revision for better performance
        self.storage.cache_rev(&curr_root.rev).await?;

        // Prepare collections for tracking changes
        let mut new_record_cids = Vec::new();
        let mut del_and_update_uris = Vec::new();
        let mut commit_ops = Vec::new();

        // Process each write to build operations and gather info
        for write in &writes {
            match write {
                PreparedWrite::Create(w) => {
                    new_record_cids.push(w.cid);
                    let uri = AtUri::from_str(&w.uri)?;
                    commit_ops.push(CommitOp {
                        action: CommitAction::Create,
                        path: format_data_key(uri.get_collection(), uri.get_rkey()),
                        cid: Some(w.cid),
                        prev: None,
                    });

                    // Validate swap_cid conditions
                    if w.swap_cid.is_some() && w.swap_cid != Some(None) {
                        return Err(anyhow::anyhow!(
                            "Bad record swap: there should be no current record for a create"
                        ));
                    }
                }
                PreparedWrite::Update(w) => {
                    new_record_cids.push(w.cid);
                    let uri = AtUri::from_str(&w.uri)?;
                    del_and_update_uris.push(uri.clone());

                    // Get the current record if it exists
                    let record = self.record.get_record(&uri, None, true).await?;
                    let curr_record = record.as_ref().map(|r| Cid::from_str(&r.cid).unwrap());

                    commit_ops.push(CommitOp {
                        action: CommitAction::Update,
                        path: format_data_key(uri.get_collection(), uri.get_rkey()),
                        cid: Some(w.cid),
                        prev: curr_record,
                    });

                    // Validate swap_cid conditions
                    if w.swap_cid.is_some() {
                        if w.swap_cid == Some(None) {
                            return Err(anyhow::anyhow!(
                                "Bad record swap: there should be a current record for an update"
                            ));
                        }

                        if let Some(Some(swap)) = w.swap_cid {
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
                PreparedWrite::Delete(w) => {
                    let uri = AtUri::from_str(&w.uri)?;
                    del_and_update_uris.push(uri.clone());

                    // Get the current record if it exists
                    let record = self.record.get_record(&uri, None, true).await?;
                    let curr_record = record.as_ref().map(|r| Cid::from_str(&r.cid).unwrap());

                    commit_ops.push(CommitOp {
                        action: CommitAction::Delete,
                        path: format_data_key(uri.get_collection(), uri.get_rkey()),
                        cid: None,
                        prev: curr_record,
                    });

                    // Validate swap_cid conditions
                    if w.swap_cid.is_some() {
                        if w.swap_cid == Some(None) {
                            return Err(anyhow::anyhow!(
                                "Bad record swap: there should be a current record for a delete"
                            ));
                        }

                        if let Some(Some(swap)) = w.swap_cid {
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
            }
        }

        // Load repository
        let repo = Repo::load(&self.storage, curr_root.cid).await?;
        let prev_data = repo.commit.data.clone();

        // Convert writes to repo operations
        let write_ops = writes
            .iter()
            .map(|w| write_to_op(w))
            .collect::<Result<Vec<_>>>()?;

        // Format the commit with the repository
        let mut commit = repo.format_commit(write_ops, signing_key).await?;

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
            let missing_blocks = self.storage.get_blocks(&new_record_blocks.missing).await?;
            commit.relevant_blocks.add_map(missing_blocks.blocks)?;
        }

        Ok(CommitDataWithOps {
            commit_data: commit,
            ops: commit_ops,
            prev_data: Some(prev_data),
        })
    }

    /// Index writes to the database
    pub(crate) async fn index_writes(&self, writes: &[PreparedWrite], rev: &str) -> Result<()> {
        let timestamp = chrono::Utc::now().to_rfc3339();

        for write in writes {
            match write {
                PreparedWrite::Create(w) => {
                    let uri = AtUri::from_str(&w.uri)?;
                    self.record_transactor
                        .index_record(
                            uri,
                            w.cid,
                            Some(&w.record),
                            WriteOpAction::Create,
                            rev,
                            Some(timestamp.clone()),
                        )
                        .await?;
                }
                PreparedWrite::Update(w) => {
                    let uri = AtUri::from_str(&w.uri)?;
                    self.record_transactor
                        .index_record(
                            uri,
                            w.cid,
                            Some(&w.record),
                            WriteOpAction::Update,
                            rev,
                            Some(timestamp.clone()),
                        )
                        .await?;
                }
                PreparedWrite::Delete(w) => {
                    let uri = AtUri::from_str(&w.uri)?;
                    self.record_transactor.delete_record(&uri).await?;
                }
            }
        }

        Ok(())
    }

    /// Get record CIDs that are duplicated elsewhere in the repository
    pub(crate) async fn get_duplicate_record_cids(
        &self,
        cids: &[Cid],
        touched_uris: &[AtUri],
    ) -> Result<Vec<Cid>> {
        if touched_uris.is_empty() || cids.is_empty() {
            return Ok(Vec::new());
        }

        // Convert URIs to strings for the query
        let uri_strings: Vec<String> = touched_uris.iter().map(|u| u.to_string()).collect();

        // Convert CIDs to strings for the query
        let cid_strings: Vec<String> = cids.iter().map(|c| c.to_string()).collect();

        let did = self.did.clone();

        // Query for records with these CIDs that aren't in the touched URIs
        let duplicate_cids = self
            .storage
            .db
            .run(move |conn| {
                use rsky_pds::schema::pds::record::dsl::*;

                record
                    .filter(did.eq(&did))
                    .filter(cid.eq_any(&cid_strings))
                    .filter(uri.ne_all(&uri_strings))
                    .select(cid)
                    .load::<String>(conn)
            })
            .await?;

        // Convert strings back to CIDs
        let cids = duplicate_cids
            .into_iter()
            .filter_map(|c| Cid::from_str(&c).ok())
            .collect();

        Ok(cids)
    }
}
