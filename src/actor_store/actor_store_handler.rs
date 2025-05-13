use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow};
use futures::TryStreamExt;
use rsky_repo::repo::Repo;
use rsky_repo::types::{CommitData, CommitDataWithOps, PreparedWrite as RskyPreparedWrite};

use super::PreparedWrite;
use super::blob::{BackgroundQueue, BlobStorePlaceholder};
use super::db::ActorDb;
use super::preference::PreferenceHandler;
use super::record::RecordHandler;
use super::repo::RepoHandler;
use crate::SigningKey;

/// Unified handler for actor store operations.
pub(crate) struct ActorStoreHandler {
    /// Actor DID
    pub did: String,
    /// Database connection
    pub db: ActorDb,
    /// Repository handler
    pub repo: RepoHandler,
    /// Record handler
    pub record: RecordHandler,
    /// Preference handler
    pub pref: PreferenceHandler,
    /// Background queue for async operations
    pub background_queue: Option<BackgroundQueue>,
    /// Signing keypair (required for write operations)
    pub signing_key: Option<Arc<SigningKey>>,
}

impl ActorStoreHandler {
    /// Create a new actor store handler with read-only capabilities
    pub(crate) fn new_reader(db: ActorDb, did: String, blobstore: BlobStorePlaceholder) -> Self {
        let record = RecordHandler::new(db.clone(), did.clone());
        let pref = PreferenceHandler::new(db.clone(), did.clone());
        let repo = RepoHandler::new_reader(db.clone(), blobstore, did.clone());

        Self {
            did,
            db,
            repo,
            record,
            pref,
            background_queue: None,
            signing_key: None,
        }
    }

    /// Create a new actor store handler with read/write capabilities
    pub(crate) fn new_writer(
        db: ActorDb,
        did: String,
        blobstore: BlobStorePlaceholder,
        signing_key: Arc<SigningKey>,
        background_queue: BackgroundQueue,
    ) -> Self {
        let record = RecordHandler::new_with_blobstore(db.clone(), blobstore.clone(), did.clone());
        let pref = PreferenceHandler::new(db.clone(), did.clone());
        let repo = RepoHandler::new_writer(
            db.clone(),
            blobstore,
            did.clone(),
            signing_key.clone(),
            background_queue.clone(),
        );

        Self {
            did,
            db,
            repo,
            record,
            pref,
            background_queue: Some(background_queue),
            signing_key: Some(signing_key),
        }
    }

    /// Set signing key (needed for write operations)
    pub(crate) fn with_signing_key(mut self, signing_key: Arc<SigningKey>) -> Self {
        self.signing_key = Some(signing_key);
        self
    }

    /// Set background queue (needed for async operations)
    pub(crate) fn with_background_queue(mut self, queue: BackgroundQueue) -> Self {
        self.background_queue = Some(queue);
        self
    }

    // Repository Operations
    // --------------------

    /// Try to load repository
    pub(crate) async fn maybe_load_repo(&self) -> Result<Option<Repo>> {
        self.repo.maybe_load_repo().await
    }

    /// Get repository root CID
    pub(crate) async fn get_repo_root(&self) -> Result<Option<atrium_repo::Cid>> {
        self.repo.get_repo_root().await
    }

    /// Create a new repository with prepared writes
    pub(crate) async fn create_repo(
        &self,
        writes: Vec<PreparedWrite>,
    ) -> Result<CommitDataWithOps> {
        if self.signing_key.is_none() {
            return Err(anyhow!(
                "No signing key available for create_repo operation"
            ));
        }

        let rsky_writes = writes
            .into_iter()
            .map(|w| RskyPreparedWrite::from(w))
            .collect::<Vec<_>>();

        self.repo.create_repo(rsky_writes).await
    }

    /// Process writes to the repository
    pub(crate) async fn process_writes(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<atrium_repo::Cid>,
    ) -> Result<CommitDataWithOps> {
        if self.signing_key.is_none() {
            return Err(anyhow!(
                "No signing key available for process_writes operation"
            ));
        }

        let rsky_writes = writes
            .into_iter()
            .map(|w| RskyPreparedWrite::from(w))
            .collect::<Vec<_>>();

        self.repo.process_writes(rsky_writes, swap_commit_cid).await
    }

    /// Import a repository from external data
    pub(crate) async fn process_import_repo(
        &self,
        commit: CommitData,
        writes: Vec<PreparedWrite>,
    ) -> Result<()> {
        let rsky_writes = writes
            .into_iter()
            .map(|w| RskyPreparedWrite::from(w))
            .collect::<Vec<_>>();

        // First index the records
        self.repo.index_writes(&rsky_writes, &commit.rev).await?;

        // Then process the commit
        self.repo.storage.apply_commit(commit.clone(), None).await?;

        // Finally process any blobs
        if let Some(bg_queue) = &self.background_queue {
            self.repo
                .blob_transactor
                .process_write_blobs(&commit.rev, rsky_writes)
                .await?;
        } else {
            return Err(anyhow!(
                "Background queue required for process_import_repo operation"
            ));
        }

        Ok(())
    }

    /// Get sync event data for replication
    pub(crate) async fn get_sync_event_data(&self) -> Result<super::repo::SyncEventData> {
        self.repo.get_sync_event_data().await
    }

    /// Destroy the repository and all associated data
    pub(crate) async fn destroy(&self) -> Result<()> {
        // Get all blob CIDs
        let blob_cids = self.repo.blob.get_blob_cids().await?;

        // Delete all blobs
        if !blob_cids.is_empty() {
            self.repo
                .blob_transactor
                .blobstore
                .delete_many(blob_cids.clone())
                .await?;
        }

        Ok(())
    }

    // Record Operations
    // ----------------

    /// Get a specific record
    pub(crate) async fn get_record(
        &self,
        uri: &rsky_syntax::aturi::AtUri,
        cid: Option<&str>,
        include_soft_deleted: bool,
    ) -> Result<Option<super::record::RecordData>> {
        self.record.get_record(uri, cid, include_soft_deleted).await
    }

    /// List collections in the repository
    pub(crate) async fn list_collections(&self) -> Result<Vec<String>> {
        self.record.list_collections().await
    }

    /// List records in a collection
    pub(crate) async fn list_records_for_collection(
        &self,
        opts: super::record::ListRecordsOptions,
    ) -> Result<Vec<super::record::RecordData>> {
        self.record.list_records_for_collection(opts).await
    }

    /// Get record count
    pub(crate) async fn record_count(&self) -> Result<i64> {
        self.record.record_count().await
    }

    /// Update record takedown status
    pub(crate) async fn update_record_takedown_status(
        &self,
        uri: &rsky_syntax::aturi::AtUri,
        takedown: atrium_api::com::atproto::admin::defs::StatusAttr,
    ) -> Result<()> {
        self.record
            .update_record_takedown_status(uri, takedown)
            .await
    }

    // Preference Operations
    // -------------------

    /// Get preferences for a namespace
    pub(crate) async fn get_preferences(
        &self,
        namespace: Option<&str>,
        scope: &str,
    ) -> Result<Vec<super::preference::AccountPreference>> {
        self.pref.get_preferences(namespace, scope).await
    }

    /// Put preferences for a namespace
    pub(crate) async fn put_preferences(
        &self,
        values: Vec<super::preference::AccountPreference>,
        namespace: &str,
        scope: &str,
    ) -> Result<()> {
        self.pref.put_preferences(values, namespace, scope).await
    }

    // Blob Operations
    // --------------

    /// Get blob metadata
    pub(crate) async fn get_blob_metadata(
        &self,
        cid: &atrium_repo::Cid,
    ) -> Result<super::blob::BlobMetadata> {
        self.repo.blob.get_blob_metadata(cid).await
    }

    /// Get blob data
    pub(crate) async fn get_blob(&self, cid: &atrium_repo::Cid) -> Result<super::blob::BlobData> {
        self.repo.blob.get_blob(cid).await
    }

    /// Update blob takedown status
    pub(crate) async fn update_blob_takedown_status(
        &self,
        cid: atrium_repo::Cid,
        takedown: atrium_api::com::atproto::admin::defs::StatusAttr,
    ) -> Result<()> {
        self.repo
            .blob
            .update_blob_takedown_status(cid, takedown)
            .await
    }

    /// Upload blob and get metadata
    pub(crate) async fn upload_blob_and_get_metadata(
        &self,
        user_suggested_mime: &str,
        blob_bytes: &[u8],
    ) -> Result<super::blob::BlobMetadata> {
        self.repo
            .blob
            .upload_blob_and_get_metadata(user_suggested_mime, blob_bytes)
            .await
    }

    /// Count blobs
    pub(crate) async fn blob_count(&self) -> Result<i64> {
        self.repo.blob.blob_count().await
    }

    // Transaction Support
    // -----------------

    /// Execute a transaction
    pub(crate) async fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut diesel::SqliteConnection) -> Result<T> + Send,
        T: Send + 'static,
    {
        self.db.transaction(f).await
    }

    /// Execute a database operation with retries
    pub(crate) async fn run<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut diesel::SqliteConnection) -> diesel::QueryResult<T> + Send,
        T: Send + 'static,
    {
        self.db.run(operation).await
    }
}
