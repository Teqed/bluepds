//! Repository transactor for the actor store.

use anyhow::Result;
use atrium_repo::Cid;
use rsky_repo::types::{CommitDataWithOps, PreparedWrite};
use rsky_syntax::aturi::AtUri;
use sqlx::SqlitePool;

use crate::SigningKey;

use super::{reader::RepoReader, sql_repo_transactor::SqlRepoTransactor};

/// Transactor for repository operations.
pub(crate) struct RepoTransactor {
    /// The repository reader.
    pub reader: RepoReader,
    /// The SQL repository transactor.
    pub storage: SqlRepoTransactor,
    /// The DID of the repository owner.
    pub did: String,
    /// The signing key for the repository.
    pub signing_key: SigningKey,
}

impl RepoTransactor {
    /// Create a new repository transactor.
    pub(crate) fn new(
        db: SqlitePool,
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
    pub(crate) async fn create_repo(&self, writes: Vec<PreparedWrite>) -> Result<CommitDataWithOps> {
        todo!("Implement create_repo")
    }

    /// Process writes to the repository.
    pub(crate) async fn process_writes(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit_cid: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        todo!("Implement process_writes")
    }

    /// Format a commit for writing.
    pub(crate) async fn format_commit(
        &self,
        writes: Vec<PreparedWrite>,
        swap_commit: Option<Cid>,
    ) -> Result<CommitDataWithOps> {
        todo!("Implement format_commit")
    }

    /// Index writes in the repository.
    pub(crate) async fn index_writes(&self, writes: Vec<PreparedWrite>, rev: &str) -> Result<()> {
        todo!("Implement index_writes")
    }

    /// Get duplicate record CIDs.
    pub(crate) async fn get_duplicate_record_cids(
        &self,
        cids: &[Cid],
        touched_uris: &[AtUri],
    ) -> Result<Vec<Cid>> {
        todo!("Implement get_duplicate_record_cids")
    }
}
