//! SQL-based repository transactor.

use anyhow::Result;
use atrium_repo::{
    Cid,
    blockstore::{
        AsyncBlockStoreWrite, Error as BlockstoreError,
    },
};
use rsky_repo::{block_map::BlockMap, types::CommitData};
use sqlx::SqlitePool;

use super::sql_repo_reader::SqlRepoReader;

/// SQL-based repository transactor that extends the reader.
pub(crate) struct SqlRepoTransactor {
    /// The inner reader.
    pub reader: SqlRepoReader,
    /// Cache for blocks.
    pub cache: BlockMap,
    /// Current timestamp.
    pub now: String,
}

impl SqlRepoTransactor {
    /// Create a new SQL repository transactor.
    pub(crate) fn new(db: SqlitePool, did: String) -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            reader: SqlRepoReader::new(db, did),
            cache: BlockMap::new(),
            now,
        }
    }

    /// Proactively cache all blocks from a particular commit.
    pub(crate) async fn cache_rev(&mut self, rev: &str) -> Result<()> {
        todo!("Implement cache_rev")
    }

    /// Apply a commit to the repository.
    pub(crate) async fn apply_commit(&self, commit: CommitData, is_create: bool) -> Result<()> {
        todo!("Implement apply_commit")
    }

    /// Update the repository root.
    pub(crate) async fn update_root(&self, cid: Cid, rev: &str, is_create: bool) -> Result<()> {
        todo!("Implement update_root")
    }

    /// Put a block into the repository.
    pub(crate) async fn put_block(&self, cid: Cid, block: &[u8], rev: &str) -> Result<()> {
        todo!("Implement put_block")
    }

    /// Put many blocks into the repository.
    pub(crate) async fn put_many(&self, blocks: &BlockMap, rev: &str) -> Result<()> {
        todo!("Implement put_many")
    }

    /// Delete many blocks from the repository.
    pub(crate) async fn delete_many(&self, cids: &[Cid]) -> Result<()> {
        todo!("Implement delete_many")
    }
}

#[async_trait::async_trait]
impl AsyncBlockStoreWrite for SqlRepoTransactor {
    fn write_block(
        &mut self,
        codec: u64,
        hash: u64,
        contents: &[u8],
    ) -> impl Future<Output = Result<Cid, BlockstoreError>> + Send {
        async move {
            todo!();
        }
    }
}
