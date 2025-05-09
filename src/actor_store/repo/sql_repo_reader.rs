//! SQL-based repository reader.

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid,
    blockstore::{AsyncBlockStoreRead, Error as BlockstoreError},
};
use rsky_repo::block_map::BlockMap;
use sha2::Digest;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// SQL-based repository reader.
pub(crate) struct SqlRepoReader {
    /// Cache for blocks to avoid redundant database queries.
    pub cache: Arc<RwLock<BlockMap>>,
    /// Database connection.
    pub db: SqlitePool,
    /// DID of the repository owner.
    pub did: String,
}

/// Repository root with CID and revision.
pub(crate) struct RootInfo {
    /// CID of the repository root.
    pub cid: Cid,
    /// Revision of the repository.
    pub rev: String,
}

impl SqlRepoReader {
    /// Create a new SQL repository reader.
    pub(crate) fn new(db: SqlitePool, did: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(BlockMap::new())),
            db,
            did,
        }
    }

    // async getRoot(): Promise<CID> {
    // async getBytes(cid: CID): Promise<Uint8Array | null> {
    // async has(cid: CID): Promise<boolean> {
    // async getCarStream(since?: string) {
    // async *iterateCarBlocks(since?: string): AsyncIterable<CarBlock> {
    // async getBlockRange(since?: string, cursor?: RevCursor) {
    // async countBlocks(): Promise<number> {
    // async destroy(): Promise<void> {

    /// Get the detailed root information.
    pub(crate) async fn get_root_detailed(&self) -> Result<RootInfo> {
        let did = self.did.clone();
        let row = sqlx::query!(r#"SELECT cid, rev FROM repo_root WHERE did = ?"#, did)
            .fetch_one(&self.db)
            .await
            .context("failed to fetch repo root")?;

        Ok(RootInfo {
            cid: Cid::from_str(&row.cid)?,
            rev: row.rev,
        })
    }

    /// Get blocks from the database.
    pub(crate) async fn get_blocks(&self, cids: Vec<Cid>) -> Result<BlocksAndMissing> {
        todo!("Implement get_blocks")
    }
}

/// Result of a get_blocks operation.
pub(crate) struct BlocksAndMissing {
    /// Blocks that were found.
    pub blocks: BlockMap,
    /// CIDs that were not found.
    pub missing: Vec<Cid>,
}

impl AsyncBlockStoreRead for SqlRepoReader {
    async fn read_block(&mut self, cid: Cid) -> Result<Vec<u8>, BlockstoreError> {
        todo!("Implement read_block")
    }

    fn read_block_into(
        &mut self,
        cid: Cid,
        contents: &mut Vec<u8>,
    ) -> impl Future<Output = Result<(), BlockstoreError>> + Send {
        todo!("Implement read_block_into");
        async move { Ok(()) }
    }
}
