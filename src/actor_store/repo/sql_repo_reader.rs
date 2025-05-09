//! SQL-based repository reader.

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid,
    blockstore::{AsyncBlockStoreRead, Error as BlockstoreError},
};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::repo::block_map::{BlockMap, BlocksAndMissing, CidSet};

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
    // async has(cid: CID): Promise<boolean> {
    // async getCarStream(since?: string) {
    // async *iterateCarBlocks(since?: string): AsyncIterable<CarBlock> {
    // async getBlockRange(since?: string, cursor?: RevCursor) {
    // async countBlocks(): Promise<number> {
    // async destroy(): Promise<void> {

    pub(crate) async fn get_bytes(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        // First check the cache
        {
            let cache_guard = self.cache.read().await;
            if let Some(cached) = cache_guard.get(*cid) {
                return Ok(Some(cached.clone()));
            }
        }

        // Not in cache, query from database
        let cid_str = cid.to_string();
        let did = self.did.clone();

        let content = sqlx::query!(r#"SELECT content FROM repo_block WHERE cid = ?"#, cid_str,)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch block content")?
            .map(|row| row.content);

        // If found, update the cache
        if let Some(bytes) = &content {
            let mut cache_guard = self.cache.write().await;
            cache_guard.set(*cid, bytes.clone());
        }

        Ok(content)
    }

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
        let cached = { self.cache.write().await.get_many(cids)? }; // TODO: use read lock?

        if cached.missing.is_empty() {
            return Ok(cached);
        }

        let missing = cached.missing.clone();
        let missing_strings: Vec<String> = missing.iter().map(|c| c.to_string()).collect();

        let mut blocks = BlockMap::new();
        let mut missing_set = CidSet::new(None);
        for cid in &missing {
            missing_set.add(*cid);
        }

        // Process in chunks to avoid too many parameters
        for chunk in missing_strings.chunks(500) {
            let placeholders = std::iter::repeat("?")
                .take(chunk.len())
                .collect::<Vec<_>>()
                .join(",");

            let query = format!(
                "SELECT cid, content FROM repo_block
                     WHERE did = ? AND cid IN ({})
                     ORDER BY cid",
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            query_builder = query_builder.bind(&self.did);
            for cid in chunk {
                query_builder = query_builder.bind(cid);
            }

            let rows = query_builder
                .map(|row: sqlx::sqlite::SqliteRow| {
                    (
                        row.get::<String, _>("cid"),
                        row.get::<Vec<u8>, _>("content"),
                    )
                })
                .fetch_all(&self.db)
                .await?;

            for (cid_str, content) in rows {
                let cid = Cid::from_str(&cid_str)?;
                blocks.set(cid, content);
                missing_set.delete(cid);
            }
        }

        // Update cache
        self.cache.write().await.add_map(blocks.clone())?; // TODO: unnecessary clone?

        // Add cached blocks
        blocks.add_map(cached.blocks)?;

        Ok(BlocksAndMissing {
            blocks,
            missing: missing_set.to_list(),
        })
    }
}

impl AsyncBlockStoreRead for SqlRepoReader {
    async fn read_block(&mut self, cid: Cid) -> Result<Vec<u8>, BlockstoreError> {
        let bytes = self
            .get_bytes(&cid)
            .await
            .unwrap()
            .ok_or(BlockstoreError::CidNotFound)?;
        Ok(bytes)
    }

    fn read_block_into(
        &mut self,
        cid: Cid,
        contents: &mut Vec<u8>,
    ) -> impl Future<Output = Result<(), BlockstoreError>> + Send {
        async move {
            let bytes = self
                .get_bytes(&cid)
                .await
                .unwrap()
                .ok_or(BlockstoreError::CidNotFound)?;
            contents.clear();
            contents.extend_from_slice(&bytes);
            Ok(())
        }
    }
}
