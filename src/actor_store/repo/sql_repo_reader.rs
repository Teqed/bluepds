//! SQL-backed repository reader implementation.

use anyhow::{Context as _, Result};
use atrium_repo::{Cid, blockstore::AsyncBlockStoreRead};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;

use super::types::{BlockGetManyResult, BlockMap, RepoRoot, RepoRootRow};

/// Repository reader that uses SQL storage.
pub struct SqlRepoReader {
    /// Cache for blocks to avoid repeated database queries.
    pub cache: BlockMap,
    /// The database connection.
    pub db: SqlitePool,
}

impl SqlRepoReader {
    /// Create a new SQL repository reader.
    pub fn new(db: SqlitePool) -> Self {
        Self {
            cache: BlockMap::new(),
            db,
        }
    }

    /// Get the root CID of the repository.
    pub async fn get_root(&self) -> Result<Cid> {
        let root = self.get_root_detailed().await?;
        Ok(root.cid)
    }

    /// Get detailed information about the repository root.
    pub async fn get_root_detailed(&self) -> Result<RepoRoot> {
        let res = sqlx::query_as!(RepoRootRow, "SELECT cid, rev FROM repo_root LIMIT 1")
            .fetch_one(&self.db)
            .await
            .context("failed to fetch repository root")?;

        Ok(RepoRoot {
            cid: Cid::try_from(res.cid).context("invalid CID format in repo_root")?,
            rev: res.rev,
        })
    }

    /// Get a block by CID.
    pub async fn get_bytes(&mut self, cid: Cid) -> Result<Option<Vec<u8>>> {
        if let Some(bytes) = self.cache.get(&cid) {
            return Ok(Some(bytes));
        }

        let cid_str = cid.to_string();
        let result = sqlx::query!("SELECT content FROM repo_block WHERE cid = ?", cid_str)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch block")?;

        if let Some(row) = result {
            self.cache.insert(cid, row.content.clone());
            Ok(Some(row.content))
        } else {
            Ok(None)
        }
    }

    /// Check if a block exists.
    pub async fn has(&mut self, cid: Cid) -> Result<bool> {
        let bytes = self.get_bytes(cid).await?;
        Ok(bytes.is_some())
    }

    /// Get multiple blocks by CID.
    pub async fn get_blocks(&mut self, cids: &[Cid]) -> Result<BlockGetManyResult> {
        let cached = self.cache.get_many(cids);

        if cached.missing.is_empty() {
            return Ok(cached);
        }

        let missing_strs: Vec<String> = cached.missing.iter().map(|c| c.to_string()).collect();

        let missing_cids = cached.missing.clone();
        let mut blocks = cached.blocks;
        let mut missing = Vec::new();

        for chunk in missing_strs.chunks(100) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

            let query = format!(
                "SELECT cid, content FROM repo_block WHERE cid IN ({})",
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            for cid_str in chunk {
                query_builder = query_builder.bind(cid_str);
            }

            let rows = query_builder
                .map(|row: sqlx::sqlite::SqliteRow| {
                    let cid_str: String = row.get(0);
                    let content: Vec<u8> = row.get(1);
                    (cid_str, content)
                })
                .fetch_all(&self.db)
                .await
                .context("failed to fetch blocks")?;

            let mut found_cids = HashMap::new();
            for (cid_str, content) in rows {
                let cid = Cid::try_from(cid_str).context("invalid CID format")?;
                blocks.insert(cid, content);
                found_cids.insert(cid.to_string(), true);
            }

            for cid in &missing_cids {
                if !found_cids.contains_key(&cid.to_string()) {
                    missing.push(*cid);
                }
            }
        }

        // Update cache with new blocks
        for (cid, content) in blocks.iter() {
            self.cache.insert(*cid, content.clone());
        }

        Ok(BlockGetManyResult { blocks, missing })
    }

    /// Count blocks in the repository.
    pub async fn count_blocks(&self) -> Result<i64> {
        let result = sqlx::query!("SELECT COUNT(*) as count FROM repo_block")
            .fetch_one(&self.db)
            .await
            .context("failed to count blocks")?;

        Ok(result.count)
    }
}

impl AsyncBlockStoreRead for SqlRepoReader {
    async fn read_block(&mut self, cid: Cid) -> std::result::Result<Vec<u8>, anyhow::Error> {
        match self.get_bytes(cid).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => Err(anyhow::anyhow!("CID not found")),
            Err(e) => Err(e),
        }
    }
}
