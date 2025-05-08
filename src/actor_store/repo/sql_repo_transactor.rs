//! SQL-backed repository transactor implementation.

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite},
};
use sqlx::SqlitePool;
use std::collections::HashMap;

use super::sql_repo_reader::SqlRepoReader;
use super::types::{BlockMap, CidSet};

/// Repository transactor that uses SQL storage.
pub struct SqlRepoTransactor {
    /// The underlying reader.
    pub reader: SqlRepoReader,
    /// The DID of the repository owner.
    pub did: String,
    /// The current timestamp.
    pub now: String,
}

impl SqlRepoTransactor {
    /// Create a new SQL repository transactor.
    pub fn new(db: SqlitePool, did: impl Into<String>, now: Option<String>) -> Self {
        Self {
            reader: SqlRepoReader::new(db),
            did: did.into(),
            now: now.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        }
    }

    /// Cache a specific revision to optimize read performance.
    pub async fn cache_rev(&mut self, rev: &str) -> Result<()> {
        let rows = sqlx::query!(
            "SELECT cid, content FROM repo_block WHERE repo_rev = ? LIMIT 15",
            rev
        )
        .fetch_all(&self.reader.db)
        .await
        .context("failed to fetch blocks for caching")?;

        for row in rows {
            let cid = Cid::try_from(row.cid).context("invalid CID format")?;
            self.reader.cache.insert(cid, row.content);
        }

        Ok(())
    }

    /// Put a block in the store.
    pub async fn put_block(&mut self, cid: Cid, block: &[u8], rev: &str) -> Result<()> {
        let cid_str = cid.to_string();
        let rev_str = rev.to_string();

        sqlx::query!(
            "INSERT INTO repo_block (cid, repoRev, size, content)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (cid) DO NOTHING",
            cid_str,
            rev_str,
            block.len() as i64,
            block
        )
        .execute(&self.reader.db)
        .await
        .context("failed to insert block")?;

        self.reader.cache.insert(cid, block.to_vec());
        Ok(())
    }

    /// Put multiple blocks in the store.
    pub async fn put_many(&mut self, to_put: &BlockMap) -> Result<()> {
        let mut tx = self
            .reader
            .db
            .begin()
            .await
            .context("failed to begin transaction")?;

        let root = self
            .reader
            .get_root_detailed()
            .await
            .context("failed to get repository root")?;
        let rev = root.rev;

        for (cid, bytes) in to_put.iter() {
            let cid_str = cid.to_string();
            sqlx::query!(
                "INSERT INTO repo_block (cid, repoRev, size, content)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (cid) DO NOTHING",
                cid_str,
                rev,
                bytes.len() as i64,
                bytes
            )
            .execute(&mut *tx)
            .await
            .context("failed to insert block in transaction")?;

            // Update cache
            self.reader.cache.insert(*cid, bytes.clone());
        }

        tx.commit().await.context("failed to commit transaction")?;
        Ok(())
    }

    /// Delete blocks from the store.
    pub async fn delete_many(&mut self, cids: &CidSet) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }

        let cid_list = cids.to_list();
        for chunk in cid_list.chunks(100) {
            let cid_strs: Vec<String> = chunk.iter().map(|c| c.to_string()).collect();
            let placeholders = (0..cid_strs.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");

            let query = format!("DELETE FROM repo_block WHERE cid IN ({})", placeholders);

            let mut query_builder = sqlx::query(&query);
            for cid_str in cid_strs {
                query_builder = query_builder.bind(cid_str);
            }

            query_builder
                .execute(&self.reader.db)
                .await
                .context("failed to delete blocks")?;
        }

        Ok(())
    }

    /// Update the repository root.
    pub async fn update_root(&mut self, cid: Cid, rev: &str, is_create: bool) -> Result<()> {
        let cid_str = cid.to_string();

        if is_create {
            sqlx::query!(
                "INSERT INTO repo_root (did, cid, rev, indexedAt) VALUES (?, ?, ?, ?)",
                self.did,
                cid_str,
                rev,
                self.now
            )
            .execute(&self.reader.db)
            .await
            .context("failed to insert repository root")?;
        } else {
            sqlx::query!(
                "UPDATE repo_root SET cid = ?, rev = ?, indexedAt = ?",
                cid_str,
                rev,
                self.now
            )
            .execute(&self.reader.db)
            .await
            .context("failed to update repository root")?;
        }

        Ok(())
    }

    /// Apply a commit to the repository.
    pub async fn apply_commit(
        &mut self,
        cid: Cid,
        rev: &str,
        blocks: &BlockMap,
        removed_cids: &CidSet,
        is_create: bool,
    ) -> Result<()> {
        let mut tx = self
            .reader
            .db
            .begin()
            .await
            .context("failed to begin transaction")?;

        // Update the root
        let cid_str = cid.to_string();
        if is_create {
            sqlx::query!(
                "INSERT INTO repo_root (did, cid, rev, indexedAt) VALUES (?, ?, ?, ?)",
                self.did,
                cid_str,
                rev,
                self.now
            )
            .execute(&mut *tx)
            .await
            .context("failed to insert repository root")?;
        } else {
            sqlx::query!(
                "UPDATE repo_root SET cid = ?, rev = ?, indexedAt = ?",
                cid_str,
                rev,
                self.now
            )
            .execute(&mut *tx)
            .await
            .context("failed to update repository root")?;
        }

        // Insert new blocks
        for (cid, bytes) in blocks.iter() {
            let cid_str = cid.to_string();
            sqlx::query!(
                "INSERT INTO repo_block (cid, repoRev, size, content)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (cid) DO NOTHING",
                cid_str,
                rev,
                bytes.len() as i64,
                bytes
            )
            .execute(&mut *tx)
            .await
            .context("failed to insert block in transaction")?;

            // Update cache
            self.reader.cache.insert(*cid, bytes.clone());
        }

        // Delete removed blocks
        if !removed_cids.is_empty() {
            let cid_list = removed_cids.to_list();
            let cid_strs: Vec<String> = cid_list.iter().map(|c| c.to_string()).collect();

            for chunk in cid_strs.chunks(100) {
                let placeholders = (0..chunk.len()).map(|_| "?").collect::<Vec<_>>().join(",");

                let query = format!("DELETE FROM repo_block WHERE cid IN ({})", placeholders);

                let mut query_builder = sqlx::query(&query);
                for cid_str in chunk {
                    query_builder = query_builder.bind(cid_str);
                }

                query_builder
                    .execute(&mut *tx)
                    .await
                    .context("failed to delete blocks")?;
            }
        }

        tx.commit().await.context("failed to commit transaction")?;
        Ok(())
    }
}

impl AsyncBlockStoreWrite for SqlRepoTransactor {
    async fn write_block(
        &mut self,
        codec: u64,
        hash: u64,
        bytes: &[u8],
    ) -> std::result::Result<Cid, anyhow::Error> {
        // Generate a CID for the block
        let multihash =
            atrium_repo::Multihash::wrap(hash, bytes).context("failed to wrap multihash")?;

        let cid = Cid::new_v1(codec, multihash);

        // Get the current revision or use a default one
        let root = self.reader.get_root_detailed().await?;

        // Store the block
        self.put_block(cid, bytes, &root.rev).await?;

        Ok(cid)
    }
}
