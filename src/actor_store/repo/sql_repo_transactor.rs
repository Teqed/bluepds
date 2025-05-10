//! SQL-based repository transactor.

use std::str::FromStr;

use anyhow::Result;
use atrium_repo::{
    Cid,
    blockstore::{AsyncBlockStoreWrite, Error as BlockstoreError},
};
use sha2::Digest;
use sqlx::SqlitePool;

use crate::repo::{block_map::BlockMap, types::CommitData};

use super::sql_repo_reader::{RootInfo, SqlRepoReader};

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

    /// Get the root CID and revision of the repository.
    pub(crate) async fn get_root_detailed(&self) -> Result<RootInfo> {
        let row = sqlx::query!(
            r#"
                SELECT cid, rev
                FROM repo_root
                WHERE did = ?
                LIMIT 1
            "#,
            self.reader.did
        )
        .fetch_one(&self.reader.db)
        .await?;

        let cid = Cid::from_str(&row.cid)?;
        Ok(RootInfo { cid, rev: row.rev })
    }

    /// Proactively cache all blocks from a particular commit.
    pub(crate) async fn cache_rev(&mut self, rev: &str) -> Result<()> {
        let did = self.reader.did.clone();
        let rows = sqlx::query!(
            r#"
                SELECT cid, content
                FROM repo_block
                WHERE repoRev = ?
                LIMIT 15
                "#,
            rev
        )
        .fetch_all(&self.reader.db)
        .await?;

        for row in rows {
            let cid = Cid::from_str(&row.cid)?;
            self.cache.set(cid, row.content.clone());
        }
        Ok(())
    }

    /// Apply a commit to the repository.
    pub(crate) async fn apply_commit(&self, commit: CommitData, is_create: bool) -> Result<()> {
        let is_create = is_create || false;
        let removed_cids_list = commit.removed_cids.to_list();

        // Run these operations in parallel for better performance
        tokio::try_join!(
            self.update_root(commit.cid, &commit.rev, is_create),
            self.put_many(&commit.new_blocks, &commit.rev),
            self.delete_many(&removed_cids_list)
        )?;

        Ok(())
    }

    /// Update the repository root.
    pub(crate) async fn update_root(&self, cid: Cid, rev: &str, is_create: bool) -> Result<()> {
        let cid_str = cid.to_string();
        let did = self.reader.did.clone();
        let now = self.now.clone();

        if is_create {
            sqlx::query!(
                r#"
                    INSERT INTO repo_root (did, cid, rev, indexedAt)
                    VALUES (?, ?, ?, ?)
                    "#,
                did,
                cid_str,
                rev,
                now
            )
            .execute(&self.reader.db)
            .await?;
        } else {
            sqlx::query!(
                r#"
                    UPDATE repo_root
                    SET cid = ?, rev = ?, indexedAt = ?
                    WHERE did = ?
                    "#,
                cid_str,
                rev,
                now,
                did
            )
            .execute(&self.reader.db)
            .await?;
        }

        Ok(())
    }

    /// Put a block into the repository.
    pub(crate) async fn put_block(&self, cid: Cid, block: &[u8], rev: &str) -> Result<()> {
        let cid_str = cid.to_string();

        let block_len = block.len() as i64;
        sqlx::query!(
            r#"
                INSERT INTO repo_block (cid, repoRev, size, content)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                "#,
            cid_str,
            rev,
            block_len,
            block
        )
        .execute(&self.reader.db)
        .await?;

        Ok(())
    }

    /// Put many blocks into the repository.
    pub(crate) async fn put_many(&self, blocks: &BlockMap, rev: &str) -> Result<()> {
        if blocks.size() == 0 {
            return Ok(());
        }

        let did = self.reader.did.clone();
        let mut batch = Vec::new();

        blocks.to_owned().map.into_iter().for_each(|(cid, bytes)| {
            batch.push((cid, did.clone(), rev, bytes.0.len() as i64, bytes.0));
        });

        // Process in chunks to avoid too many parameters
        for chunk in batch.chunks(50) {
            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "INSERT INTO repo_block (cid, did, repoRev, size, content) VALUES {} ON CONFLICT DO NOTHING",
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            for (cid, did, rev, size, content) in chunk {
                query_builder = query_builder
                    .bind(cid)
                    .bind(did)
                    .bind(rev)
                    .bind(size)
                    .bind(content);
            }

            query_builder.execute(&self.reader.db).await?;
        }

        Ok(())
    }

    /// Delete many blocks from the repository.
    pub(crate) async fn delete_many(&self, cids: &[Cid]) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }

        let did = self.reader.did.clone();
        let cid_strings: Vec<String> = cids.iter().map(|c| c.to_string()).collect();

        // Process in chunks to avoid too many parameters
        for chunk in cid_strings.chunks(500) {
            let placeholders = std::iter::repeat("?")
                .take(chunk.len())
                .collect::<Vec<_>>()
                .join(",");

            let query = format!(
                "DELETE FROM repo_block WHERE did = ? AND cid IN ({})",
                placeholders
            );

            let mut query_builder = sqlx::query(&query);
            query_builder = query_builder.bind(&did);
            for cid in chunk {
                query_builder = query_builder.bind(cid);
            }

            query_builder.execute(&self.reader.db).await?;
        }

        Ok(())
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
        let contents = contents.to_vec();
        let rev = self.now.clone();

        async move {
            let digest = match hash {
                atrium_repo::blockstore::SHA2_256 => sha2::Sha256::digest(&contents),
                _ => return Err(BlockstoreError::UnsupportedHash(hash)),
            };

            let multihash = atrium_repo::Multihash::wrap(hash, &digest)
                .map_err(|_| BlockstoreError::UnsupportedHash(hash))?;

            let cid = Cid::new_v1(codec, multihash);
            let cid_str = cid.to_string();
            let contents_len = contents.len() as i64;

            sqlx::query!(
                r#"
                    INSERT INTO repo_block (cid, repoRev, size, content)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    "#,
                cid_str,
                rev,
                contents_len,
                contents
            )
            .execute(&self.reader.db)
            .await
            .map_err(|e| BlockstoreError::Other(Box::new(e)))?;

            self.cache.set(cid, contents);
            Ok(cid)
        }
    }
}
