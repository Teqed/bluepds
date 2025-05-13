use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use atrium_repo::blockstore::{
    AsyncBlockStoreRead, AsyncBlockStoreWrite, Error as BlockstoreError,
};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;
use futures::{StreamExt, TryStreamExt, stream};
use rsky_pds::models::{RepoBlock, RepoRoot};
use rsky_repo::block_map::{BlockMap, BlocksAndMissing};
use rsky_repo::car::blocks_to_car_file;
use rsky_repo::cid_set::CidSet;
use rsky_repo::storage::CidAndRev;
use rsky_repo::storage::RepoRootError::RepoRootNotFoundError;
use rsky_repo::storage::readable_blockstore::ReadableBlockstore;
use rsky_repo::storage::types::RepoStorage;
use rsky_repo::types::CommitData;
use sha2::{Digest, Sha256};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::actor_store::db::ActorDb;

#[derive(Clone, Debug)]
pub struct SqlRepoStorage {
    /// In-memory cache for blocks
    pub cache: Arc<RwLock<BlockMap>>,
    /// Database connection
    pub db: ActorDb,
    /// DID of the actor
    pub did: String,
    /// Current timestamp
    pub now: String,
}

impl SqlRepoStorage {
    /// Create a new SQL repository storage
    pub fn new(did: String, db: ActorDb, now: Option<String>) -> Self {
        let now = now.unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        Self {
            cache: Arc::new(RwLock::new(BlockMap::new())),
            db,
            did,
            now,
        }
    }

    /// Get the CAR stream for the repository
    pub async fn get_car_stream(&self, since: Option<String>) -> Result<Vec<u8>> {
        match self.get_root().await {
            None => Err(anyhow::Error::new(RepoRootNotFoundError)),
            Some(root) => {
                let mut car = BlockMap::new();
                let mut cursor: Option<CidAndRev> = None;

                loop {
                    let blocks = self.get_block_range(&since, &cursor).await?;
                    if blocks.is_empty() {
                        break;
                    }

                    // Add blocks to car
                    for block in &blocks {
                        car.set(Cid::from_str(&block.cid)?, block.content.clone());
                    }

                    if let Some(last_block) = blocks.last() {
                        cursor = Some(CidAndRev {
                            cid: Cid::from_str(&last_block.cid)?,
                            rev: last_block.repoRev.clone(),
                        });
                    } else {
                        break;
                    }
                }

                blocks_to_car_file(Some(&root), car).await
            }
        }
    }

    /// Get a range of blocks from the database
    pub async fn get_block_range(
        &self,
        since: &Option<String>,
        cursor: &Option<CidAndRev>,
    ) -> Result<Vec<RepoBlock>> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                use rsky_pds::schema::pds::repo_block::dsl::*;

                let mut query = repo_block.filter(did.eq(&did)).limit(500).into_boxed();

                if let Some(c) = cursor {
                    query = query.filter(
                        repoRev
                            .lt(&c.rev)
                            .or(repoRev.eq(&c.rev).and(cid.lt(&c.cid.to_string()))),
                    );
                }

                if let Some(s) = since {
                    query = query.filter(repoRev.gt(s));
                }

                query
                    .order((repoRev.desc(), cid.desc()))
                    .load::<RepoBlock>(conn)
            })
            .await
    }

    /// Count total blocks for this repository
    pub async fn count_blocks(&self) -> Result<i64> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                use rsky_pds::schema::pds::repo_block::dsl::*;

                repo_block.filter(did.eq(&did)).count().get_result(conn)
            })
            .await
    }

    /// Proactively cache blocks from a specific revision
    pub async fn cache_rev(&mut self, rev: &str) -> Result<()> {
        let did = self.did.clone();
        let rev_string = rev.to_string();

        let blocks = self
            .db
            .run(move |conn| {
                use rsky_pds::schema::pds::repo_block::dsl::*;

                repo_block
                    .filter(did.eq(&did))
                    .filter(repoRev.eq(&rev_string))
                    .select((cid, content))
                    .limit(15)
                    .load::<(String, Vec<u8>)>(conn)
            })
            .await?;

        let mut cache_guard = self.cache.write().await;
        for (cid_str, content) in blocks {
            let cid = Cid::from_str(&cid_str)?;
            cache_guard.set(cid, content);
        }

        Ok(())
    }

    /// Delete multiple blocks by their CIDs
    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }

        let did = self.did.clone();
        let cid_strings: Vec<String> = cids.into_iter().map(|c| c.to_string()).collect();

        // Process in chunks to avoid too many parameters
        for chunk in cid_strings.chunks(100) {
            let chunk_vec = chunk.to_vec();
            let did_clone = did.clone();

            self.db
                .run(move |conn| {
                    use rsky_pds::schema::pds::repo_block::dsl::*;

                    diesel::delete(repo_block)
                        .filter(did.eq(&did_clone))
                        .filter(cid.eq_any(&chunk_vec))
                        .execute(conn)
                })
                .await?;
        }

        Ok(())
    }

    /// Get the detailed root information
    pub async fn get_root_detailed(&self) -> Result<CidAndRev> {
        let did = self.did.clone();

        let root = self
            .db
            .run(move |conn| {
                use rsky_pds::schema::pds::repo_root::dsl::*;

                repo_root
                    .filter(did.eq(&did))
                    .first::<RepoRoot>(conn)
                    .optional()
            })
            .await?;

        match root {
            Some(r) => Ok(CidAndRev {
                cid: Cid::from_str(&r.cid)?,
                rev: r.rev,
            }),
            None => Err(anyhow::Error::new(RepoRootNotFoundError)),
        }
    }
}

impl ReadableBlockstore for SqlRepoStorage {
    fn get_bytes<'a>(
        &'a self,
        cid: &'a Cid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>>> + Send + Sync + 'a>> {
        let did = self.did.clone();
        let cid = cid.clone();

        Box::pin(async move {
            // Check cache first
            {
                let cache_guard = self.cache.read().await;
                if let Some(cached) = cache_guard.get(cid) {
                    return Ok(Some(cached.clone()));
                }
            }

            // Not in cache, query database
            let cid_str = cid.to_string();
            let result = self
                .db
                .run(move |conn| {
                    use rsky_pds::schema::pds::repo_block::dsl::*;

                    repo_block
                        .filter(did.eq(&did))
                        .filter(cid.eq(&cid_str))
                        .select(content)
                        .first::<Vec<u8>>(conn)
                        .optional()
                })
                .await?;

            // Update cache if found
            if let Some(content) = &result {
                let mut cache_guard = self.cache.write().await;
                cache_guard.set(cid, content.clone());
            }

            Ok(result)
        })
    }

    fn has<'a>(
        &'a self,
        cid: Cid,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + Sync + 'a>> {
        Box::pin(async move {
            let bytes = self.get_bytes(&cid).await?;
            Ok(bytes.is_some())
        })
    }

    fn get_blocks<'a>(
        &'a self,
        cids: Vec<Cid>,
    ) -> Pin<Box<dyn Future<Output = Result<BlocksAndMissing>> + Send + Sync + 'a>> {
        Box::pin(async move {
            // Check cache first
            let cached = {
                let mut cache_guard = self.cache.write().await;
                cache_guard.get_many(cids.clone())?
            };

            if cached.missing.is_empty() {
                return Ok(cached);
            }

            // Prepare data structures for missing blocks
            let missing = CidSet::new(Some(cached.missing.clone()));
            let missing_strings: Vec<String> =
                cached.missing.iter().map(|c| c.to_string()).collect();
            let did = self.did.clone();

            // Create block map for results
            let mut blocks = BlockMap::new();
            let mut missing_set = CidSet::new(Some(cached.missing.clone()));

            // Query database in chunks
            for chunk in missing_strings.chunks(100) {
                let chunk_vec = chunk.to_vec();
                let did_clone = did.clone();

                let rows = self
                    .db
                    .run(move |conn| {
                        use rsky_pds::schema::pds::repo_block::dsl::*;

                        repo_block
                            .filter(did.eq(&did_clone))
                            .filter(cid.eq_any(&chunk_vec))
                            .select((cid, content))
                            .load::<(String, Vec<u8>)>(conn)
                    })
                    .await?;

                // Process results
                for (cid_str, content) in rows {
                    let block_cid = Cid::from_str(&cid_str)?;
                    blocks.set(block_cid, content.clone());
                    missing_set.delete(block_cid);

                    // Update cache
                    let mut cache_guard = self.cache.write().await;
                    cache_guard.set(block_cid, content);
                }
            }

            // Combine with cached blocks
            blocks.add_map(cached.blocks)?;

            Ok(BlocksAndMissing {
                blocks,
                missing: missing_set.to_list(),
            })
        })
    }
}

impl RepoStorage for SqlRepoStorage {
    fn get_root<'a>(&'a self) -> Pin<Box<dyn Future<Output = Option<Cid>> + Send + Sync + 'a>> {
        Box::pin(async move {
            match self.get_root_detailed().await {
                Ok(root) => Some(root.cid),
                Err(_) => None,
            }
        })
    }

    fn put_block<'a>(
        &'a self,
        cid: Cid,
        bytes: Vec<u8>,
        rev: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'a>> {
        let did = self.did.clone();
        let bytes_clone = bytes.clone();

        Box::pin(async move {
            let cid_str = cid.to_string();
            let size = bytes.len() as i32;

            self.db
                .run(move |conn| {
                    use rsky_pds::schema::pds::repo_block::dsl::*;

                    diesel::insert_into(repo_block)
                        .values((
                            did.eq(&did),
                            cid.eq(&cid_str),
                            repoRev.eq(&rev),
                            size.eq(size),
                            content.eq(&bytes),
                        ))
                        .on_conflict_do_nothing()
                        .execute(conn)
                })
                .await?;

            // Update cache
            let mut cache_guard = self.cache.write().await;
            cache_guard.set(cid, bytes_clone);

            Ok(())
        })
    }

    fn put_many<'a>(
        &'a self,
        to_put: BlockMap,
        rev: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'a>> {
        let did = self.did.clone();

        Box::pin(async move {
            if to_put.size() == 0 {
                return Ok(());
            }

            // Prepare blocks for insertion
            let blocks: Vec<(String, String, String, i32, Vec<u8>)> = to_put
                .map
                .iter()
                .map(|(cid, bytes)| {
                    (
                        did.clone(),
                        cid.to_string(),
                        rev.clone(),
                        bytes.0.len() as i32,
                        bytes.0.clone(),
                    )
                })
                .collect();

            // Process in chunks
            for chunk in blocks.chunks(50) {
                let chunk_vec = chunk.to_vec();

                self.db
                    .run(move |conn| {
                        use rsky_pds::schema::pds::repo_block::dsl::*;

                        let values: Vec<_> = chunk_vec
                            .iter()
                            .map(|(did_val, cid_val, rev_val, size_val, content_val)| {
                                (
                                    did.eq(did_val),
                                    cid.eq(cid_val),
                                    repoRev.eq(rev_val),
                                    size.eq(*size_val),
                                    content.eq(content_val),
                                )
                            })
                            .collect();

                        diesel::insert_into(repo_block)
                            .values(&values)
                            .on_conflict_do_nothing()
                            .execute(conn)
                    })
                    .await?;
            }

            // Update cache with all blocks
            {
                let mut cache_guard = self.cache.write().await;
                for (cid, bytes) in &to_put.map {
                    cache_guard.set(*cid, bytes.0.clone());
                }
            }

            Ok(())
        })
    }

    fn update_root<'a>(
        &'a self,
        cid: Cid,
        rev: String,
        is_create: Option<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'a>> {
        let did = self.did.clone();
        let now = self.now.clone();
        let is_create = is_create.unwrap_or(false);

        Box::pin(async move {
            let cid_str = cid.to_string();

            if is_create {
                // Insert new root
                self.db
                    .run(move |conn| {
                        use rsky_pds::schema::pds::repo_root::dsl::*;

                        diesel::insert_into(repo_root)
                            .values((
                                did.eq(&did),
                                cid.eq(&cid_str),
                                rev.eq(&rev),
                                indexedAt.eq(&now),
                            ))
                            .execute(conn)
                    })
                    .await?;
            } else {
                // Update existing root
                self.db
                    .run(move |conn| {
                        use rsky_pds::schema::pds::repo_root::dsl::*;

                        diesel::update(repo_root)
                            .filter(did.eq(&did))
                            .set((cid.eq(&cid_str), rev.eq(&rev), indexedAt.eq(&now)))
                            .execute(conn)
                    })
                    .await?;
            }

            Ok(())
        })
    }

    fn apply_commit<'a>(
        &'a self,
        commit: CommitData,
        is_create: Option<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'a>> {
        Box::pin(async move {
            // Apply commit in three steps
            self.update_root(commit.cid, commit.rev.clone(), is_create)
                .await?;
            self.put_many(commit.new_blocks, commit.rev).await?;
            self.delete_many(commit.removed_cids.to_list()).await?;

            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl AsyncBlockStoreRead for SqlRepoStorage {
    async fn read_block(&mut self, cid: Cid) -> Result<Vec<u8>, BlockstoreError> {
        let bytes = self
            .get_bytes(&cid)
            .await
            .map_err(|e| BlockstoreError::Other(Box::new(e)))?
            .ok_or(BlockstoreError::CidNotFound)?;

        Ok(bytes)
    }

    fn read_block_into(
        &mut self,
        cid: Cid,
        contents: &mut Vec<u8>,
    ) -> impl Future<Output = Result<(), BlockstoreError>> + Send {
        async move {
            let bytes = self.read_block(cid).await?;
            contents.clear();
            contents.extend_from_slice(&bytes);
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl AsyncBlockStoreWrite for SqlRepoStorage {
    fn write_block(
        &mut self,
        codec: u64,
        hash: u64,
        contents: &[u8],
    ) -> impl Future<Output = Result<Cid, BlockstoreError>> + Send {
        let contents = contents.to_vec();
        let rev = self.now.clone();

        async move {
            // Calculate digest based on hash algorithm
            let digest = match hash {
                atrium_repo::blockstore::SHA2_256 => sha2::Sha256::digest(&contents),
                _ => return Err(BlockstoreError::UnsupportedHash(hash)),
            };

            // Create multihash
            let multihash = atrium_repo::Multihash::wrap(hash, &digest)
                .map_err(|_| BlockstoreError::UnsupportedHash(hash))?;

            // Create CID
            let cid = Cid::new_v1(codec, multihash);

            // Store the block
            self.put_block(cid, contents, rev)
                .await
                .map_err(|e| BlockstoreError::Other(Box::new(e)))?;

            Ok(cid)
        }
    }
}
