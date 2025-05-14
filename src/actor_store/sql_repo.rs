//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/actor_store/record/mod.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend

use anyhow::Result;
use cidv10::Cid;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Text};
use diesel::*;
use futures::{StreamExt, TryStreamExt, stream};
use rsky_pds::models;
use rsky_pds::models::RepoBlock;
use rsky_repo::block_map::{BlockMap, BlocksAndMissing};
use rsky_repo::car::blocks_to_car_file;
use rsky_repo::cid_set::CidSet;
use rsky_repo::storage::CidAndRev;
use rsky_repo::storage::RepoRootError::RepoRootNotFoundError;
use rsky_repo::storage::readable_blockstore::ReadableBlockstore;
use rsky_repo::storage::types::RepoStorage;
use rsky_repo::types::CommitData;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SqlRepoReader {
    pub cache: Arc<RwLock<BlockMap>>,
    pub db: deadpool_diesel::sqlite::Object,
    pub root: Option<Cid>,
    pub rev: Option<String>,
    pub now: String,
    pub did: String,
}

impl std::fmt::Debug for SqlRepoReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlRepoReader")
            .field("did", &self.did)
            .field("root", &self.root)
            .field("rev", &self.rev)
            .finish()
    }
}

impl ReadableBlockstore for SqlRepoReader {
    fn get_bytes<'life>(
        &'life self,
        cid: &'life Cid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>>> + Send + Sync + 'life>> {
        let did: String = self.did.clone();
        let cid = cid.clone();

        Box::pin(async move {
            use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;
            let cached = {
                let cache_guard = self.cache.read().await;
                cache_guard.get(cid).map(|v| v.clone())
            };
            if let Some(cached_result) = cached {
                return Ok(Some(cached_result.clone()));
            }

            let found: Option<Vec<u8>> = self
                .db
                .interact(move |conn| {
                    RepoBlockSchema::repo_block
                        .filter(RepoBlockSchema::cid.eq(cid.to_string()))
                        .filter(RepoBlockSchema::did.eq(did))
                        .select(RepoBlockSchema::content)
                        .first(conn)
                        .optional()
                })
                .await
                .expect("Failed to get block")?;
            match found {
                None => Ok(None),
                Some(result) => {
                    {
                        let mut cache_guard = self.cache.write().await;
                        cache_guard.set(cid, result.clone());
                    }
                    Ok(Some(result))
                }
            }
        })
    }

    fn has<'life>(
        &'life self,
        cid: Cid,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + Sync + 'life>> {
        Box::pin(async move {
            let got = <Self as ReadableBlockstore>::get_bytes(self, &cid).await?;
            Ok(got.is_some())
        })
    }

    fn get_blocks<'life>(
        &'life self,
        cids: Vec<Cid>,
    ) -> Pin<Box<dyn Future<Output = Result<BlocksAndMissing>> + Send + Sync + 'life>> {
        let did: String = self.did.clone();

        Box::pin(async move {
            use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;
            let cached = {
                let mut cache_guard = self.cache.write().await;
                cache_guard.get_many(cids)?
            };

            if cached.missing.is_empty() {
                return Ok(cached);
            }
            let missing = CidSet::new(Some(cached.missing.clone()));
            let missing_strings: Vec<String> =
                cached.missing.into_iter().map(|c| c.to_string()).collect();

            let blocks = Arc::new(tokio::sync::Mutex::new(BlockMap::new()));
            let missing_set = Arc::new(tokio::sync::Mutex::new(missing));

            let _: Vec<_> = stream::iter(missing_strings.chunks(500))
                .then(|batch| {
                    let this_did = did.clone();
                    let blocks = Arc::clone(&blocks);
                    let missing = Arc::clone(&missing_set);
                    let batch = batch.to_vec(); // Convert to owned Vec

                    async move {
                        // Database query
                        let rows: Vec<(String, Vec<u8>)> = self
                            .db
                            .interact(move |conn| {
                                RepoBlockSchema::repo_block
                                    .filter(RepoBlockSchema::cid.eq_any(batch))
                                    .filter(RepoBlockSchema::did.eq(this_did))
                                    .select((RepoBlockSchema::cid, RepoBlockSchema::content))
                                    .load(conn)
                            })
                            .await
                            .expect("Failed to get blocks")?;

                        // Process rows with locked access
                        let mut blocks = blocks.lock().await;
                        let mut missing = missing.lock().await;

                        for row in rows {
                            let cid = Cid::from_str(&row.0)?; // Proper error handling
                            blocks.set(cid, row.1);
                            missing.delete(cid);
                        }

                        Ok::<(), anyhow::Error>(())
                    }
                })
                .try_collect()
                .await?;

            // Extract values from synchronization primitives
            let mut blocks = Arc::try_unwrap(blocks)
                .expect("Arc still has owners")
                .into_inner();
            let missing = Arc::try_unwrap(missing_set)
                .expect("Arc still has owners")
                .into_inner();

            {
                let mut cache_guard = self.cache.write().await;
                cache_guard.add_map(blocks.clone())?;
            }

            blocks.add_map(cached.blocks)?;

            Ok(BlocksAndMissing {
                blocks,
                missing: missing.to_list(),
            })
        })
    }
}

impl RepoStorage for SqlRepoReader {
    fn get_root<'life>(
        &'life self,
    ) -> Pin<Box<dyn Future<Output = Option<Cid>> + Send + Sync + 'life>> {
        Box::pin(async move {
            match self.get_root_detailed().await {
                Ok(root) => Some(root.cid),
                Err(_) => None,
            }
        })
    }

    fn put_block<'life>(
        &'life self,
        cid: Cid,
        bytes: Vec<u8>,
        rev: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'life>> {
        let did: String = self.did.clone();
        let bytes_cloned = bytes.clone();
        Box::pin(async move {
            use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

            self.db
                .interact(move |conn| {
                    insert_into(RepoBlockSchema::repo_block)
                        .values((
                            RepoBlockSchema::did.eq(did),
                            RepoBlockSchema::cid.eq(cid.to_string()),
                            RepoBlockSchema::repoRev.eq(rev),
                            RepoBlockSchema::size.eq(bytes.len() as i32),
                            RepoBlockSchema::content.eq(bytes),
                        ))
                        .execute(conn)
                })
                .await
                .expect("Failed to put block")?;
            {
                let mut cache_guard = self.cache.write().await;
                cache_guard.set(cid, bytes_cloned);
            }
            Ok(())
        })
    }

    fn put_many<'life>(
        &'life self,
        to_put: BlockMap,
        rev: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'life>> {
        let did: String = self.did.clone();

        Box::pin(async move {
            use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

            let blocks: Vec<RepoBlock> = to_put
                .map
                .iter()
                .map(|(cid, bytes)| RepoBlock {
                    cid: cid.to_string(),
                    did: did.clone(),
                    repo_rev: rev.clone(),
                    size: bytes.0.len() as i32,
                    content: bytes.0.clone(),
                })
                .collect();

            let chunks: Vec<Vec<RepoBlock>> =
                blocks.chunks(50).map(|chunk| chunk.to_vec()).collect();

            for batch in chunks {
                self.db
                    .interact(move |conn| {
                        insert_or_ignore_into(RepoBlockSchema::repo_block)
                            .values(&batch)
                            .execute(conn)
                    })
                    .await
                    .expect("Failed to insert blocks")?;
            }

            Ok(())
        })
    }
    fn update_root<'life>(
        &'life self,
        cid: Cid,
        rev: String,
        is_create: Option<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'life>> {
        let did: String = self.did.clone();
        let now: String = self.now.clone();

        Box::pin(async move {
            use rsky_pds::schema::pds::repo_root::dsl as RepoRootSchema;

            let is_create = is_create.unwrap_or(false);
            if is_create {
                self.db
                    .interact(move |conn| {
                        insert_into(RepoRootSchema::repo_root)
                            .values((
                                RepoRootSchema::did.eq(did),
                                RepoRootSchema::cid.eq(cid.to_string()),
                                RepoRootSchema::rev.eq(rev),
                                RepoRootSchema::indexedAt.eq(now),
                            ))
                            .execute(conn)
                    })
                    .await
                    .expect("Failed to create root")?;
            } else {
                self.db
                    .interact(move |conn| {
                        update(RepoRootSchema::repo_root)
                            .filter(RepoRootSchema::did.eq(did))
                            .set((
                                RepoRootSchema::cid.eq(cid.to_string()),
                                RepoRootSchema::rev.eq(rev),
                                RepoRootSchema::indexedAt.eq(now),
                            ))
                            .execute(conn)
                    })
                    .await
                    .expect("Failed to update root")?;
            }
            Ok(())
        })
    }

    fn apply_commit<'life>(
        &'life self,
        commit: CommitData,
        is_create: Option<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'life>> {
        Box::pin(async move {
            self.update_root(commit.cid, commit.rev.clone(), is_create)
                .await?;
            self.put_many(commit.new_blocks, commit.rev).await?;
            self.delete_many(commit.removed_cids.to_list()).await?;
            Ok(())
        })
    }
}

// Basically handles getting ipld blocks from db
impl SqlRepoReader {
    pub fn new(did: String, now: Option<String>, db: deadpool_diesel::sqlite::Object) -> Self {
        let now = now.unwrap_or_else(rsky_common::now);
        SqlRepoReader {
            cache: Arc::new(RwLock::new(BlockMap::new())),
            root: None,
            rev: None,
            db,
            now,
            did,
        }
    }

    pub async fn get_car_stream(&self, since: Option<String>) -> Result<Vec<u8>> {
        match self.get_root().await {
            None => Err(anyhow::Error::new(RepoRootNotFoundError)),
            Some(root) => {
                let mut car = BlockMap::new();
                let mut cursor: Option<CidAndRev> = None;
                let mut write_rows = |rows: Vec<RepoBlock>| -> Result<()> {
                    for row in rows {
                        car.set(Cid::from_str(&row.cid)?, row.content);
                    }
                    Ok(())
                };
                loop {
                    let res = self.get_block_range(&since, &cursor).await?;
                    write_rows(res.clone())?;
                    if let Some(last_row) = res.last() {
                        cursor = Some(CidAndRev {
                            cid: Cid::from_str(&last_row.cid)?,
                            rev: last_row.repo_rev.clone(),
                        });
                    } else {
                        break;
                    }
                }
                blocks_to_car_file(Some(&root), car).await
            }
        }
    }

    pub async fn get_block_range(
        &self,
        since: &Option<String>,
        cursor: &Option<CidAndRev>,
    ) -> Result<Vec<RepoBlock>> {
        let did: String = self.did.clone();
        let since = since.clone();
        let cursor = cursor.clone();
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        Ok(self
            .db
            .interact(move |conn| {
                let mut builder = RepoBlockSchema::repo_block
                    .select(RepoBlock::as_select())
                    .order((RepoBlockSchema::repoRev.desc(), RepoBlockSchema::cid.desc()))
                    .filter(RepoBlockSchema::did.eq(did))
                    .limit(500)
                    .into_boxed();

                if let Some(cursor) = cursor {
                    // use this syntax to ensure we hit the index
                    builder = builder.filter(
                        sql::<Bool>("((")
                            .bind(RepoBlockSchema::repoRev)
                            .sql(", ")
                            .bind(RepoBlockSchema::cid)
                            .sql(") < (")
                            .bind::<Text, _>(cursor.rev.clone())
                            .sql(", ")
                            .bind::<Text, _>(cursor.cid.to_string())
                            .sql("))"),
                    );
                }
                if let Some(since) = since {
                    builder = builder.filter(RepoBlockSchema::repoRev.gt(since));
                }
                builder.load(conn)
            })
            .await
            .expect("Failed to get block range")?)
    }

    pub async fn count_blocks(&self) -> Result<i64> {
        let did: String = self.did.clone();
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        let res = self
            .db
            .interact(move |conn| {
                RepoBlockSchema::repo_block
                    .filter(RepoBlockSchema::did.eq(did))
                    .count()
                    .get_result(conn)
            })
            .await
            .expect("Failed to count blocks")?;
        Ok(res)
    }

    // Transactors
    // -------------------

    /// Proactively cache all blocks from a particular commit (to prevent multiple roundtrips)
    pub async fn cache_rev(&mut self, rev: String) -> Result<()> {
        let did: String = self.did.clone();
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        let result: Vec<(String, Vec<u8>)> = self
            .db
            .interact(move |conn| {
                RepoBlockSchema::repo_block
                    .filter(RepoBlockSchema::did.eq(did))
                    .filter(RepoBlockSchema::repoRev.eq(rev))
                    .select((RepoBlockSchema::cid, RepoBlockSchema::content))
                    .limit(15)
                    .get_results::<(String, Vec<u8>)>(conn)
            })
            .await
            .expect("Failed to cache rev")?;
        for row in result {
            let mut cache_guard = self.cache.write().await;
            cache_guard.set(Cid::from_str(&row.0)?, row.1)
        }
        Ok(())
    }

    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }
        let did: String = self.did.clone();
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        let cid_strings: Vec<String> = cids.into_iter().map(|c| c.to_string()).collect();
        self.db
            .interact(move |conn| {
                delete(RepoBlockSchema::repo_block)
                    .filter(RepoBlockSchema::did.eq(did))
                    .filter(RepoBlockSchema::cid.eq_any(cid_strings))
                    .execute(conn)
            })
            .await
            .expect("Failed to delete many")?;
        Ok(())
    }

    pub async fn get_root_detailed(&self) -> Result<CidAndRev> {
        let did: String = self.did.clone();
        use rsky_pds::schema::pds::repo_root::dsl as RepoRootSchema;

        let res = self
            .db
            .interact(move |conn| {
                RepoRootSchema::repo_root
                    .filter(RepoRootSchema::did.eq(did))
                    .select(models::RepoRoot::as_select())
                    .first(conn)
            })
            .await
            .expect("Failed to get root")?;

        Ok(CidAndRev {
            cid: Cid::from_str(&res.cid)?,
            rev: res.rev,
        })
    }
}
