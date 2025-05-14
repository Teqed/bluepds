//! Record storage and retrieval for the actor store.
//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/actor_store/record/mod.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend

use anyhow::{Error, Result, bail};
use cidv10::Cid;
use diesel::*;
use futures::stream::{self, StreamExt};
use rsky_lexicon::com::atproto::admin::StatusAttr;
use rsky_pds::actor_store::record::{GetRecord, RecordsForCollection, get_backlinks};
use rsky_pds::models::{Backlink, Record, RepoBlock};
use rsky_repo::types::{RepoRecord, WriteOpAction};
use rsky_repo::util::cbor_to_lex_record;
use rsky_syntax::aturi::AtUri;
use std::env;
use std::str::FromStr;

use crate::actor_store::db::ActorDb;

/// Combined handler for record operations with both read and write capabilities.
pub(crate) struct RecordReader {
    /// Database connection.
    pub db: ActorDb,
    /// DID of the actor.
    pub did: String,
}

impl RecordReader {
    /// Create a new record handler.
    pub(crate) fn new(did: String, db: ActorDb) -> Self {
        Self { did, db }
    }

    /// Count the total number of records.
    pub(crate) async fn record_count(&mut self) -> Result<i64> {
        use rsky_pds::schema::pds::record::dsl::*;

        let other_did = self.did.clone();
        self.db
            .run(move |conn| {
                let res: i64 = record.filter(did.eq(&other_did)).count().get_result(conn)?;
                Ok(res)
            })
            .await
    }

    /// List all collections in the repository.
    pub(crate) async fn list_collections(&self) -> Result<Vec<String>> {
        use rsky_pds::schema::pds::record::dsl::*;

        let other_did = self.did.clone();
        self.db
            .run(move |conn| {
                let collections = record
                    .filter(did.eq(&other_did))
                    .select(collection)
                    .group_by(collection)
                    .load::<String>(conn)?
                    .into_iter()
                    .collect::<Vec<String>>();
                Ok(collections)
            })
            .await
    }

    /// List records for a specific collection.
    pub(crate) async fn list_records_for_collection(
        &mut self,
        collection: String,
        limit: i64,
        reverse: bool,
        cursor: Option<String>,
        rkey_start: Option<String>,
        rkey_end: Option<String>,
        include_soft_deleted: Option<bool>,
    ) -> Result<Vec<RecordsForCollection>> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        let include_soft_deleted: bool = if let Some(include_soft_deleted) = include_soft_deleted {
            include_soft_deleted
        } else {
            false
        };
        let mut builder = RecordSchema::record
            .inner_join(RepoBlockSchema::repo_block.on(RepoBlockSchema::cid.eq(RecordSchema::cid)))
            .limit(limit)
            .select((Record::as_select(), RepoBlock::as_select()))
            .filter(RecordSchema::did.eq(self.did.clone()))
            .filter(RecordSchema::collection.eq(collection))
            .into_boxed();
        if !include_soft_deleted {
            builder = builder.filter(RecordSchema::takedownRef.is_null());
        }
        if reverse {
            builder = builder.order(RecordSchema::rkey.asc());
        } else {
            builder = builder.order(RecordSchema::rkey.desc());
        }

        if let Some(cursor) = cursor {
            if reverse {
                builder = builder.filter(RecordSchema::rkey.gt(cursor));
            } else {
                builder = builder.filter(RecordSchema::rkey.lt(cursor));
            }
        } else {
            if let Some(rkey_start) = rkey_start {
                builder = builder.filter(RecordSchema::rkey.gt(rkey_start));
            }
            if let Some(rkey_end) = rkey_end {
                builder = builder.filter(RecordSchema::rkey.lt(rkey_end));
            }
        }
        let res: Vec<(Record, RepoBlock)> = self.db.run(move |conn| builder.load(conn)).await?;
        res.into_iter()
            .map(|row| {
                Ok(RecordsForCollection {
                    uri: row.0.uri,
                    cid: row.0.cid,
                    value: cbor_to_lex_record(row.1.content)?,
                })
            })
            .collect::<Result<Vec<RecordsForCollection>>>()
    }

    /// Get a specific record by URI.
    pub(crate) async fn get_record(
        &mut self,
        uri: &AtUri,
        cid: Option<String>,
        include_soft_deleted: Option<bool>,
    ) -> Result<Option<GetRecord>> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;
        use rsky_pds::schema::pds::repo_block::dsl as RepoBlockSchema;

        let include_soft_deleted: bool = if let Some(include_soft_deleted) = include_soft_deleted {
            include_soft_deleted
        } else {
            false
        };
        let mut builder = RecordSchema::record
            .inner_join(RepoBlockSchema::repo_block.on(RepoBlockSchema::cid.eq(RecordSchema::cid)))
            .select((Record::as_select(), RepoBlock::as_select()))
            .filter(RecordSchema::uri.eq(uri.to_string()))
            .into_boxed();
        if !include_soft_deleted {
            builder = builder.filter(RecordSchema::takedownRef.is_null());
        }
        if let Some(cid) = cid {
            builder = builder.filter(RecordSchema::cid.eq(cid));
        }
        let record: Option<(Record, RepoBlock)> = self
            .db
            .run(move |conn| builder.first(conn).optional())
            .await?;
        if let Some(record) = record {
            Ok(Some(GetRecord {
                uri: record.0.uri,
                cid: record.0.cid,
                value: cbor_to_lex_record(record.1.content)?,
                indexed_at: record.0.indexed_at,
                takedown_ref: record.0.takedown_ref,
            }))
        } else {
            Ok(None)
        }
    }

    /// Check if a record exists.
    pub(crate) async fn has_record(
        &mut self,
        uri: String,
        cid: Option<String>,
        include_soft_deleted: Option<bool>,
    ) -> Result<bool> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        let include_soft_deleted: bool = if let Some(include_soft_deleted) = include_soft_deleted {
            include_soft_deleted
        } else {
            false
        };
        let mut builder = RecordSchema::record
            .select(RecordSchema::uri)
            .filter(RecordSchema::uri.eq(uri))
            .into_boxed();
        if !include_soft_deleted {
            builder = builder.filter(RecordSchema::takedownRef.is_null());
        }
        if let Some(cid) = cid {
            builder = builder.filter(RecordSchema::cid.eq(cid));
        }
        let record_uri = self
            .db
            .run(move |conn| builder.first::<String>(conn).optional())
            .await?;
        Ok(!!record_uri.is_some())
    }

    /// Get the takedown status of a record.
    pub(crate) async fn get_record_takedown_status(
        &self,
        uri: String,
    ) -> Result<Option<StatusAttr>> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        let res = self
            .db
            .run(move |conn| {
                RecordSchema::record
                    .select(RecordSchema::takedownRef)
                    .filter(RecordSchema::uri.eq(uri))
                    .first::<Option<String>>(conn)
                    .optional()
            })
            .await?;
        if let Some(res) = res {
            if let Some(takedown_ref) = res {
                Ok(Some(StatusAttr {
                    applied: true,
                    r#ref: Some(takedown_ref),
                }))
            } else {
                Ok(Some(StatusAttr {
                    applied: false,
                    r#ref: None,
                }))
            }
        } else {
            Ok(None)
        }
    }

    /// Get the current CID for a record URI.
    pub(crate) async fn get_current_record_cid(&self, uri: String) -> Result<Option<Cid>> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        let res = self
            .db
            .run(move |conn| {
                RecordSchema::record
                    .select(RecordSchema::cid)
                    .filter(RecordSchema::uri.eq(uri))
                    .first::<String>(conn)
                    .optional()
            })
            .await?;
        if let Some(res) = res {
            Ok(Some(Cid::from_str(&res)?))
        } else {
            Ok(None)
        }
    }

    /// Get backlinks for a record.
    pub(crate) async fn get_record_backlinks(
        &self,
        collection: String,
        path: String,
        link_to: String,
    ) -> Result<Vec<Record>> {
        use rsky_pds::schema::pds::backlink::dsl as BacklinkSchema;
        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        let res = self
            .db
            .run(move |conn| {
                RecordSchema::record
                    .inner_join(
                        BacklinkSchema::backlink.on(BacklinkSchema::uri.eq(RecordSchema::uri)),
                    )
                    .select(Record::as_select())
                    .filter(BacklinkSchema::path.eq(path))
                    .filter(BacklinkSchema::linkTo.eq(link_to))
                    .filter(RecordSchema::collection.eq(collection))
                    .load::<Record>(conn)
            })
            .await?;
        Ok(res)
    }

    /// Get backlink conflicts for a record.
    pub(crate) async fn get_backlink_conflicts(
        &self,
        uri: &AtUri,
        record: &RepoRecord,
    ) -> Result<Vec<AtUri>> {
        let record_backlinks = get_backlinks(uri, record)?;
        let conflicts: Vec<Vec<Record>> = stream::iter(record_backlinks)
            .then(|backlink| async move {
                Ok::<Vec<Record>, Error>(
                    self.get_record_backlinks(
                        uri.get_collection(),
                        backlink.path,
                        backlink.link_to,
                    )
                    .await?,
                )
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(conflicts
            .into_iter()
            .flatten()
            .filter_map(|record| {
                AtUri::make(
                    env::var("BLUEPDS_HOST_NAME").unwrap_or("localhost".to_owned()),
                    Some(String::from(uri.get_collection())),
                    Some(record.rkey),
                )
                .ok()
            })
            .collect::<Vec<AtUri>>())
    }

    // Transactor methods
    // -----------------

    /// Index a record in the database.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn index_record(
        &self,
        uri: AtUri,
        cid: Cid,
        record: Option<RepoRecord>,
        action: Option<WriteOpAction>, // Create or update with a default of create
        repo_rev: String,
        timestamp: Option<String>,
    ) -> Result<()> {
        tracing::debug!("@LOG DEBUG RecordReader::index_record, indexing record {uri}");

        let collection = uri.get_collection();
        let rkey = uri.get_rkey();
        let hostname = uri.get_hostname().to_string();
        let action = action.unwrap_or(WriteOpAction::Create);
        let indexed_at = timestamp.unwrap_or_else(|| rsky_common::now());
        let row = Record {
            did: self.did.clone(),
            uri: uri.to_string(),
            cid: cid.to_string(),
            collection: collection.clone(),
            rkey: rkey.to_string(),
            repo_rev: Some(repo_rev.clone()),
            indexed_at: indexed_at.clone(),
            takedown_ref: None,
        };

        if !hostname.starts_with("did:") {
            bail!("Expected indexed URI to contain DID")
        } else if collection.is_empty() {
            bail!("Expected indexed URI to contain a collection")
        } else if rkey.is_empty() {
            bail!("Expected indexed URI to contain a record key")
        }

        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        // Track current version of record
        let (record, uri) = self
            .db
            .run(move |conn| {
                insert_into(RecordSchema::record)
                    .values(row)
                    .on_conflict(RecordSchema::uri)
                    .do_update()
                    .set((
                        RecordSchema::cid.eq(cid.to_string()),
                        RecordSchema::repoRev.eq(&repo_rev),
                        RecordSchema::indexedAt.eq(&indexed_at),
                    ))
                    .execute(conn)?;
                Ok::<_, Error>((record, uri))
            })
            .await?;

        if let Some(record) = record {
            // Maintain backlinks
            let backlinks = get_backlinks(&uri, &record)?;
            if let WriteOpAction::Update = action {
                // On update just recreate backlinks from scratch for the record, so we can clear out
                // the old ones. E.g. for weird cases like updating a follow to be for a different did.
                self.remove_backlinks_by_uri(&uri).await?;
            }
            self.add_backlinks(backlinks).await?;
        }
        tracing::debug!("@LOG DEBUG RecordReader::index_record, indexed record {uri}");
        Ok(())
    }

    /// Delete a record from the database.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn delete_record(&self, uri: &AtUri) -> Result<()> {
        tracing::debug!("@LOG DEBUG RecordReader::delete_record, deleting indexed record {uri}");
        use rsky_pds::schema::pds::backlink::dsl as BacklinkSchema;
        use rsky_pds::schema::pds::record::dsl as RecordSchema;
        let uri = uri.to_string();
        self.db
            .run(move |conn| {
                delete(RecordSchema::record)
                    .filter(RecordSchema::uri.eq(&uri))
                    .execute(conn)?;
                delete(BacklinkSchema::backlink)
                    .filter(BacklinkSchema::uri.eq(&uri))
                    .execute(conn)?;
                tracing::debug!(
                    "@LOG DEBUG RecordReader::delete_record, deleted indexed record {uri}"
                );
                Ok(())
            })
            .await
    }

    /// Remove backlinks for a URI.
    pub(crate) async fn remove_backlinks_by_uri(&self, uri: &AtUri) -> Result<()> {
        use rsky_pds::schema::pds::backlink::dsl as BacklinkSchema;
        let uri = uri.to_string();
        self.db
            .run(move |conn| {
                delete(BacklinkSchema::backlink)
                    .filter(BacklinkSchema::uri.eq(uri))
                    .execute(conn)?;
                Ok(())
            })
            .await
    }

    /// Add backlinks to the database.
    pub(crate) async fn add_backlinks(&self, backlinks: Vec<Backlink>) -> Result<()> {
        if backlinks.len() == 0 {
            Ok(())
        } else {
            use rsky_pds::schema::pds::backlink::dsl as BacklinkSchema;
            self.db
                .run(move |conn| {
                    insert_into(BacklinkSchema::backlink)
                        .values(&backlinks)
                        .on_conflict_do_nothing()
                        .execute(conn)?;
                    Ok(())
                })
                .await
        }
    }

    /// Update the takedown status of a record.
    pub(crate) async fn update_record_takedown_status(
        &self,
        uri: &AtUri,
        takedown: StatusAttr,
    ) -> Result<()> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;

        let takedown_ref: Option<String> = match takedown.applied {
            true => match takedown.r#ref {
                Some(takedown_ref) => Some(takedown_ref),
                None => Some(rsky_common::now()),
            },
            false => None,
        };
        let uri_string = uri.to_string();

        self.db
            .run(move |conn| {
                update(RecordSchema::record)
                    .filter(RecordSchema::uri.eq(uri_string))
                    .set(RecordSchema::takedownRef.eq(takedown_ref))
                    .execute(conn)?;
                Ok(())
            })
            .await
    }
}
