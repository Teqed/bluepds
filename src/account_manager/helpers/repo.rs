//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/repo.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use anyhow::Result;
use cidv10::Cid;
use deadpool_diesel::{Manager, Pool, sqlite::Object};
use diesel::*;

pub async fn update_root(
    did: String,
    cid: Cid,
    rev: String,
    db: &Pool<Manager<SqliteConnection>, Object>,
) -> Result<()> {
    // @TODO balance risk of a race in the case of a long retry
    use crate::schema::actor_store::repo_root::dsl as RepoRootSchema;

    let now = rsky_common::now();

    _ = db
        .get()
        .await?
        .interact(move |conn| {
            insert_into(RepoRootSchema::repo_root)
                .values((
                    RepoRootSchema::did.eq(did),
                    RepoRootSchema::cid.eq(cid.to_string()),
                    RepoRootSchema::rev.eq(rev.clone()),
                    RepoRootSchema::indexedAt.eq(now),
                ))
                .on_conflict(RepoRootSchema::did)
                .do_update()
                .set((
                    RepoRootSchema::cid.eq(cid.to_string()),
                    RepoRootSchema::rev.eq(rev),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to update repo root")?;

    Ok(())
}
