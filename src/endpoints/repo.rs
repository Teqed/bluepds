use std::str::FromStr;

use anyhow::Context;
use atrium_api::com::atproto::repo;
use atrium_repo::Repository;
use axum::{extract::State, routing::get, Json, Router};
use constcat::concat;

use crate::{auth::AuthenticatedUser, AppState, Db, Result};

async fn create_record(
    user: AuthenticatedUser,
    State(db): State<Db>,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    let mut storage = user
        .storage()
        .await
        .context("failed to open user storage")?;

    let did = user.did();
    let root = sqlx::query_scalar!(r#"SELECT root FROM accounts WHERE did = ?"#, did)
        .fetch_one(&db)
        .await
        .context("failed to query user root")?;
    let root = atrium_repo::Cid::from_str(&root).unwrap();

    let mut repo = Repository::open(&mut storage, root)
        .await
        .context("failed to open user repo")?;

    Ok(Json(
        repo::create_record::OutputData {
            cid: todo!(),
            commit: None,
            uri: "".to_string(),
            validation_status: Some("unknown".to_string()),
        }
        .into(),
    ))
}

pub fn routes() -> Router<AppState> {
    Router::new().route(concat!("/", repo::create_record::NSID), get(create_record))
}
