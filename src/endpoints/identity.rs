use std::collections::HashMap;

use anyhow::{anyhow, Context};
use atrium_api::{
    com::atproto::identity,
    types::string::{Datetime, Handle},
};
use atrium_crypto::keypair::Did;
use atrium_repo::blockstore::{AsyncBlockStoreWrite, CarStore, DAG_CBOR, SHA2_256};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use constcat::concat;

use crate::{
    auth::AuthenticatedUser,
    config::AppConfig,
    did,
    firehose::FirehoseProducer,
    plc::{self, PlcOperation, PlcService},
    AppState, Client, Db, Error, Result, RotationKey, SigningKey,
};

async fn resolve_handle(
    State(db): State<Db>,
    State(client): State<Client>,
    Query(input): Query<identity::resolve_handle::Parameters>,
) -> Result<Json<identity::resolve_handle::Output>> {
    let handle = input.handle.as_str();
    if let Ok(did) = sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
        .fetch_one(&db)
        .await
    {
        let did = atrium_api::types::string::Did::new(did).unwrap();
        return Ok(Json(identity::resolve_handle::OutputData { did }.into()));
    }

    // HACK: Query bsky to see if they have this handle cached.
    let r = client
        .get(format!(
            "https://api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={handle}"
        ))
        .send()
        .await
        .context("failed to query upstream server")?
        .json()
        .await
        .context("failed to decode response as JSON")?;

    Ok(Json(r))
}

#[expect(unused_variables, reason = "Not yet implemented")]
async fn request_plc_operation_signature(user: AuthenticatedUser) -> Result<()> {
    todo!()
}

#[expect(unused_variables, reason = "Not yet implemented")]
async fn sign_plc_operation(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(rkey): State<RotationKey>,
    State(config): State<AppConfig>,
    Json(input): Json<identity::sign_plc_operation::Input>,
) -> Result<Json<identity::sign_plc_operation::Output>> {
    todo!()
}

#[expect(clippy::too_many_arguments, reason = "Many parameters are required for this endpoint")]
async fn update_handle(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(rkey): State<RotationKey>,
    State(client): State<Client>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<identity::update_handle::Input>,
) -> Result<()> {
    let handle = input.handle.as_str();
    let did_str = user.did();
    let did = atrium_api::types::string::Did::new(user.did()).unwrap();

    let existing_did = sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
        .fetch_optional(&db)
        .await
        .context("failed to query did count")?;

    if let Some(existing_did) = existing_did {
        if existing_did != did_str {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("attempted to update handle to one that is already in use"),
            ));
        }
    }

    let plc_cid = sqlx::query_scalar!(r#"SELECT plc_root FROM accounts WHERE did = ?"#, did_str)
        .fetch_one(&db)
        .await
        .context("failed to fetch user PLC root")?;

    // Ensure the existing DID is resolvable.
    // If not, we need to register the original handle.
    let _did = did::resolve(&client, did.clone())
        .await
        .with_context(|| format!("failed to resolve DID for {did_str}"))?;

    let op = PlcOperation {
        typ: "plc_operation".to_string(),
        rotation_keys: vec![rkey.did().to_string()],
        verification_methods: HashMap::from([("atproto".to_string(), skey.did().to_string())]),
        also_known_as: vec![input.handle.as_str().to_string()],
        services: HashMap::from([(
            "atproto_pds".to_string(),
            PlcService::Pds {
                endpoint: config.host_name.clone(),
            },
        )]),
        prev: Some(plc_cid),
    };

    let op = plc::sign_op(&rkey, op)
        .await
        .context("failed to sign plc op")?;

    if !config.test {
        plc::submit(&client, did.as_str(), &op)
            .await
            .context("failed to submit PLC operation")?;
    }

    // FIXME: Properly abstract these implementation details.
    let did_hash = did_str.strip_prefix("did:plc:").unwrap();
    let doc = tokio::fs::File::options()
        .read(true)
        .write(true)
        .open(config.plc.path.join(format!("{}.car", did_hash)))
        .await
        .context("failed to open did doc")?;

    let mut plc_doc = CarStore::open(doc)
        .await
        .context("failed to open did carstore")?;

    let op_bytes = serde_ipld_dagcbor::to_vec(&op).context("failed to encode plc op")?;
    let plc_cid = plc_doc
        .write_block(DAG_CBOR, SHA2_256, &op_bytes)
        .await
        .context("failed to write genesis commit")?;

    let cid_str = plc_cid.to_string();

    sqlx::query!(
        r#"UPDATE accounts SET plc_root = ? WHERE did = ?"#,
        cid_str,
        did_str
    )
    .execute(&db)
    .await
    .context("failed to update account PLC root")?;

    // Broadcast the identity event now that the new identity is resolvable on the public directory.
    fhp.identity(
        atrium_api::com::atproto::sync::subscribe_repos::IdentityData {
            did: did.clone(),
            handle: Some(Handle::new(handle.to_string()).unwrap()),
            seq: 0, // Filled by firehose later.
            time: Datetime::now(),
        },
    )
    .await;

    Ok(())
}

#[rustfmt::skip]
pub fn routes() -> Router<AppState> {
    // AP /xrpc/com.atproto.identity.updateHandle
    // AP /xrpc/com.atproto.identity.requestPlcOperationSignature
    // AP /xrpc/com.atproto.identity.signPlcOperation
    // UG /xrpc/com.atproto.identity.resolveHandle
    Router::new()
        .route(concat!("/", identity::update_handle::NSID),                   post(update_handle))
        .route(concat!("/", identity::request_plc_operation_signature::NSID), post(request_plc_operation_signature))
        .route(concat!("/", identity::sign_plc_operation::NSID),              post(sign_plc_operation))
        .route(concat!("/", identity::resolve_handle::NSID),                   get(resolve_handle))
}
