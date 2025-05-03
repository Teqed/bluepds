//! Identity endpoints (/xrpc/com.atproto.identity.*)
use std::collections::HashMap;

use anyhow::{Context as _, anyhow};
use atrium_api::{
    com::atproto::identity,
    types::string::{Datetime, Handle},
};
use atrium_crypto::keypair::Did as _;
use atrium_repo::blockstore::{AsyncBlockStoreWrite as _, CarStore, DAG_CBOR, SHA2_256};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use constcat::concat;

use crate::{
    AppState, Client, Db, Error, Result, RotationKey, SigningKey,
    auth::AuthenticatedUser,
    config::AppConfig,
    did,
    firehose::FirehoseProducer,
    plc::{self, PlcOperation, PlcService},
};

/// (GET) Resolves an atproto handle (hostname) to a DID. Does not necessarily bi-directionally verify against the the DID document.
/// ### Query Parameters
/// - handle: The handle to resolve.
/// ### Responses
/// - 200 OK: {did: did}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `HandleNotFound`]}
/// - 401 Unauthorized
async fn resolve_handle(
    State(db): State<Db>,
    State(client): State<Client>,
    Query(input): Query<identity::resolve_handle::ParametersData>,
) -> Result<Json<identity::resolve_handle::Output>> {
    let handle = input.handle.as_str();
    if let Ok(did) = sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
        .fetch_one(&db)
        .await
    {
        return Ok(Json(
            identity::resolve_handle::OutputData {
                did: atrium_api::types::string::Did::new(did).expect("should be valid DID format"),
            }
            .into(),
        ));
    }

    // HACK: Query bsky to see if they have this handle cached.
    let response = client
        .get(format!(
            "https://api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={handle}"
        ))
        .send()
        .await
        .context("failed to query upstream server")?
        .json()
        .await
        .context("failed to decode response as JSON")?;

    Ok(Json(response))
}

#[expect(unused_variables, clippy::todo, reason = "Not yet implemented")]
/// Request an email with a code to in order to request a signed PLC operation. Requires Auth.
/// - POST /xrpc/com.atproto.identity.requestPlcOperationSignature
/// ### Responses
/// - 200 OK
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn request_plc_operation_signature(user: AuthenticatedUser) -> Result<()> {
    todo!()
}

#[expect(unused_variables, clippy::todo, reason = "Not yet implemented")]
/// Signs a PLC operation to update some value(s) in the requesting DID's document.
/// - POST /xrpc/com.atproto.identity.signPlcOperation
/// ### Request Body
/// - token: string // A token received through com.atproto.identity.requestPlcOperationSignature
/// - rotationKeys: string[]
/// - alsoKnownAs: string[]
/// - verificationMethods: services
/// ### Responses
/// - 200 OK: {operation: string}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn sign_plc_operation(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(rkey): State<RotationKey>,
    State(config): State<AppConfig>,
    Json(input): Json<identity::sign_plc_operation::Input>,
) -> Result<Json<identity::sign_plc_operation::Output>> {
    todo!()
}

#[expect(
    clippy::too_many_arguments,
    reason = "Many parameters are required for this endpoint"
)]
/// Updates the current account's handle. Verifies handle validity, and updates did:plc document if necessary. Implemented by PDS, and requires auth.
/// - POST /xrpc/com.atproto.identity.updateHandle
/// ### Query Parameters
/// - handle: handle // The new handle.
/// ### Responses
/// - 200 OK
/// ## Errors
/// - If the handle is already in use.
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
/// ## Panics
/// - If the handle is not valid.
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
    let did = atrium_api::types::string::Did::new(user.did()).expect("should be valid DID format");

    if let Some(existing_did) =
        sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
            .fetch_optional(&db)
            .await
            .context("failed to query did count")?
    {
        if existing_did != did_str {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("attempted to update handle to one that is already in use"),
            ));
        }
    }

    // Ensure the existing DID is resolvable.
    // If not, we need to register the original handle.
    let _did = did::resolve(&client, did.clone())
        .await
        .with_context(|| format!("failed to resolve DID for {did_str}"))
        .context("should be able to resolve DID")?;

    let op = plc::sign_op(
        &rkey,
        PlcOperation {
            typ: "plc_operation".to_owned(),
            rotation_keys: vec![rkey.did()],
            verification_methods: HashMap::from([("atproto".to_owned(), skey.did())]),
            also_known_as: vec![input.handle.as_str().to_owned()],
            services: HashMap::from([(
                "atproto_pds".to_owned(),
                PlcService::Pds {
                    endpoint: config.host_name.clone(),
                },
            )]),
            prev: Some(
                sqlx::query_scalar!(r#"SELECT plc_root FROM accounts WHERE did = ?"#, did_str)
                    .fetch_one(&db)
                    .await
                    .context("failed to fetch user PLC root")?,
            ),
        },
    )
    .context("failed to sign plc op")?;

    if !config.test {
        plc::submit(&client, did.as_str(), &op)
            .await
            .context("failed to submit PLC operation")?;
    }

    // FIXME: Properly abstract these implementation details.
    let did_hash = did_str
        .strip_prefix("did:plc:")
        .context("should be valid DID format")?;
    let doc = tokio::fs::File::options()
        .read(true)
        .write(true)
        .open(config.plc.path.join(format!("{did_hash}.car")))
        .await
        .context("failed to open did doc")?;

    let op_bytes = serde_ipld_dagcbor::to_vec(&op).context("failed to encode plc op")?;

    let plc_cid = CarStore::open(doc)
        .await
        .context("failed to open did carstore")?
        .write_block(DAG_CBOR, SHA2_256, &op_bytes)
        .await
        .context("failed to write genesis commit")?;

    let cid_str = plc_cid.to_string();

    _ = sqlx::query!(
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
            handle: Some(Handle::new(handle.to_owned()).expect("should be valid handle")),
            seq: 0, // Filled by firehose later.
            time: Datetime::now(),
        },
    )
    .await;

    Ok(())
}

async fn todo() -> Result<()> {
    Err(Error::unimplemented(anyhow!("not implemented")))
}

#[rustfmt::skip]
/// Identity endpoints (/xrpc/com.atproto.identity.*)
/// ### Routes
/// - AP /xrpc/com.atproto.identity.updateHandle                    -> [`update_handle`]
/// - AP /xrpc/com.atproto.identity.requestPlcOperationSignature    -> [`request_plc_operation_signature`]
/// - AP /xrpc/com.atproto.identity.signPlcOperation                -> [`sign_plc_operation`]
/// - UG /xrpc/com.atproto.identity.resolveHandle                   -> [`resolve_handle`]
pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route(concat!("/", identity::get_recommended_did_credentials::NSID), get(todo))
        .route(concat!("/", identity::request_plc_operation_signature::NSID), post(request_plc_operation_signature))
        .route(concat!("/", identity::resolve_handle::NSID),                   get(resolve_handle))
        .route(concat!("/", identity::sign_plc_operation::NSID),              post(sign_plc_operation))
        .route(concat!("/", identity::submit_plc_operation::NSID),            post(todo))
        .route(concat!("/", identity::update_handle::NSID),                   post(update_handle))
}
