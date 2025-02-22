use std::{collections::HashMap, str::FromStr};

use anyhow::{anyhow, Context};
use argon2::{password_hash::SaltString, Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use atrium_api::{
    com::atproto::server,
    types::string::{Datetime, Did, Handle, Tid},
};
use atrium_crypto::keypair::Did as _;
use atrium_repo::{
    blockstore::{AsyncBlockStoreWrite, CarStore, DAG_CBOR, SHA2_256},
    Cid, Repository,
};
use axum::{
    extract::{Query, Request, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use constcat::concat;
use rand::Rng;
use sha2::Digest;
use tracing::info;
use uuid::Uuid;

use crate::{
    auth::{self, AuthenticatedUser},
    config::AppConfig,
    firehose::{Commit, FirehoseProducer},
    plc::{self, PlcOperation, PlcService},
    AppState, Db, Error, Result, RotationKey, SigningKey,
};

async fn create_invite_code(
    _user: AuthenticatedUser,
    State(db): State<Db>,
    Json(input): Json<server::create_invite_code::Input>,
) -> Result<Json<server::create_invite_code::Output>> {
    let uuid = Uuid::new_v4().to_string();
    let did = input.for_account.as_deref();
    let count = std::cmp::min(input.use_count, 100); // Maximum of 100 uses for any code.

    if count <= 0 {
        return Err(anyhow!("use_count must be greater than 0").into());
    }

    let r: String = sqlx::query_scalar!(
        r#"
        INSERT INTO invites (id, did, count, created_at)
            VALUES (?, ?, ?, datetime('now'))
            RETURNING id
        "#,
        uuid,
        did,
        count,
    )
    .fetch_one(&db)
    .await
    .context("failed to create new invite code")?;

    Ok(Json(
        server::create_invite_code::OutputData { code: r }.into(),
    ))
}

async fn create_account(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(rkey): State<RotationKey>,
    State(client): State<reqwest::Client>,
    State(config): State<AppConfig>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<server::create_account::Input>,
) -> Result<Json<server::create_account::Output>> {
    let email = input.email.as_deref().context("no email provided")?;
    let pass = input.password.as_deref().context("no password provided")?;

    // TODO: `input.plc_op`
    // TODO: `input.recovery_key`
    if input.plc_op.is_some() || input.recovery_key.is_some() {
        return Err(Error::unimplemented(anyhow!("plc_op / recovery_key")));
    }

    // Begin a new transaction to actually create the user's profile.
    // Unless committed, the transaction will be automatically rolled back.
    let mut tx = db.begin().await.context("failed to begin transaction")?;

    let _invite = match &input.invite_code {
        Some(code) => {
            let invite: Option<String> = sqlx::query_scalar!(
                r#"
                UPDATE invites
                    SET count = count - 1
                    WHERE id = ?
                    AND count > 0
                    RETURNING id
                "#,
                code
            )
            .fetch_optional(&mut *tx)
            .await
            .context("failed to check invite code")?;

            invite.context("invalid invite code")?
        }
        None => {
            return Err(anyhow!("invite code required").into());
        }
    };

    // Account can be created. Synthesize a new DID for the user.
    // https://github.com/did-method-plc/did-method-plc?tab=readme-ov-file#did-creation
    let op = PlcOperation {
        typ: "plc_operation".to_string(),
        rotation_keys: vec![rkey.did().to_string()],
        verification_methods: HashMap::from([("atproto".to_string(), skey.did().to_string())]),
        also_known_as: vec![format!("at://{}", input.handle.as_str())],
        services: HashMap::from([(
            "atproto_pds".to_string(),
            PlcService::Pds {
                endpoint: format!("https://{}", config.host_name),
            },
        )]),
        prev: None,
    };

    let op = plc::sign_op(&rkey, op)
        .await
        .context("failed to sign genesis op")?;
    let op_bytes = serde_ipld_dagcbor::to_vec(&op).context("failed to encode genesis op")?;

    let digest = base32::encode(
        base32::Alphabet::Rfc4648Lower { padding: false },
        sha2::Sha256::digest(&op_bytes).as_slice(),
    );

    let did_hash = &digest[..24];
    let did = format!("did:plc:{}", did_hash);

    let doc = tokio::fs::File::create(config.plc.path.join(format!("{}.car", did_hash)))
        .await
        .context("failed to create did doc")?;

    let mut plc_doc = CarStore::create(doc)
        .await
        .context("failed to create did doc")?;

    let plc_cid = plc_doc
        .write_block(DAG_CBOR, SHA2_256, &op_bytes)
        .await
        .context("failed to write genesis commit")?;

    if !config.test {
        // Send the new account's data to the PLC directory.
        plc::submit(&client, &did, &op)
            .await
            .context("failed to submit PLC operation to directory")?;
    }

    // Now hash the user's password.
    let salt = SaltString::generate(&mut rand::thread_rng());
    let pass = Argon2::default()
        .hash_password(pass.as_bytes(), salt.as_salt())
        .context("failed to hash password")?
        .to_string();
    let handle = input.handle.as_str().to_owned();

    // Write out an initial commit for the user.
    // https://atproto.com/guides/account-lifecycle
    let (cid, rev, store) = async {
        let file = tokio::fs::File::create_new(config.repo.path.join(format!("{}.car", did_hash)))
            .await
            .context("failed to create repo file")?;
        let mut store = CarStore::create(file)
            .await
            .context("failed to create carstore")?;

        let repo_builder = Repository::create(&mut store, Did::from_str(&did).unwrap())
            .await
            .context("failed to initialize user repo")?;

        // Sign the root commit.
        let sig = skey
            .sign(&repo_builder.bytes())
            .context("failed to sign root commit")?;
        let mut repo = repo_builder
            .finalize(sig)
            .await
            .context("failed to attach signature to root commit")?;

        let root = repo.root();
        let rev = repo.commit().rev();

        let mut mem = Vec::new();
        let mut firehose_store =
            CarStore::create_with_roots(std::io::Cursor::new(&mut mem), [repo.root()])
                .await
                .context("failed to create temp carstore")?;

        repo.export_into(&mut firehose_store)
            .await
            .context("failed to export repository")?;

        Ok::<(Cid, Tid, Vec<u8>), anyhow::Error>((root, rev, mem))
    }
    .await
    .context("failed to create user repo")?;

    let plc_cid = plc_cid.to_string();
    let cid_str = cid.to_string();
    let rev_str = rev.as_str();

    sqlx::query!(
        r#"
        INSERT INTO accounts (did, email, password, root, plc_root, rev, created_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'));

        INSERT INTO handles (did, handle, created_at)
            VALUES (?, ?, datetime('now'));

        -- Cleanup stale invite codes
        DELETE FROM invites
            WHERE count <= 0;
        "#,
        did,
        email,
        pass,
        cid_str,
        plc_cid,
        rev_str,
        did,
        handle
    )
    .execute(&mut *tx)
    .await
    .context("failed to create new account")?;

    let session_id = Uuid::new_v4().to_string();

    // N.B: Because there are a number of shortcomings with JSON web tokens, I've opted to
    // use classic session identifiers instead. Bluesky's frontend will need to be updated
    // to use a more secure authentication token.
    //
    // Reference: https://datatracker.ietf.org/doc/html/rfc8725
    sqlx::query!(
        r#"
        INSERT OR ROLLBACK INTO sessions (id, did, created_at)
            VALUES (?, ?, datetime('now'))
        "#,
        session_id,
        did
    )
    .execute(&mut *tx)
    .await
    .context("failed to create new session")?;

    // The account is fully created. Commit the SQL transaction to the database.
    tx.commit().await.context("failed to commit transaction")?;

    // Broadcast the identity event now that the new identity is resolvable on the public directory.
    fhp.identity(
        atrium_api::com::atproto::sync::subscribe_repos::IdentityData {
            did: Did::from_str(&did).unwrap(),
            handle: Some(Handle::new(handle).unwrap()),
            seq: 0, // Filled by firehose later.
            time: Datetime::now(),
        },
    )
    .await;

    // The new account is now active on this PDS, so we can broadcast the account firehose event.
    fhp.account(
        atrium_api::com::atproto::sync::subscribe_repos::AccountData {
            active: true,
            did: Did::from_str(&did).unwrap(),
            seq: 0,       // Filled by firehose later.
            status: None, // "takedown" / "suspended" / "deactivated"
            time: Datetime::now(),
        },
    )
    .await;

    info!("new account: {did} {email} {pass}");
    let did = Did::from_str(&did).unwrap();

    fhp.commit(Commit {
        car: store,
        ops: Vec::new(),
        cid: cid,
        rev: rev.to_string(),
        did: did.clone(),
    })
    .await;

    Ok(Json(
        server::create_account::OutputData {
            access_jwt: session_id.clone(),
            did: Did::from_str(&did).unwrap(),
            did_doc: None,
            handle: input.handle.clone(),
            refresh_jwt: session_id.clone(),
        }
        .into(),
    ))
}

async fn create_session(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    Json(input): Json<server::create_session::Input>,
) -> Result<Json<server::create_session::Output>> {
    let handle = &input.identifier;
    let password = &input.password;

    // TODO: `input.allow_takedown`
    // TODO: `input.auth_factor_token`

    let account = sqlx::query!(
        r#"
        WITH LatestHandles AS (
            SELECT did, handle
            FROM handles
            WHERE (did, created_at) IN (
                SELECT did, MAX(created_at) AS max_created_at
                FROM handles
                GROUP BY did
            )
        )
        SELECT a.did, a.password, h.handle
        FROM accounts a
        LEFT JOIN LatestHandles h ON a.did = h.did
        WHERE h.handle = ?
        "#,
        handle
    )
    .fetch_optional(&db)
    .await
    .context("failed to authenticate")?;

    let account = if let Some(account) = account {
        account
    } else {
        // SEC: We need to call the below `verify_password` function even if the account is not valid.
        // This is vulnerable to a timing attack where an attacker can determine if an account exists
        // by timing how long it takes for us to generate a response.
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("failed to validate credentials"),
        ));
    };

    match argon2::Argon2::default().verify_password(
        password.as_bytes(),
        &PasswordHash::new(account.password.as_str()).context("invalid password hash in db")?,
    ) {
        Ok(_) => {}
        Err(_e) => {
            return Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("failed to validate credentials"),
            ));
        }
    }

    let did = account.did;

    let token = auth::sign(
        &skey,
        "at+jwt",
        serde_json::json!({
            "iss": did.clone(),
            "aud": format!("did:web:{}", config.host_name),
            "exp": (chrono::Utc::now() + std::time::Duration::from_secs(2 * 60 * 60)).timestamp()
        }),
    )
    .context("failed to sign jwt")?;

    let refresh_token = auth::sign(
        &skey,
        "refresh+jwt",
        serde_json::json!({
            "iss": did.clone(),
            "aud": format!("did:web:{}", config.host_name),
            "exp": (chrono::Utc::now() + std::time::Duration::from_secs(2 * 60 * 60)).timestamp()
        }),
    )
    .context("failed to sign refresh jwt")?;

    Ok(Json(
        server::create_session::OutputData {
            access_jwt: token,
            refresh_jwt: refresh_token,

            active: Some(true),
            did: Did::from_str(&did).unwrap(),
            did_doc: None,
            email: None,
            email_auth_factor: None,
            email_confirmed: None,
            handle: Handle::new(account.handle).unwrap(),
            status: None,
        }
        .into(),
    ))
}

async fn refresh_session(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    req: Request,
) -> Result<Json<server::refresh_session::Output>> {
    let auth = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .context("no authorization header provided")?;
    let token = auth
        .to_str()
        .ok()
        .and_then(|auth| auth.strip_prefix("Bearer "))
        .context("invalid authentication token")?;

    let (typ, claims) =
        auth::verify(&skey.did(), token).context("failed to verify refresh token")?;
    if typ != "refresh+jwt" {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid refresh token"),
        ));
    }

    let did = claims
        .get("iss")
        .and_then(|s| s.as_str())
        .context("invalid jwt")?;

    let user = sqlx::query!(
        r#"
        SELECT a.status, h.handle
        FROM accounts a
        JOIN handles h ON a.did = h.did
        WHERE a.did = ?
        ORDER BY h.created_at ASC
        LIMIT 1
        "#,
        did
    )
    .fetch_one(&db)
    .await
    .context("failed to fetch user account")?;

    let token = auth::sign(
        &skey,
        "at+jwt",
        serde_json::json!({
            "iss": did,
            "aud": format!("did:web:{}", config.host_name),
            "exp": (chrono::Utc::now() + std::time::Duration::from_secs(2 * 60 * 60)).timestamp()
        }),
    )
    .context("failed to sign jwt")?;

    let refresh_token = auth::sign(
        &skey,
        "refresh+jwt",
        serde_json::json!({
            "iss": format!("did:web:{}", config.host_name),
            "aud": did,
            "exp": (chrono::Utc::now() + std::time::Duration::from_secs(30 * 60 * 60)).timestamp()
        }),
    )
    .context("failed to sign refresh jwt")?;

    let active = user.status == "active";
    let status = if active { None } else { Some(user.status) };

    Ok(Json(
        server::refresh_session::OutputData {
            access_jwt: token,
            refresh_jwt: refresh_token,

            active: Some(active), // TODO?
            did: atrium_api::types::string::Did::new(did.to_string()).unwrap(),
            did_doc: None,
            handle: atrium_api::types::string::Handle::new(user.handle).unwrap(),
            status: status,
        }
        .into(),
    ))
}

async fn get_service_auth(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    Query(input): Query<server::get_service_auth::Parameters>,
) -> Result<Json<server::get_service_auth::Output>> {
    let user_did = user.did();
    let aud = input.aud.as_str();

    let exp = (chrono::Utc::now() + std::time::Duration::from_secs(60)).timestamp();
    let jti = rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();

    let mut claims = serde_json::json!({
        "iss": user_did.as_str(),
        "aud": aud,
        "exp": exp,
        "jti": jti,
    });

    if let Some(lxm) = &input.lxm {
        claims["lxm"] = serde_json::Value::String(lxm.to_string());
    }

    // Mint a bearer token by signing a JSON web token.
    let token = auth::sign(&skey, "JWT", claims).context("failed to sign jwt")?;

    Ok(Json(server::get_service_auth::OutputData { token }.into()))
}

async fn get_session(
    user: AuthenticatedUser,
    State(db): State<Db>,
) -> Result<Json<server::get_session::Output>> {
    let did = user.did();

    let user = sqlx::query!(
        r#"
        SELECT a.email, a.status, (
            SELECT h.handle
            FROM handles h
            WHERE h.did = a.did
            ORDER BY h.created_at ASC
            LIMIT 1
        ) AS handle
        FROM accounts a
        WHERE a.did = ?
        "#,
        did
    )
    .fetch_optional(&db)
    .await
    .context("failed to fetch session")?;

    if let Some(user) = user {
        let active = user.status == "active";
        let status = if active { None } else { Some(user.status) };

        Ok(Json(
            server::get_session::OutputData {
                active: Some(active),
                did: Did::from_str(&did).unwrap(),
                did_doc: None,
                email: Some(user.email),
                email_auth_factor: None,
                email_confirmed: None,
                handle: Handle::new(user.handle).unwrap(),
                status,
            }
            .into(),
        ))
    } else {
        Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("user not found"),
        ))
    }
}

async fn describe_server(
    State(config): State<AppConfig>,
) -> Result<Json<server::describe_server::Output>> {
    Ok(Json(
        server::describe_server::OutputData {
            available_user_domains: vec![],
            contact: None,
            did: Did::from_str(&format!("did:web:{}", config.host_name)).unwrap(),
            invite_code_required: Some(true),
            links: None,
            phone_verification_required: Some(false), // email verification
        }
        .into(),
    ))
}

#[rustfmt::skip]
pub fn routes() -> Router<AppState> {
    // UG /xrpc/com.atproto.server.describeServer
    // UP /xrpc/com.atproto.server.createAccount
    // UP /xrpc/com.atproto.server.createSession
    // AP /xrpc/com.atproto.server.refreshSession
    // AG /xrpc/com.atproto.server.getServiceAuth
    // AG /xrpc/com.atproto.server.getSession
    // AP /xrpc/com.atproto.server.createInviteCode
    Router::new()
        .route(concat!("/", server::describe_server::NSID),     get(describe_server))
        .route(concat!("/", server::create_account::NSID),     post(create_account))
        .route(concat!("/", server::create_session::NSID),     post(create_session))
        .route(concat!("/", server::refresh_session::NSID),    post(refresh_session))
        .route(concat!("/", server::get_service_auth::NSID),    get(get_service_auth))
        .route(concat!("/", server::get_session::NSID),         get(get_session))
        .route(concat!("/", server::create_invite_code::NSID), post(create_invite_code))
}
