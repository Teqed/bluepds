//! Server endpoints. (/xrpc/com.atproto.server.*)
use std::{collections::HashMap, str::FromStr as _};

use anyhow::{Context as _, anyhow};
use argon2::{
    Argon2, PasswordHash, PasswordHasher as _, PasswordVerifier as _, password_hash::SaltString,
};
use atrium_api::{
    com::atproto::server,
    types::string::{Datetime, Did, Handle, Tid},
};
use atrium_crypto::keypair::Did as _;
use atrium_repo::{
    Cid, Repository,
    blockstore::{AsyncBlockStoreWrite as _, CarStore, DAG_CBOR, SHA2_256},
};
use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::StatusCode,
    routing::{get, post},
};
use constcat::concat;
use metrics::counter;
use rand::Rng as _;
use sha2::Digest as _;
use uuid::Uuid;

use crate::{
    AppState, Client, Db, Error, Result, RotationKey, SigningKey,
    auth::{self, AuthenticatedUser},
    config::AppConfig,
    firehose::{Commit, FirehoseProducer},
    metrics::AUTH_FAILED,
    plc::{self, PlcOperation, PlcService},
};

/// This is a dummy password that can be used in absence of a real password.
const DUMMY_PASSWORD: &str = "$argon2id$v=19$m=19456,t=2,p=1$En2LAfHjeO0SZD5IUU1Abg$RpS8nHhhqY4qco2uyd41p9Y/1C+Lvi214MAWukzKQMI";

/// Create an invite code.
/// - POST /xrpc/com.atproto.server.createInviteCode
/// ### Request Body
/// - `useCount`: integer
/// - `forAccount`: string (optional)
/// ### Responses
/// - 200 OK: {code: string}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
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

    Ok(Json(
        server::create_invite_code::OutputData {
            code: sqlx::query_scalar!(
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
            .context("failed to create new invite code")?,
        }
        .into(),
    ))
}

#[expect(clippy::too_many_lines, reason = "TODO: refactor")]
/// Create an account. Implemented by PDS.
/// - POST /xrpc/com.atproto.server.createAccount
/// ### Request Body
/// - `email`: string
/// - `handle`: string (required)
/// - `did`: string - Pre-existing atproto DID, being imported to a new account.
/// - `inviteCode`: string
/// - `verificationCode`: string
/// - `verificationPhone`: string
/// - `password`: string - Initial account password. May need to meet instance-specific password strength requirements.
/// - `recoveryKey`: string - DID PLC rotation key (aka, recovery key) to be included in PLC creation operation.
/// - `plcOp`: object
/// ## Responses
/// - 200 OK: {"accessJwt": "string","refreshJwt": "string","handle": "string","did": "string","didDoc": {}}
/// - 400 Bad Request: {error: [`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `InvalidHandle`, `InvalidPassword`, \
///   `InvalidInviteCode`, `HandleNotAvailable`, `UnsupportedDomain`, `UnresolvableDid`, `IncompatibleDidDoc`)}
/// - 401 Unauthorized
async fn create_account(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(rkey): State<RotationKey>,
    State(client): State<Client>,
    State(config): State<AppConfig>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<server::create_account::Input>,
) -> Result<Json<server::create_account::Output>> {
    let email = input.email.as_deref().context("no email provided")?;
    // Hash the user's password.
    let pass = Argon2::default()
        .hash_password(
            input
                .password
                .as_deref()
                .context("no password provided")?
                .as_bytes(),
            SaltString::generate(&mut rand::thread_rng()).as_salt(),
        )
        .context("failed to hash password")?
        .to_string();
    let handle = input.handle.as_str().to_owned();

    // TODO: Handle the account migration flow.
    // Users will hit this endpoint with a service-level authentication token.
    //
    // https://github.com/bluesky-social/pds/blob/main/ACCOUNT_MIGRATION.md

    // TODO: `input.plc_op`
    if input.plc_op.is_some() {
        return Err(Error::unimplemented(anyhow!("plc_op")));
    }

    let recovery_keys = if let Some(ref key) = input.recovery_key {
        // Ensure the provided recovery key is valid.
        if let Err(error) = atrium_crypto::did::parse_did_key(key) {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow::Error::new(error).context("provided recovery key is in invalid format"),
            ));
        }

        // Enroll the user-provided recovery key at a higher priority than our own.
        vec![key.clone(), rkey.did()]
    } else {
        vec![rkey.did()]
    };

    // Begin a new transaction to actually create the user's profile.
    // Unless committed, the transaction will be automatically rolled back.
    let mut tx = db.begin().await.context("failed to begin transaction")?;

    // TODO: Make this its own toggle instead of tied to test mode
    if !config.test {
        let _invite = match input.invite_code {
            Some(ref code) => {
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
    }

    // Account can be created. Synthesize a new DID for the user.
    // https://github.com/did-method-plc/did-method-plc?tab=readme-ov-file#did-creation
    let op = plc::sign_op(
        &rkey,
        PlcOperation {
            typ: "plc_operation".to_owned(),
            rotation_keys: recovery_keys,
            verification_methods: HashMap::from([("atproto".to_owned(), skey.did())]),
            also_known_as: vec![format!("at://{}", input.handle.as_str())],
            services: HashMap::from([(
                "atproto_pds".to_owned(),
                PlcService::Pds {
                    endpoint: format!("https://{}", config.host_name),
                },
            )]),
            prev: None,
        },
    )
    .context("failed to sign genesis op")?;
    let op_bytes = serde_ipld_dagcbor::to_vec(&op).context("failed to encode genesis op")?;

    let did_hash = {
        let digest = base32::encode(
            base32::Alphabet::Rfc4648Lower { padding: false },
            sha2::Sha256::digest(&op_bytes).as_slice(),
        );
        if digest.len() < 24 {
            return Err(anyhow!("digest too short").into());
        }
        #[expect(clippy::string_slice, reason = "digest length confirmed")]
        digest[..24].to_owned()
    };
    let did = format!("did:plc:{did_hash}");

    let doc = tokio::fs::File::create(config.plc.path.join(format!("{did_hash}.car")))
        .await
        .context("failed to create did doc")?;

    let mut plc_doc = CarStore::create(doc)
        .await
        .context("failed to create did doc")?;

    let plc_cid = plc_doc
        .write_block(DAG_CBOR, SHA2_256, &op_bytes)
        .await
        .context("failed to write genesis commit")?
        .to_string();

    if !config.test {
        // Send the new account's data to the PLC directory.
        plc::submit(&client, &did, &op)
            .await
            .context("failed to submit PLC operation to directory")?;
    }

    // Write out an initial commit for the user.
    // https://atproto.com/guides/account-lifecycle
    let (cid, rev, store) = async {
        let file = tokio::fs::File::create_new(config.repo.path.join(format!("{did_hash}.car")))
            .await
            .context("failed to create repo file")?;
        let mut store = CarStore::create(file)
            .await
            .context("failed to create carstore")?;

        let repo_builder = Repository::create(
            &mut store,
            Did::from_str(&did).expect("should be valid DID format"),
        )
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

    let cid_str = cid.to_string();
    let rev_str = rev.as_str();

    _ = sqlx::query!(
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

    // The account is fully created. Commit the SQL transaction to the database.
    tx.commit().await.context("failed to commit transaction")?;

    // Broadcast the identity event now that the new identity is resolvable on the public directory.
    fhp.identity(
        atrium_api::com::atproto::sync::subscribe_repos::IdentityData {
            did: Did::from_str(&did).expect("should be valid DID format"),
            handle: Some(Handle::new(handle).expect("should be valid handle")),
            seq: 0, // Filled by firehose later.
            time: Datetime::now(),
        },
    )
    .await;

    // The new account is now active on this PDS, so we can broadcast the account firehose event.
    fhp.account(
        atrium_api::com::atproto::sync::subscribe_repos::AccountData {
            active: true,
            did: Did::from_str(&did).expect("should be valid DID format"),
            seq: 0,       // Filled by firehose later.
            status: None, // "takedown" / "suspended" / "deactivated"
            time: Datetime::now(),
        },
    )
    .await;

    let did = Did::from_str(&did).expect("should be valid DID format");

    fhp.commit(Commit {
        car: store,
        ops: Vec::new(),
        cid,
        rev: rev.to_string(),
        did: did.clone(),
        pcid: None,
        blobs: Vec::new(),
    })
    .await;

    // Finally, sign some authentication tokens for the new user.
    let token = auth::sign(
        &skey,
        "at+jwt",
        &serde_json::json!({
            "scope": "com.atproto.access",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_signed(chrono::Duration::hours(4)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
        }),
    )
    .context("failed to sign jwt")?;

    let refresh_token = auth::sign(
        &skey,
        "refresh+jwt",
        &serde_json::json!({
            "scope": "com.atproto.refresh",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_days(chrono::Days::new(90)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
        }),
    )
    .context("failed to sign refresh jwt")?;

    Ok(Json(
        server::create_account::OutputData {
            access_jwt: token,
            did,
            did_doc: None,
            handle: input.handle.clone(),
            refresh_jwt: refresh_token,
        }
        .into(),
    ))
}

/// Create an authentication session.
/// - POST /xrpc/com.atproto.server.createSession
/// ### Request Body
/// - `identifier`: string - Handle or other identifier supported by the server for the authenticating user.
/// - `password`: string - Password for the authenticating user.
/// - `authFactorToken` - string (optional)
/// - `allowTakedown` - boolean (optional) - When true, instead of throwing error for takendown accounts, a valid response with a narrow scoped token will be returned
/// ### Responses
/// - 200 OK: {"accessJwt": "string","refreshJwt": "string","handle": "string","did": "string","didDoc": {},"email": "string","emailConfirmed": true,"emailAuthFactor": true,"active": true,"status": "takendown"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `AccountTakedown`, `AuthFactorTokenRequired`]}
/// - 401 Unauthorized
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

    let Some(account) = sqlx::query!(
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
    .context("failed to authenticate")?
    else {
        counter!(AUTH_FAILED).increment(1);

        // SEC: Call argon2's `verify_password` to simulate password verification and discard the result.
        // We do this to avoid exposing a timing attack where attackers can measure the response time to
        // determine whether or not an account exists.
        _ = Argon2::default().verify_password(
            password.as_bytes(),
            &PasswordHash::new(DUMMY_PASSWORD).context("should be valid password hash")?,
        );

        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("failed to validate credentials"),
        ));
    };

    match Argon2::default().verify_password(
        password.as_bytes(),
        &PasswordHash::new(account.password.as_str()).context("invalid password hash in db")?,
    ) {
        Ok(()) => {}
        Err(_e) => {
            counter!(AUTH_FAILED).increment(1);

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
        &serde_json::json!({
            "scope": "com.atproto.access",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_signed(chrono::Duration::hours(4)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
        }),
    )
    .context("failed to sign jwt")?;

    let refresh_token = auth::sign(
        &skey,
        "refresh+jwt",
        &serde_json::json!({
            "scope": "com.atproto.refresh",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_days(chrono::Days::new(90)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
        }),
    )
    .context("failed to sign refresh jwt")?;

    Ok(Json(
        server::create_session::OutputData {
            access_jwt: token,
            refresh_jwt: refresh_token,

            active: Some(true),
            did: Did::from_str(&did).expect("should be valid DID format"),
            did_doc: None,
            email: None,
            email_auth_factor: None,
            email_confirmed: None,
            handle: Handle::new(account.handle).expect("should be valid handle"),
            status: None,
        }
        .into(),
    ))
}

/// Refresh an authentication session. Requires auth using the 'refreshJwt' (not the 'accessJwt').
/// - POST /xrpc/com.atproto.server.refreshSession
/// ### Responses
/// - 200 OK: {"accessJwt": "string","refreshJwt": "string","handle": "string","did": "string","didDoc": {},"active": true,"status": "takendown"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `AccountTakedown`]}
/// - 401 Unauthorized
async fn refresh_session(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    req: Request,
) -> Result<Json<server::refresh_session::Output>> {
    // TODO: store hashes of refresh tokens and enforce single-use
    let auth_token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .context("no authorization header provided")?
        .to_str()
        .ok()
        .and_then(|auth| auth.strip_prefix("Bearer "))
        .context("invalid authentication token")?;

    let (typ, claims) =
        auth::verify(&skey.did(), auth_token).context("failed to verify refresh token")?;
    if typ != "refresh+jwt" {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid refresh token"),
        ));
    }
    if claims
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .context("failed to get `exp`")?
        < chrono::Utc::now().timestamp()
    {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("refresh token expired"),
        ));
    }
    if claims
        .get("aud")
        .and_then(|audience| audience.as_str())
        .context("invalid jwt")?
        != format!("did:web:{}", config.host_name)
    {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid audience"),
        ));
    }

    let did = claims
        .get("sub")
        .and_then(|subject| subject.as_str())
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
        &serde_json::json!({
            "scope": "com.atproto.access",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_signed(chrono::Duration::hours(4)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
        }),
    )
    .context("failed to sign jwt")?;

    let refresh_token = auth::sign(
        &skey,
        "refresh+jwt",
        &serde_json::json!({
            "scope": "com.atproto.refresh",
            "sub": did,
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().checked_add_days(chrono::Days::new(90)).context("should be valid time")?.timestamp(),
            "aud": format!("did:web:{}", config.host_name)
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
            did: Did::new(did.to_owned()).expect("should be valid DID format"),
            did_doc: None,
            handle: Handle::new(user.handle).expect("should be valid handle"),
            status,
        }
        .into(),
    ))
}

/// Get a signed token on behalf of the requesting DID for the requested service.
/// - GET /xrpc/com.atproto.server.getServiceAuth
/// ### Request Query Parameters
/// - `aud`: string - The DID of the service that the token will be used to authenticate with
/// - `exp`: integer (optional) - The time in Unix Epoch seconds that the JWT expires. Defaults to 60 seconds in the future. The service may enforce certain time bounds on tokens depending on the requested scope.
/// - `lxm`: string (optional) - Lexicon (XRPC) method to bind the requested token to
/// ### Responses
/// - 200 OK: {token: string}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `BadExpiration`]}
/// - 401 Unauthorized
async fn get_service_auth(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    Query(input): Query<server::get_service_auth::ParametersData>,
) -> Result<Json<server::get_service_auth::Output>> {
    let user_did = user.did();
    let aud = input.aud.as_str();

    let exp = (chrono::Utc::now().checked_add_signed(chrono::Duration::minutes(1)))
        .context("should be valid expiration datetime")?
        .timestamp();
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

    if let Some(ref lxm) = input.lxm {
        claims = claims
            .as_object_mut()
            .context("should be a valid object")?
            .insert("lxm".to_owned(), serde_json::Value::String(lxm.to_string()))
            .context("should be able to insert lxm into claims")?;
    }

    // Mint a bearer token by signing a JSON web token.
    let token = auth::sign(&skey, "JWT", &claims).context("failed to sign jwt")?;

    Ok(Json(server::get_service_auth::OutputData { token }.into()))
}

/// Get information about the current auth session. Requires auth.
/// - GET /xrpc/com.atproto.server.getSession
/// ### Responses
/// - 200 OK: {"handle": "string","did": "string","email": "string","emailConfirmed": true,"emailAuthFactor": true,"didDoc": {},"active": true,"status": "takendown"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn get_session(
    user: AuthenticatedUser,
    State(db): State<Db>,
) -> Result<Json<server::get_session::Output>> {
    let did = user.did();
    #[expect(clippy::shadow_unrelated, reason = "is related")]
    if let Some(user) = sqlx::query!(
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
    .context("failed to fetch session")?
    {
        let active = user.status == "active";
        let status = if active { None } else { Some(user.status) };

        Ok(Json(
            server::get_session::OutputData {
                active: Some(active),
                did: Did::from_str(&did).expect("should be valid DID format"),
                did_doc: None,
                email: Some(user.email),
                email_auth_factor: None,
                email_confirmed: None,
                handle: Handle::new(user.handle).expect("should be valid handle"),
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

/// Describes the server's account creation requirements and capabilities. Implemented by PDS.
/// - GET /xrpc/com.atproto.server.describeServer
/// ### Responses
/// - 200 OK: {"inviteCodeRequired": true,"phoneVerificationRequired": true,"availableUserDomains": [`string`],"links": {"privacyPolicy": "string","termsOfService": "string"},"contact": {"email": "string"},"did": "string"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn describe_server(
    State(config): State<AppConfig>,
) -> Result<Json<server::describe_server::Output>> {
    Ok(Json(
        server::describe_server::OutputData {
            available_user_domains: vec![],
            contact: None,
            did: Did::from_str(&format!("did:web:{}", config.host_name))
                .expect("should be valid DID format"),
            invite_code_required: Some(true),
            links: None,
            phone_verification_required: Some(false), // email verification
        }
        .into(),
    ))
}

async fn todo() -> Result<()> {
    Err(Error::unimplemented(anyhow!("not implemented")))
}

#[rustfmt::skip]
/// These endpoints are part of the atproto PDS server and account management APIs. \
/// Requests often require authentication and are made directly to the user's own PDS instance.
/// ### Routes
/// - `POST /xrpc/com.atproto.server.createAccount` -> [`create_account`]
/// - `POST /xrpc/com.atproto.server.createInviteCode` -> [`create_invite_code`]
/// - `POST /xrpc/com.atproto.server.createSession` -> [`create_session`]
/// - `GET /xrpc/com.atproto.server.describeServer` -> [`describe_server`]
/// - `GET /xrpc/com.atproto.server.getServiceAuth` -> [`get_service_auth`]
/// - `GET /xrpc/com.atproto.server.getSession` -> [`get_session`]
/// - `POST /xrpc/com.atproto.server.refreshSession` -> [`refresh_session`]
pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route(concat!("/", server::activate_account::NSID),     post(todo))
        .route(concat!("/", server::check_account_status::NSID), post(todo))
        .route(concat!("/", server::confirm_email::NSID),       post(todo))
        .route(concat!("/", server::create_account::NSID),     post(create_account))
        .route(concat!("/", server::create_app_password::NSID), post(todo))
        .route(concat!("/", server::create_invite_code::NSID), post(create_invite_code))
        .route(concat!("/", server::create_invite_codes::NSID), post(todo))
        .route(concat!("/", server::create_session::NSID),     post(create_session))
        .route(concat!("/", server::deactivate_account::NSID), post(todo))
        .route(concat!("/", server::delete_account::NSID),     post(todo))
        .route(concat!("/", server::delete_session::NSID),     post(todo))
        .route(concat!("/", server::describe_server::NSID),     get(describe_server))
        .route(concat!("/", server::get_account_invite_codes::NSID), post(todo))
        .route(concat!("/", server::get_service_auth::NSID),    get(get_service_auth))
        .route(concat!("/", server::get_session::NSID),         get(get_session))
        .route(concat!("/", server::list_app_passwords::NSID), post(todo))
        .route(concat!("/", server::refresh_session::NSID),    post(refresh_session))
        .route(concat!("/", server::request_account_delete::NSID), post(todo))
        .route(concat!("/", server::request_email_confirmation::NSID), post(todo))
        .route(concat!("/", server::request_email_update::NSID), post(todo))
        .route(concat!("/", server::request_password_reset::NSID), post(todo))
        .route(concat!("/", server::reserve_signing_key::NSID), post(todo))
        .route(concat!("/", server::reset_password::NSID),      post(todo))
        .route(concat!("/", server::revoke_app_password::NSID), post(todo))
        .route(concat!("/", server::update_email::NSID),       post(todo))
}
