//! Testing utilities for the PDS.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result};
use atrium_api::{
    com::atproto::server,
    types::{
        Unknown,
        string::{AtIdentifier, Did, Handle, Nsid, RecordKey},
    },
};
use figment::{Figment, providers::Format as _};
use futures::future::join_all;
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::{AppState, auth::AuthenticatedUser, config::AppConfig};

/// Global test state, created once for all tests.
pub(crate) static TEST_STATE: OnceCell<TestState> = OnceCell::const_new();

/// A temporary test directory that will be cleaned up when the struct is dropped.
struct TempDir {
    /// The path to the directory.
    path: PathBuf,
}

impl TempDir {
    /// Create a new temporary directory.
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("bluepds-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    /// Get the path to the directory.
    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Test state for the application.
pub(crate) struct TestState {
    /// The temporary directory for test data.
    temp_dir: TempDir,
    /// The address the test server is listening on.
    address: SocketAddr,
    /// The application configuration.
    config: AppConfig,
    /// The HTTP client.
    client: reqwest::Client,
}

impl TestState {
    /// Create a new test state.
    async fn new() -> Result<Self> {
        // Create a temporary directory for test data
        let temp_dir = TempDir::new()?;

        // Find a free port
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
        let address = listener.local_addr()?;
        drop(listener);

        // Configure the test app
        #[derive(Serialize, Deserialize)]
        struct TestConfigInput {
            host_name: Option<String>,
            db: Option<String>,
            listen_address: Option<SocketAddr>,
            key: Option<PathBuf>,
            test: Option<bool>,
        }

        let test_config = TestConfigInput {
            host_name: Some(format!("localhost:{}", address.port())),
            db: Some(format!("sqlite://{}/test.db", temp_dir.path().display())),
            listen_address: Some(address),
            key: Some(temp_dir.path().join("test.key")),
            test: Some(true),
        };

        let config: AppConfig = Figment::new()
            .admerge(figment::providers::Toml::file("default.toml"))
            .admerge(figment::providers::Env::prefixed("BLUEPDS_"))
            .merge(figment::providers::Serialized::defaults(test_config))
            .merge(
                figment::providers::Toml::string(
                    r#"
                [firehose]
                relays = []

                [repo]
                path = "repo"

                [plc]
                path = "plc"

                [blob]
                path = "blob"
                limit = 10485760   # 10 MB
            "#,
                )
                .nested(),
            )
            .extract()?;

        // Create directories
        std::fs::create_dir_all(temp_dir.path().join("repo"))?;
        std::fs::create_dir_all(temp_dir.path().join("plc"))?;
        std::fs::create_dir_all(temp_dir.path().join("blob"))?;

        // Create client
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            temp_dir,
            address,
            config,
            client,
        })
    }

    /// Start the application in a background task.
    async fn start_app(&self) -> Result<()> {
        // Get a reference to the config that can be moved into the task
        let config = self.config.clone();
        let address = self.address;

        // Start the application in a background task
        tokio::spawn(async move {
            // Set up the application
            use crate::*;

            // Initialize metrics (noop in test mode)
            let _ = metrics::setup(None);

            // Create client
            let simple_client = reqwest::Client::builder()
                .user_agent(APP_USER_AGENT)
                .build()
                .context("failed to build requester client")?;
            let client = reqwest_middleware::ClientBuilder::new(simple_client.clone())
                .with(http_cache_reqwest::Cache(http_cache_reqwest::HttpCache {
                    mode: http_cache_reqwest::CacheMode::Default,
                    manager: http_cache_reqwest::MokaManager::default(),
                    options: http_cache_reqwest::HttpCacheOptions::default(),
                }))
                .build();

            // Create a test keypair
            std::fs::create_dir_all(&config.key.parent().context("should have parent")?)?;
            let (skey, rkey) = {
                let skey =
                    atrium_crypto::keypair::Secp256k1Keypair::create(&mut rand::thread_rng());
                let rkey =
                    atrium_crypto::keypair::Secp256k1Keypair::create(&mut rand::thread_rng());

                let keys = KeyData {
                    skey: skey.export(),
                    rkey: rkey.export(),
                };

                let mut f =
                    std::fs::File::create(&config.key).context("failed to create key file")?;
                serde_ipld_dagcbor::to_writer(&mut f, &keys)
                    .context("failed to serialize crypto keys")?;

                (SigningKey(Arc::new(skey)), RotationKey(Arc::new(rkey)))
            };

            // Set up database
            let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&config.db)
                .context("failed to parse database options")?
                .create_if_missing(true);
            let db = sqlx::SqlitePool::connect_with(opts).await?;

            sqlx::migrate!()
                .run(&db)
                .await
                .context("failed to apply migrations")?;

            // Create firehose
            let (_fh, fhp) = firehose::spawn(client.clone(), config.clone());

            // Create the application state
            let app_state = AppState {
                cred: azure_identity::DefaultAzureCredential::new()?,
                config: config.clone(),
                db: db.clone(),
                client: client.clone(),
                simple_client,
                firehose: fhp,
                signing_key: skey,
                rotation_key: rkey,
            };

            // Create the router
            let app = axum::Router::new()
                .route("/", axum::routing::get(crate::index))
                .merge(crate::oauth::routes())
                .nest(
                    "/xrpc",
                    crate::endpoints::routes()
                        .merge(crate::actor_endpoints::routes())
                        .fallback(crate::service_proxy),
                )
                .layer(tower_http::cors::CorsLayer::permissive())
                .layer(tower_http::trace::TraceLayer::new_for_http())
                .with_state(app_state);

            println!("Test server listening on {address}");

            // Listen for connections
            let listener = tokio::net::TcpListener::bind(&address)
                .await
                .context("failed to bind address")?;

            axum::serve(listener, app.into_make_service())
                .await
                .context("failed to serve app")
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    /// Create a test account.
    pub async fn create_test_account(&self) -> Result<TestAccount> {
        let handle = "test.handle";
        println!("Creating test account with handle: {}", handle);

        // Create the account
        let response = self
            .client
            .post(&format!(
                "http://{}/xrpc/com.atproto.server.createAccount",
                self.address
            ))
            .json(&server::create_account::InputData {
                did: None,
                verification_code: None,
                verification_phone: None,
                email: Some(format!("{}@example.com", &handle)),
                handle: Handle::new(handle.to_owned()).expect("should be able to create handle"),
                password: Some("password123".to_string()),
                invite_code: None,
                recovery_key: None,
                plc_op: None,
            })
            .send()
            .await?;

        let account: server::create_account::Output = response.json().await?;

        Ok(TestAccount {
            handle: handle.to_owned(),
            did: account.did.to_string(),
            access_token: account.access_jwt.clone(),
            refresh_token: account.refresh_jwt.clone(),
        })
    }

    /// Get a base URL for the test server.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

/// A test account that can be used for testing.
pub struct TestAccount {
    /// The account handle.
    pub handle: String,
    /// The account DID.
    pub did: String,
    /// The access token for the account.
    pub access_token: String,
    /// The refresh token for the account.
    pub refresh_token: String,
}

/// Initialize the test state.
pub async fn init_test_state() -> Result<&'static TestState> {
    TEST_STATE
        .get_or_try_init(|| async {
            let state = TestState::new().await?;
            state.start_app().await?;
            Ok(state)
        })
        .await
}

/// Create a record benchmark that creates records and measures the time it takes.
pub async fn create_record_benchmark(count: usize, concurrent: usize) -> Result<Duration> {
    // Initialize the test state
    let state = init_test_state().await?;

    // Create a test account
    let account = state.create_test_account().await?;

    // Create the client with authorization
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let start = Instant::now();

    // Split the work into batches
    let mut handles = Vec::new();
    for batch_idx in 0..concurrent {
        let batch_size = count / concurrent;
        let client = client.clone();
        let base_url = state.base_url();
        let account_did = account.did.clone();
        let account_handle = account.handle.clone();
        let access_token = account.access_token.clone();

        let handle = tokio::spawn(async move {
            let mut results = Vec::new();

            for i in 0..batch_size {
                let request_start = Instant::now();
                let record_idx = batch_idx * batch_size + i;

                let result = client
                    .post(&format!("{}/xrpc/com.atproto.repo.createRecord", base_url))
                    .header("Authorization", format!("Bearer {}", access_token))
                    .json(&atrium_api::com::atproto::repo::create_record::InputData {
                        repo: AtIdentifier::Did(Did::new(account_did.clone()).unwrap()),
                        collection: Nsid::new("app.bsky.feed.post".to_string()).unwrap(),
                        rkey: Some(RecordKey::new(format!("test-{}", record_idx)).unwrap()),
                        validate: None,
                        record: serde_json::from_str(
                            &serde_json::json!({
                                "$type": "app.bsky.feed.post",
                                "text": format!("Test post {} from {}", record_idx, account_handle),
                                "createdAt": chrono::Utc::now().to_rfc3339(),
                            })
                            .to_string(),
                        )
                        .unwrap(),
                        swap_commit: None,
                    })
                    .send()
                    .await;

                let request_duration = request_start.elapsed();
                if record_idx % 10 == 0 {
                    println!("Created record {} in {:?}", record_idx, request_duration);
                }
                results.push(result);
            }

            results
        });

        handles.push(handle);
    }

    // Wait for all batches to complete
    let results = join_all(handles).await;

    // Check for errors
    for result in results {
        let batch_results = result?;
        for result in batch_results {
            match result {
                Ok(response) => {
                    if !response.status().is_success() {
                        return Err(anyhow::anyhow!(
                            "Failed to create record: {}",
                            response.status()
                        ));
                    }
                }
                Err(err) => {
                    return Err(anyhow::anyhow!("Failed to create record: {}", err));
                }
            }
        }
    }

    let duration = start.elapsed();
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_account() -> Result<()> {
        let state = init_test_state().await?;
        let account = state.create_test_account().await?;

        println!("Created test account: {}", account.handle);
        assert!(!account.handle.is_empty());
        assert!(!account.did.is_empty());
        assert!(!account.access_token.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_record_benchmark() -> Result<()> {
        let duration = create_record_benchmark(100, 1).await?;

        println!("Created 100 records in {:?}", duration);
        assert!(duration.as_secs() < 100, "Benchmark took too long");

        Ok(())
    }
}
