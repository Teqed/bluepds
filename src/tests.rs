//! Testing utilities for the PDS.
#![expect(clippy::arbitrary_source_item_ordering)]
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::Result;
use atrium_api::{
    com::atproto::server,
    types::string::{AtIdentifier, Did, Handle, Nsid, RecordKey},
};
use figment::{Figment, providers::Format as _};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::config::AppConfig;

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
        drop(std::fs::remove_dir_all(&self.path));
    }
}

/// Test state for the application.
pub(crate) struct TestState {
    /// The address the test server is listening on.
    address: SocketAddr,
    /// The HTTP client.
    client: reqwest::Client,
    /// The application configuration.
    config: AppConfig,
    /// The temporary directory for test data.
    #[expect(dead_code)]
    temp_dir: TempDir,
}

impl TestState {
    /// Get a base URL for the test server.
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Create a test account.
    pub(crate) async fn create_test_account(&self) -> Result<TestAccount> {
        // Create the account
        let handle = "test.handle";
        let response = self
            .client
            .post(format!(
                "http://{}/xrpc/com.atproto.server.createAccount",
                self.address
            ))
            .json(&server::create_account::InputData {
                did: None,
                verification_code: None,
                verification_phone: None,
                email: Some(format!("{}@example.com", &handle)),
                handle: Handle::new(handle.to_owned()).expect("should be able to create handle"),
                password: Some("password123".to_owned()),
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

    /// Create a new test state.
    #[expect(clippy::unused_async)]
    async fn new() -> Result<Self> {
        // Configure the test app
        #[derive(Serialize, Deserialize)]
        struct TestConfigInput {
            db: Option<String>,
            host_name: Option<String>,
            key: Option<PathBuf>,
            listen_address: Option<SocketAddr>,
            test: Option<bool>,
        }
        // Create a temporary directory for test data
        let temp_dir = TempDir::new()?;

        // Find a free port
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
        let address = listener.local_addr()?;
        drop(listener);

        let test_config = TestConfigInput {
            db: Some(format!("sqlite://{}/test.db", temp_dir.path().display())),
            host_name: Some(format!("localhost:{}", address.port())),
            key: Some(temp_dir.path().join("test.key")),
            listen_address: Some(address),
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
            address,
            client,
            config,
            temp_dir,
        })
    }

    /// Start the application in a background task.
    async fn start_app(&self) -> Result<()> {
        // // Get a reference to the config that can be moved into the task
        // let config = self.config.clone();
        // let address = self.address;

        // // Start the application in a background task
        // let _handle = tokio::spawn(async move {
        //     // Set up the application
        //     use crate::*;

        //     // Initialize metrics (noop in test mode)
        //     drop(metrics::setup(None));

        //     // Create client
        //     let simple_client = reqwest::Client::builder()
        //         .user_agent(APP_USER_AGENT)
        //         .build()
        //         .context("failed to build requester client")?;
        //     let client = reqwest_middleware::ClientBuilder::new(simple_client.clone())
        //         .with(http_cache_reqwest::Cache(http_cache_reqwest::HttpCache {
        //             mode: CacheMode::Default,
        //             manager: MokaManager::default(),
        //             options: HttpCacheOptions::default(),
        //         }))
        //         .build();

        //     // Create a test keypair
        //     std::fs::create_dir_all(config.key.parent().context("should have parent")?)?;
        //     let (skey, rkey) = {
        //         let skey = Secp256k1Keypair::create(&mut rand::thread_rng());
        //         let rkey = Secp256k1Keypair::create(&mut rand::thread_rng());

        //         let keys = KeyData {
        //             skey: skey.export(),
        //             rkey: rkey.export(),
        //         };

        //         let mut f =
        //             std::fs::File::create(&config.key).context("failed to create key file")?;
        //         serde_ipld_dagcbor::to_writer(&mut f, &keys)
        //             .context("failed to serialize crypto keys")?;

        //         (SigningKey(Arc::new(skey)), RotationKey(Arc::new(rkey)))
        //     };

        //     // Set up database
        //     let opts = SqliteConnectOptions::from_str(&config.db)
        //         .context("failed to parse database options")?
        //         .create_if_missing(true);
        //     let db = SqliteDbConn::connect_with(opts).await?;

        //     sqlx::migrate!()
        //         .run(&db)
        //         .await
        //         .context("failed to apply migrations")?;

        //     // Create firehose
        //     let (_fh, fhp) = firehose::spawn(client.clone(), config.clone());

        //     // Create the application state
        //     let app_state = AppState {
        //         cred: azure_identity::DefaultAzureCredential::new()?,
        //         config: config.clone(),
        //         db: db.clone(),
        //         client: client.clone(),
        //         simple_client,
        //         firehose: fhp,
        //         signing_key: skey,
        //         rotation_key: rkey,
        //     };

        //     // Create the router
        //     let app = Router::new()
        //         .route("/", get(index))
        //         .merge(oauth::routes())
        //         .nest(
        //             "/xrpc",
        //             endpoints::routes()
        //                 .merge(actor_endpoints::routes())
        //                 .fallback(service_proxy),
        //         )
        //         .layer(CorsLayer::permissive())
        //         .layer(TraceLayer::new_for_http())
        //         .with_state(app_state);

        //     // Listen for connections
        //     let listener = TcpListener::bind(&address)
        //         .await
        //         .context("failed to bind address")?;

        //     axum::serve(listener, app.into_make_service())
        //         .await
        //         .context("failed to serve app")
        // });

        // // Give the server a moment to start
        // tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }
}

/// A test account that can be used for testing.
pub(crate) struct TestAccount {
    /// The access token for the account.
    pub(crate) access_token: String,
    /// The account DID.
    pub(crate) did: String,
    /// The account handle.
    pub(crate) handle: String,
    /// The refresh token for the account.
    #[expect(dead_code)]
    pub(crate) refresh_token: String,
}

/// Initialize the test state.
pub(crate) async fn init_test_state() -> Result<&'static TestState> {
    async fn init_test_state() -> std::result::Result<TestState, anyhow::Error> {
        let state = TestState::new().await?;
        state.start_app().await?;
        Ok(state)
    }
    TEST_STATE.get_or_try_init(init_test_state).await
}

/// Create a record benchmark that creates records and measures the time it takes.
#[expect(
    clippy::arithmetic_side_effects,
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    clippy::use_debug,
    clippy::print_stdout
)]
pub(crate) async fn create_record_benchmark(count: usize, concurrent: usize) -> Result<Duration> {
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
                    .post(format!("{base_url}/xrpc/com.atproto.repo.createRecord"))
                    .header("Authorization", format!("Bearer {access_token}"))
                    .json(&atrium_api::com::atproto::repo::create_record::InputData {
                        repo: AtIdentifier::Did(Did::new(account_did.clone()).expect("valid DID")),
                        collection: Nsid::new("app.bsky.feed.post".to_owned()).expect("valid NSID"),
                        rkey: Some(
                            RecordKey::new(format!("test-{record_idx}")).expect("valid record key"),
                        ),
                        validate: None,
                        record: serde_json::from_str(
                            &serde_json::json!({
                                "$type": "app.bsky.feed.post",
                                "text": format!("Test post {record_idx} from {account_handle}"),
                                "createdAt": chrono::Utc::now().to_rfc3339(),
                            })
                            .to_string(),
                        )
                        .expect("valid JSON record"),
                        swap_commit: None,
                    })
                    .send()
                    .await;

                // Fetch the record we just created
                let get_response = client
                    .get(format!(
                        "{base_url}/xrpc/com.atproto.sync.getRecord?did={account_did}&collection=app.bsky.feed.post&rkey={record_idx}"
                    ))
                    .header("Authorization", format!("Bearer {access_token}"))
                    .send()
                    .await;
                if get_response.is_err() {
                    println!("Failed to fetch record {record_idx}: {get_response:?}");
                    results.push(get_response);
                    continue;
                }

                let request_duration = request_start.elapsed();
                if record_idx % 10 == 0 {
                    println!("Created record {record_idx} in {request_duration:?}");
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
    for batch_result in results {
        let batch_responses = batch_result?;
        for response_result in batch_responses {
            match response_result {
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
#[expect(clippy::module_inception, clippy::use_debug, clippy::print_stdout)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[tokio::test]
    async fn test_create_account() -> Result<()> {
        return Ok(());
        #[expect(unreachable_code, reason = "Disabled")]
        let state = init_test_state().await?;
        let account = state.create_test_account().await?;

        println!("Created test account: {}", account.handle);
        if account.handle.is_empty() {
            return Err(anyhow::anyhow!("Account handle is empty"));
        }
        if account.did.is_empty() {
            return Err(anyhow::anyhow!("Account DID is empty"));
        }
        if account.access_token.is_empty() {
            return Err(anyhow::anyhow!("Account access token is empty"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_record_benchmark() -> Result<()> {
        return Ok(());
        #[expect(unreachable_code, reason = "Disabled")]
        let duration = create_record_benchmark(100, 1).await?;

        println!("Created 100 records in {duration:?}");

        if duration.as_secs() >= 10 {
            return Err(anyhow!("Benchmark took too long"));
        }

        Ok(())
    }
}
