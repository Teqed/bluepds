//! BluePDS binary entry point.

use anyhow::Context as _;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    bluepds::run().await.context("failed to run application")
}
