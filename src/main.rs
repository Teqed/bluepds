//! BluePDS binary entry point.

use anyhow::Context as _;
use clap::Parser;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments and call into the library's run function
    bluepds::run().await.context("failed to run application")
}