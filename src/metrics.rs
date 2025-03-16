//! Metric name constants.

use std::time::Duration;

use anyhow::Context;
use metrics::{describe_counter, describe_gauge};
use metrics_exporter_prometheus::PrometheusBuilder;

use crate::config;

pub const AUTH_FAILED: &str = "bluepds.auth.failed"; // Counter.

pub const FIREHOSE_HISTORY: &str = "bluepds.firehose.history"; // Gauge.
pub const FIREHOSE_LISTENERS: &str = "bluepds.firehose.listeners"; // Gauge.
pub const FIREHOSE_MESSAGES: &str = "bluepds.firehose.messages"; // Counter.
pub const FIREHOSE_SEQUENCE: &str = "bluepds.firehose.sequence"; // Counter.

pub const REPO_COMMITS: &str = "bluepds.repo.commits"; // Counter.
pub const REPO_OP_CREATE: &str = "bluepds.repo.op.create"; // Counter.
pub const REPO_OP_UPDATE: &str = "bluepds.repo.op.update"; // Counter.
pub const REPO_OP_DELETE: &str = "bluepds.repo.op.delete"; // Counter.

/// Must be ran exactly once on startup. This will declare all of the instruments for `metrics`.
pub fn setup(config: &Option<config::MetricConfig>) -> anyhow::Result<()> {
    describe_counter!(AUTH_FAILED, "The number of failed authentication attempts.");

    describe_gauge!(FIREHOSE_HISTORY, "The size of the firehose history buffer.");
    describe_gauge!(
        FIREHOSE_LISTENERS,
        "The number of active consumers on the firehose."
    );
    describe_counter!(
        FIREHOSE_MESSAGES,
        "All messages that have been broadcast on the firehose."
    );
    describe_counter!(
        FIREHOSE_SEQUENCE,
        "The current sequence number on the firehose."
    );

    describe_counter!(
        REPO_COMMITS,
        "The count of commits created for all repositories."
    );
    describe_counter!(REPO_OP_CREATE, "The count of created records.");
    describe_counter!(REPO_OP_UPDATE, "The count of updated records.");
    describe_counter!(REPO_OP_DELETE, "The count of deleted records.");

    if let Some(config) = config {
        match config {
            config::MetricConfig::PrometheusPush(prometheus_config) => {
                PrometheusBuilder::new()
                    .with_push_gateway(
                        prometheus_config.url.clone(),
                        Duration::from_secs(10),
                        None,
                        None,
                    )
                    .context("failed to set up push gateway")?
                    .install()
                    .context("failed to install metrics exporter")?;
            }
        }
    }

    Ok(())
}
