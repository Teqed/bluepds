//! Moderation-related database table definitions.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Table names for moderation-related entities.
pub const ACTION_TABLE_NAME: &str = "moderation_action";
pub const ACTION_SUBJECT_BLOB_TABLE_NAME: &str = "moderation_action_subject_blob";
pub const REPORT_TABLE_NAME: &str = "moderation_report";
pub const REPORT_RESOLUTION_TABLE_NAME: &str = "moderation_report_resolution";

/// Represents a moderation action.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[sqlx(rename_all = "camelCase")]
pub struct ModerationAction {
    pub id: i32, // Auto-generated ID
    pub action: String,
    pub subject_type: String,
    pub subject_did: String,
    pub subject_uri: Option<String>,
    pub subject_cid: Option<String>,
    pub create_label_vals: Option<String>,
    pub negate_label_vals: Option<String>,
    pub comment: Option<String>,
    pub created_at: String,
    pub created_by: String,
    pub duration_in_hours: Option<i32>,
    pub expires_at: Option<String>,
    pub meta: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Represents a subject blob associated with a moderation action.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[sqlx(rename_all = "camelCase")]
pub struct ModerationActionSubjectBlob {
    pub action_id: i32,
    pub cid: String,
    pub record_uri: String,
}

/// Represents a moderation report.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[sqlx(rename_all = "camelCase")]
pub struct ModerationReport {
    pub id: i32, // Auto-generated ID
    pub subject_type: String,
    pub subject_did: String,
    pub subject_uri: Option<String>,
    pub subject_cid: Option<String>,
    pub reason_type: String,
    pub reason: Option<String>,
    pub reported_by_did: String,
    pub created_at: String,
}

/// Represents a resolution for a moderation report.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[sqlx(rename_all = "camelCase")]
pub struct ModerationReportResolution {
    pub report_id: i32,
    pub action_id: i32,
    pub created_at: String,
    pub created_by: String,
}

/// Represents a partial database schema for moderation-related tables.
pub struct PartialDB {
    pub moderation_action: Vec<ModerationAction>,
    pub moderation_action_subject_blob: Vec<ModerationActionSubjectBlob>,
    pub moderation_report: Vec<ModerationReport>,
    pub moderation_report_resolution: Vec<ModerationReportResolution>,
}
