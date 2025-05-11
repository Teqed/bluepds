//! This module contains utility functions and types for working with SQLite databases using SQLx.

use sqlx::Error;
use std::collections::HashSet;

/// Returns a SQL clause to check if a record is not soft-deleted.
pub fn not_soft_deleted_clause(alias: &str) -> String {
    format!(r#"{}."takedownRef" IS NULL"#, alias)
}

/// Checks if a record is soft-deleted.
pub fn is_soft_deleted(takedown_ref: Option<&str>) -> bool {
    takedown_ref.is_some()
}

/// SQL clause to count all rows.
pub const COUNT_ALL: &str = "COUNT(*)";

/// SQL clause to count distinct rows based on a reference.
pub fn count_distinct(ref_col: &str) -> String {
    format!("COUNT(DISTINCT {})", ref_col)
}

/// Generates a SQL clause for the `excluded` column in an `ON CONFLICT` clause.
pub fn excluded(col: &str) -> String {
    format!("excluded.{}", col)
}

/// Generates a SQL clause for a large `WHERE IN` clause using a hash lookup.
/// # DEPRECATED
/// Use SQLx parameterized queries instead.
#[deprecated = "Use SQLx parameterized queries instead"]
pub fn values_list(vals: &[&str]) -> String {
    let values = vals
        .iter()
        .map(|val| format!("('{}')", val))
        .collect::<Vec<_>>()
        .join(", ");
    format!("(VALUES {})", values)
}

/// Retries an asynchronous SQLite operation with exponential backoff.
pub async fn retry_sqlite<F, Fut, T>(operation: F) -> Result<T, sqlx::Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let max_retries = 60;
    let mut attempt = 0;

    while attempt < max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if is_retryable_sqlite_error(&err) => {
                if let Some(wait_ms) = get_wait_ms_sqlite(attempt, 5000) {
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                    attempt += 1;
                } else {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        }
    }

    Err(sqlx::Error::Protocol("Max retries exceeded".into()))
}

/// Checks if an error is retryable for SQLite.
fn is_retryable_sqlite_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Database(db_err) if {
            let code = db_err.code().unwrap_or_default().to_string();
            RETRY_ERRORS.contains(code.as_str())
        }
    )
}

/// Calculates the wait time for retries based on SQLite's backoff strategy.
fn get_wait_ms_sqlite(attempt: usize, timeout: u64) -> Option<u64> {
    const DELAYS: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
    const TOTALS: [u64; 12] = [0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228];

    if attempt >= DELAYS.len() {
        let delay = DELAYS.last().unwrap();
        let prior = TOTALS.last().unwrap() + delay * (attempt as u64 - (DELAYS.len() as u64 - 1));
        if prior + delay > timeout {
            return None;
        }
        Some(*delay)
    } else {
        let delay = DELAYS[attempt];
        let prior = TOTALS[attempt];
        if prior + delay > timeout {
            None
        } else {
            Some(delay)
        }
    }
}

/// Checks if an error is a unique constraint violation.
pub fn is_err_unique_violation(err: &Error) -> bool {
    matches!(
        err,
        Error::Database(db_err) if {
            let code = db_err.code().unwrap_or_default();
            code == "23505" || code == "SQLITE_CONSTRAINT_UNIQUE"
        }
    )
}

lazy_static::lazy_static! {
    /// Set of retryable SQLite error codes.
    static ref RETRY_ERRORS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("SQLITE_BUSY");
        set.insert("SQLITE_BUSY_SNAPSHOT");
        set.insert("SQLITE_BUSY_RECOVERY");
        set.insert("SQLITE_BUSY_TIMEOUT");
        set
    };
}
