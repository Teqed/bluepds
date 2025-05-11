use std::fmt::Debug;

/// Represents a cursor with primary and secondary parts.
#[derive(Debug, Clone)]
pub struct Cursor {
    pub primary: String,
    pub secondary: String,
}

/// Represents a labeled result with primary and secondary parts.
#[derive(Debug, Clone)]
pub struct LabeledResult {
    pub primary: String,
    pub secondary: String,
}

/// Trait defining the interface for a keyset-paginated cursor.
pub trait GenericKeyset<R, LR: Debug> {
    fn label_result(&self, result: R) -> LR;
    fn labeled_result_to_cursor(&self, labeled: LR) -> Cursor;
    fn cursor_to_labeled_result(&self, cursor: Cursor) -> LR;

    fn pack_from_result(&self, results: Vec<R>) -> Option<String> {
        todo!()
        // results
        //     .last()
        //     .map(|result| self.pack(Some(self.label_result(result.clone()))))
    }

    fn pack(&self, labeled: Option<LR>) -> Option<String> {
        labeled.map(|l| self.pack_cursor(self.labeled_result_to_cursor(l)))
    }

    fn unpack(&self, cursor_str: Option<String>) -> Option<LR> {
        cursor_str
            .and_then(|cursor| self.unpack_cursor(cursor))
            .map(|c| self.cursor_to_labeled_result(c))
    }

    fn pack_cursor(&self, cursor: Cursor) -> String {
        format!("{}::{}", cursor.primary, cursor.secondary)
    }

    fn unpack_cursor(&self, cursor_str: String) -> Option<Cursor> {
        let parts: Vec<&str> = cursor_str.split("::").collect();
        if parts.len() == 2 {
            Some(Cursor {
                primary: parts[0].to_string(),
                secondary: parts[1].to_string(),
            })
        } else {
            None
        }
    }
}

/// A concrete implementation of `GenericKeyset` for time and CID-based pagination.
pub struct TimeCidKeyset;

impl TimeCidKeyset {
    pub fn new() -> Self {
        Self
    }
}

impl GenericKeyset<CreatedAtCidResult, LabeledResult> for TimeCidKeyset {
    fn label_result(&self, result: CreatedAtCidResult) -> LabeledResult {
        LabeledResult {
            primary: result.created_at,
            secondary: result.cid,
        }
    }

    fn labeled_result_to_cursor(&self, labeled: LabeledResult) -> Cursor {
        Cursor {
            primary: labeled.primary,
            secondary: labeled.secondary,
        }
    }

    fn cursor_to_labeled_result(&self, cursor: Cursor) -> LabeledResult {
        LabeledResult {
            primary: cursor.primary,
            secondary: cursor.secondary,
        }
    }
}

/// Represents a database result with created_at and cid fields.
#[derive(Debug, Clone)]
pub struct CreatedAtCidResult {
    pub created_at: String,
    pub cid: String,
}

/// Pagination options for queries.
pub struct PaginationOptions<'a> {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub direction: Option<&'a str>,
    pub try_index: Option<bool>,
}

/// Applies pagination to a query.
pub fn paginate<K>(query: &mut String, opts: PaginationOptions, keyset: &K) -> String
where
    K: GenericKeyset<CreatedAtCidResult, LabeledResult>,
{
    let PaginationOptions {
        limit,
        cursor,
        direction,
        try_index,
    } = opts;

    let direction = direction.unwrap_or("desc");
    let labeled = cursor.and_then(|c| keyset.unpack(Some(c)));
    let keyset_sql = labeled.map(|l| get_sql(&l, direction, try_index.unwrap_or(false)));

    if let Some(sql) = keyset_sql {
        query.push_str(&format!(" WHERE {}", sql));
    }

    if let Some(l) = limit {
        query.push_str(&format!(" LIMIT {}", l));
    }

    query.push_str(&format!(
        " ORDER BY primary {} secondary {}",
        direction, direction
    ));

    query.clone()
}

/// Generates SQL conditions for pagination.
fn get_sql(labeled: &LabeledResult, direction: &str, try_index: bool) -> String {
    if try_index {
        if direction == "asc" {
            format!(
                "(primary, secondary) > ('{}', '{}')",
                labeled.primary, labeled.secondary
            )
        } else {
            format!(
                "(primary, secondary) < ('{}', '{}')",
                labeled.primary, labeled.secondary
            )
        }
    } else {
        if direction == "asc" {
            format!(
                "(primary > '{}' OR (primary = '{}' AND secondary > '{}'))",
                labeled.primary, labeled.primary, labeled.secondary
            )
        } else {
            format!(
                "(primary < '{}' OR (primary = '{}' AND secondary < '{}'))",
                labeled.primary, labeled.primary, labeled.secondary
            )
        }
    }
}
