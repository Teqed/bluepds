// src/actor_store/preference.rs
use anyhow::Result;
use serde_json::Value;
use sqlx::Row;

use crate::actor_store::db::ActorDb;

#[derive(Clone)]
pub struct PreferenceReader {
    db: ActorDb,
}

impl PreferenceReader {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn get_preferences(&self, namespace: Option<&str>) -> Result<Vec<Value>> {
        let rows = match namespace {
            Some(ns) => {
                let pattern = format!("{}%", ns);
                sqlx::query(
                    "SELECT name, value_json FROM account_pref
                     WHERE name = ? OR name LIKE ?",
                )
                .bind(ns)
                .bind(&pattern)
                .fetch_all(&self.db.pool)
                .await?
            }
            None => {
                sqlx::query("SELECT name, value_json FROM account_pref")
                    .fetch_all(&self.db.pool)
                    .await?
            }
        };

        let mut prefs = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get("name");
            let value_json: String = row.get("value_json");
            if let Ok(value) = serde_json::from_str(&value_json) {
                let mut obj: Value = value;
                if let Value::Object(ref mut map) = obj {
                    map.insert("$type".to_string(), Value::String(name));
                }
                prefs.push(obj);
            }
        }

        Ok(prefs)
    }
}

#[derive(Clone)]
pub struct PreferenceTransactor {
    db: ActorDb,
}

impl PreferenceTransactor {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn put_preferences(&self, values: Vec<Value>, namespace: &str) -> Result<()> {
        self.db.assert_transaction();

        // Delete existing preferences in namespace
        let pattern = format!("{}%", namespace);
        sqlx::query("DELETE FROM account_pref WHERE name = ? OR name LIKE ?")
            .bind(namespace)
            .bind(&pattern)
            .execute(&self.db.pool)
            .await?;

        // Insert new preferences
        for value in values {
            if let Some(type_val) = value.get("$type") {
                if let Some(name) = type_val.as_str() {
                    // Check namespace
                    if name == namespace || name.starts_with(&format!("{}.", namespace)) {
                        let mut pref_value = value.clone();
                        if let Value::Object(ref mut map) = pref_value {
                            map.remove("$type");
                        }

                        let value_json = serde_json::to_string(&pref_value)?;

                        sqlx::query("INSERT INTO account_pref (name, value_json) VALUES (?, ?)")
                            .bind(name)
                            .bind(&value_json)
                            .execute(&self.db.pool)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}
