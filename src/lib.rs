//! PDS implementation.
mod account_manager;
mod actor_endpoints;
mod actor_store;
mod apis;
mod auth;
mod config;
mod db;
mod did;
pub mod error;
mod metrics;
mod models;
mod oauth;
mod pipethrough;
mod schema;
mod serve;
mod service_proxy;

pub use serve::run;

/// The index (/) route.
async fn index() -> impl axum::response::IntoResponse {
    r"
         __                         __
        /\ \__                     /\ \__
    __  \ \ ,_\  _____   _ __   ___\ \ ,_\   ___
  /'__'\ \ \ \/ /\ '__'\/\''__\/ __'\ \ \/  / __'\
 /\ \L\.\_\ \ \_\ \ \L\ \ \ \//\ \L\ \ \ \_/\ \L\ \
 \ \__/.\_\\ \__\\ \ ,__/\ \_\\ \____/\ \__\ \____/
  \/__/\/_/ \/__/ \ \ \/  \/_/ \/___/  \/__/\/___/
                   \ \_\
                    \/_/


This is an AT Protocol Personal Data Server (aka, an atproto PDS)

Most API routes are under /xrpc/

      Code: https://github.com/DrChat/bluepds
  Protocol: https://atproto.com
    "
}
