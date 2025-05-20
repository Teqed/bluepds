//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/pipethrough.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for Axum instead of Rocket

use anyhow::{Result, bail};
use axum::extract::{FromRequestParts, State};
use rsky_identity::IdResolver;
use rsky_pds::apis::ApiError;
use rsky_pds::auth_verifier::{AccessOutput, AccessStandard};
use rsky_pds::config::{ServerConfig, ServiceConfig, env_to_cfg};
use rsky_pds::pipethrough::{OverrideOpts, ProxyHeader, UrlAndAud};
use rsky_pds::xrpc_server::types::{HandlerPipeThrough, InvalidRequestError, XRPCError};
use rsky_pds::{APP_USER_AGENT, SharedIdResolver, context};
// use lazy_static::lazy_static;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, Response};
// use rocket::data::ToByteUnit;
// use rocket::http::{Method, Status};
// use rocket::request::{FromRequest, Outcome, Request};
// use rocket::{Data, State};
use axum::{
    body::Bytes,
    http::{self, HeaderMap},
};
use rsky_common::{GetServiceEndpointOpts, get_service_endpoint};
use rsky_repo::types::Ids;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ubyte::ToByteUnit as _;
use url::Url;

use crate::serve::AppState;

// pub struct OverrideOpts {
//     pub aud: Option<String>,
//     pub lxm: Option<String>,
// }

// pub struct UrlAndAud {
//     pub url: Url,
//     pub aud: String,
//     pub lxm: String,
// }

// pub struct ProxyHeader {
//     pub did: String,
//     pub service_url: String,
// }

pub struct ProxyRequest {
    pub headers: BTreeMap<String, String>,
    pub query: Option<String>,
    pub path: String,
    pub method: Method,
    pub id_resolver: Arc<tokio::sync::RwLock<rsky_identity::IdResolver>>,
    pub cfg: ServerConfig,
}
impl FromRequestParts<AppState> for ProxyRequest {
    // type Rejection = ApiError;
    type Rejection = axum::response::Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let headers = parts
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect::<BTreeMap<String, String>>();
        let query = parts.uri.query().map(|s| s.to_string());
        let path = parts.uri.path().to_string();
        let method = parts.method.clone();
        let id_resolver = state.id_resolver.clone();
        // let cfg = state.cfg.clone();
        let cfg = env_to_cfg(); // TODO: use state.cfg.clone();

        Ok(Self {
            headers,
            query,
            path,
            method,
            id_resolver,
            cfg,
        })
    }
}

// #[rocket::async_trait]
// impl<'r> FromRequest<'r> for HandlerPipeThrough {
//     type Error = anyhow::Error;

//     #[tracing::instrument(skip_all)]
//     async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
//         match AccessStandard::from_request(req).await {
//             Outcome::Success(output) => {
//                 let AccessOutput { credentials, .. } = output.access;
//                 let requester: Option<String> = match credentials {
//                     None => None,
//                     Some(credentials) => credentials.did,
//                 };
//                 let headers = req.headers().clone().into_iter().fold(
//                     BTreeMap::new(),
//                     |mut acc: BTreeMap<String, String>, cur| {
//                         let _ = acc.insert(cur.name().to_string(), cur.value().to_string());
//                         acc
//                     },
//                 );
//                 let proxy_req = ProxyRequest {
//                     headers,
//                     query: match req.uri().query() {
//                         None => None,
//                         Some(query) => Some(query.to_string()),
//                     },
//                     path: req.uri().path().to_string(),
//                     method: req.method(),
//                     id_resolver: req.guard::<&State<SharedIdResolver>>().await.unwrap(),
//                     cfg: req.guard::<&State<ServerConfig>>().await.unwrap(),
//                 };
//                 match pipethrough(
//                     &proxy_req,
//                     requester,
//                     OverrideOpts {
//                         aud: None,
//                         lxm: None,
//                     },
//                 )
//                 .await
//                 {
//                     Ok(res) => Outcome::Success(res),
//                     Err(error) => match error.downcast_ref() {
//                         Some(InvalidRequestError::XRPCError(xrpc)) => {
//                             if let XRPCError::FailedResponse {
//                                 status,
//                                 error,
//                                 message,
//                                 headers,
//                             } = xrpc
//                             {
//                                 tracing::error!(
//                                     "@LOG: XRPC ERROR Status:{status}; Message: {message:?}; Error: {error:?}; Headers: {headers:?}"
//                                 );
//                             }
//                             req.local_cache(|| Some(ApiError::InvalidRequest(error.to_string())));
//                             Outcome::Error((Status::BadRequest, error))
//                         }
//                         _ => {
//                             req.local_cache(|| Some(ApiError::InvalidRequest(error.to_string())));
//                             Outcome::Error((Status::BadRequest, error))
//                         }
//                     },
//                 }
//             }
//             Outcome::Error(err) => {
//                 req.local_cache(|| Some(ApiError::RuntimeError));
//                 Outcome::Error((
//                     Status::BadRequest,
//                     anyhow::Error::new(InvalidRequestError::AuthError(err.1)),
//                 ))
//             }
//             _ => panic!("Unexpected outcome during Pipethrough"),
//         }
//     }
// }

// #[rocket::async_trait]
// impl<'r> FromRequest<'r> for ProxyRequest<'r> {
//     type Error = anyhow::Error;

//     async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
//         let headers = req.headers().clone().into_iter().fold(
//             BTreeMap::new(),
//             |mut acc: BTreeMap<String, String>, cur| {
//                 let _ = acc.insert(cur.name().to_string(), cur.value().to_string());
//                 acc
//             },
//         );
//         Outcome::Success(Self {
//             headers,
//             query: match req.uri().query() {
//                 None => None,
//                 Some(query) => Some(query.to_string()),
//             },
//             path: req.uri().path().to_string(),
//             method: req.method(),
//             id_resolver: req.guard::<&State<SharedIdResolver>>().await.unwrap(),
//             cfg: req.guard::<&State<ServerConfig>>().await.unwrap(),
//         })
//     }
// }

pub async fn pipethrough(
    req: &ProxyRequest,
    requester: Option<String>,
    override_opts: OverrideOpts,
) -> Result<HandlerPipeThrough> {
    let UrlAndAud {
        url,
        aud,
        lxm: nsid,
    } = format_url_and_aud(req, override_opts.aud).await?;
    let lxm = override_opts.lxm.unwrap_or(nsid);
    let headers = format_headers(req, aud, lxm, requester).await?;
    let req_init = format_req_init(req, url, headers, None)?;
    let res = make_request(req_init).await?;
    parse_proxy_res(res).await
}

pub async fn pipethrough_procedure<T: serde::Serialize>(
    req: &ProxyRequest,
    requester: Option<String>,
    body: Option<T>,
) -> Result<HandlerPipeThrough> {
    let UrlAndAud {
        url,
        aud,
        lxm: nsid,
    } = format_url_and_aud(req, None).await?;
    let headers = format_headers(req, aud, nsid, requester).await?;
    let encoded_body: Option<Vec<u8>> = match body {
        None => None,
        Some(body) => Some(serde_json::to_string(&body)?.into_bytes()),
    };
    let req_init = format_req_init(req, url, headers, encoded_body)?;
    let res = make_request(req_init).await?;
    parse_proxy_res(res).await
}

#[tracing::instrument(skip_all)]
pub async fn pipethrough_procedure_post(
    req: &ProxyRequest,
    requester: Option<String>,
    body: Option<Bytes>,
) -> Result<HandlerPipeThrough, ApiError> {
    let UrlAndAud {
        url,
        aud,
        lxm: nsid,
    } = format_url_and_aud(req, None).await?;
    let headers = format_headers(req, aud, nsid, requester).await?;
    let encoded_body: Option<JsonValue>;
    match body {
        None => encoded_body = None,
        Some(body) => {
            // let res = match body.open(50.megabytes()).into_string().await {
            //     Ok(res1) => {
            //         tracing::info!(res1.value);
            //         res1.value
            //     }
            //     Err(error) => {
            //         tracing::error!("{error}");
            //         return Err(ApiError::RuntimeError);
            //     }
            // };
            let res = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");

            match serde_json::from_str(res.as_str()) {
                Ok(res) => {
                    encoded_body = Some(res);
                }
                Err(error) => {
                    tracing::error!("{error}");
                    return Err(ApiError::RuntimeError);
                }
            }
        }
    };
    let req_init = format_req_init_with_value(req, url, headers, encoded_body)?;
    let res = make_request(req_init).await?;
    Ok(parse_proxy_res(res).await?)
}

// Request setup/formatting
// -------------------

const REQ_HEADERS_TO_FORWARD: [&str; 4] = [
    "accept-language",
    "content-type",
    "atproto-accept-labelers",
    "x-bsky-topics",
];

#[tracing::instrument(skip_all)]
pub async fn format_url_and_aud(
    req: &ProxyRequest,
    aud_override: Option<String>,
) -> Result<UrlAndAud> {
    let proxy_to = parse_proxy_header(req).await?;
    let nsid = parse_req_nsid(req);
    let default_proxy = default_service(req, &nsid).await;
    let service_url = match proxy_to {
        Some(ref proxy_to) => {
            tracing::info!(
                "@LOG: format_url_and_aud() proxy_to: {:?}",
                proxy_to.service_url
            );
            Some(proxy_to.service_url.clone())
        }
        None => match default_proxy {
            Some(ref default_proxy) => Some(default_proxy.url.clone()),
            None => None,
        },
    };
    let aud = match aud_override {
        Some(_) => aud_override,
        None => match proxy_to {
            Some(proxy_to) => Some(proxy_to.did),
            None => match default_proxy {
                Some(default_proxy) => Some(default_proxy.did),
                None => None,
            },
        },
    };
    match (service_url, aud) {
        (Some(service_url), Some(aud)) => {
            let mut url = Url::parse(format!("{0}{1}", service_url, req.path).as_str())?;
            if let Some(ref params) = req.query {
                url.set_query(Some(params.as_str()));
            }
            if !req.cfg.service.dev_mode && !is_safe_url(url.clone()) {
                bail!(InvalidRequestError::InvalidServiceUrl(url.to_string()));
            }
            Ok(UrlAndAud {
                url,
                aud,
                lxm: nsid,
            })
        }
        _ => bail!(InvalidRequestError::NoServiceConfigured(req.path.clone())),
    }
}

pub async fn format_headers(
    req: &ProxyRequest,
    aud: String,
    lxm: String,
    requester: Option<String>,
) -> Result<HeaderMap> {
    let mut headers: HeaderMap = match requester {
        Some(requester) => context::service_auth_headers(&requester, &aud, &lxm).await?,
        None => HeaderMap::new(),
    };
    // forward select headers to upstream services
    for header in REQ_HEADERS_TO_FORWARD {
        let val = req.headers.get(header);
        if let Some(val) = val {
            headers.insert(header, HeaderValue::from_str(val)?);
        }
    }
    Ok(headers)
}

pub fn format_req_init(
    req: &ProxyRequest,
    url: Url,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
) -> Result<RequestBuilder> {
    match req.method {
        Method::GET => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.get(url))
        }
        Method::HEAD => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.head(url))
        }
        Method::POST => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.post(url).body(body.unwrap()))
        }
        _ => bail!(InvalidRequestError::MethodNotFound),
    }
}

pub fn format_req_init_with_value(
    req: &ProxyRequest,
    url: Url,
    headers: HeaderMap,
    body: Option<JsonValue>,
) -> Result<RequestBuilder> {
    match req.method {
        Method::GET => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.get(url))
        }
        Method::HEAD => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.head(url))
        }
        Method::POST => {
            let client = Client::builder()
                .user_agent(APP_USER_AGENT)
                .http2_keep_alive_while_idle(true)
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .default_headers(headers)
                .build()?;
            Ok(client.post(url).json(&body.unwrap()))
        }
        _ => bail!(InvalidRequestError::MethodNotFound),
    }
}

pub async fn parse_proxy_header(req: &ProxyRequest) -> Result<Option<ProxyHeader>> {
    let headers = &req.headers;
    let proxy_to: Option<&String> = headers.get("atproto-proxy");
    match proxy_to {
        None => Ok(None),
        Some(proxy_to) => {
            let parts: Vec<&str> = proxy_to.split("#").collect::<Vec<&str>>();
            match (parts.get(0), parts.get(1), parts.get(2)) {
                (Some(did), Some(service_id), None) => {
                    let did = did.to_string();
                    let mut lock = req.id_resolver.write().await;
                    match lock.did.resolve(did.clone(), None).await? {
                        None => bail!(InvalidRequestError::CannotResolveProxyDid),
                        Some(did_doc) => {
                            match get_service_endpoint(
                                did_doc,
                                GetServiceEndpointOpts {
                                    id: format!("#{service_id}"),
                                    r#type: None,
                                },
                            ) {
                                None => bail!(InvalidRequestError::CannotResolveServiceUrl),
                                Some(service_url) => Ok(Some(ProxyHeader { did, service_url })),
                            }
                        }
                    }
                }
                (_, None, _) => bail!(InvalidRequestError::NoServiceId),
                _ => bail!("error parsing atproto-proxy header"),
            }
        }
    }
}

pub fn parse_req_nsid(req: &ProxyRequest) -> String {
    let nsid = req.path.as_str().replace("/xrpc/", "");
    match nsid.ends_with("/") {
        false => nsid,
        true => nsid
            .trim_end_matches(|c| c == nsid.chars().last().unwrap())
            .to_string(),
    }
}

// Sending request
// -------------------
#[tracing::instrument(skip_all)]
pub async fn make_request(req_init: RequestBuilder) -> Result<Response> {
    let res = req_init.send().await;
    match res {
        Err(e) => {
            tracing::error!("@LOG WARN: pipethrough network error {}", e.to_string());
            bail!(InvalidRequestError::XRPCError(XRPCError::UpstreamFailure))
        }
        Ok(res) => match res.error_for_status_ref() {
            Ok(_) => Ok(res),
            Err(_) => {
                let status = res.status().to_string();
                let headers = res.headers().clone();
                let error_body = res.json::<JsonValue>().await?;
                bail!(InvalidRequestError::XRPCError(XRPCError::FailedResponse {
                    status,
                    headers,
                    error: match error_body["error"].as_str() {
                        None => None,
                        Some(error_body_error) => Some(error_body_error.to_string()),
                    },
                    message: match error_body["message"].as_str() {
                        None => None,
                        Some(error_body_message) => Some(error_body_message.to_string()),
                    }
                }))
            }
        },
    }
}

// Response parsing/forwarding
// -------------------

const RES_HEADERS_TO_FORWARD: [&str; 4] = [
    "content-type",
    "content-language",
    "atproto-repo-rev",
    "atproto-content-labelers",
];

pub async fn parse_proxy_res(res: Response) -> Result<HandlerPipeThrough> {
    let encoding = match res.headers().get(CONTENT_TYPE) {
        Some(content_type) => content_type.to_str()?,
        None => "application/json",
    };
    // Release borrow
    let encoding = encoding.to_string();
    let res_headers = RES_HEADERS_TO_FORWARD.into_iter().fold(
        BTreeMap::new(),
        |mut acc: BTreeMap<String, String>, cur| {
            let _ = match res.headers().get(cur) {
                Some(res_header_val) => acc.insert(
                    cur.to_string(),
                    res_header_val.clone().to_str().unwrap().to_string(),
                ),
                None => None,
            };
            acc
        },
    );
    let buffer = read_array_buffer_res(res).await?;
    Ok(HandlerPipeThrough {
        encoding,
        buffer,
        headers: Some(res_headers),
    })
}

// Utils
// -------------------

pub async fn default_service(req: &ProxyRequest, nsid: &str) -> Option<ServiceConfig> {
    let cfg = req.cfg.clone();
    match Ids::from_str(nsid) {
        Ok(Ids::ToolsOzoneTeamAddMember) => cfg.mod_service,
        Ok(Ids::ToolsOzoneTeamDeleteMember) => cfg.mod_service,
        Ok(Ids::ToolsOzoneTeamUpdateMember) => cfg.mod_service,
        Ok(Ids::ToolsOzoneTeamListMembers) => cfg.mod_service,
        Ok(Ids::ToolsOzoneCommunicationCreateTemplate) => cfg.mod_service,
        Ok(Ids::ToolsOzoneCommunicationDeleteTemplate) => cfg.mod_service,
        Ok(Ids::ToolsOzoneCommunicationUpdateTemplate) => cfg.mod_service,
        Ok(Ids::ToolsOzoneCommunicationListTemplates) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationEmitEvent) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationGetEvent) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationGetRecord) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationGetRepo) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationQueryEvents) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationQueryStatuses) => cfg.mod_service,
        Ok(Ids::ToolsOzoneModerationSearchRepos) => cfg.mod_service,
        Ok(Ids::ComAtprotoModerationCreateReport) => cfg.report_service,
        _ => cfg.bsky_app_view,
    }
}

pub fn parse_res<T: DeserializeOwned>(_nsid: String, res: HandlerPipeThrough) -> Result<T> {
    let buffer = res.buffer;
    let record = serde_json::from_slice::<T>(buffer.as_slice())?;
    Ok(record)
}

#[tracing::instrument(skip_all)]
pub async fn read_array_buffer_res(res: Response) -> Result<Vec<u8>> {
    match res.bytes().await {
        Ok(bytes) => Ok(bytes.to_vec()),
        Err(err) => {
            tracing::error!("@LOG WARN: pipethrough network error {}", err.to_string());
            bail!("UpstreamFailure")
        }
    }
}

pub fn is_safe_url(url: Url) -> bool {
    if url.scheme() != "https" {
        return false;
    }
    match url.host_str() {
        None => false,
        Some(hostname) if hostname == "localhost" => false,
        Some(hostname) => {
            if std::net::IpAddr::from_str(hostname).is_ok() {
                return false;
            }
            true
        }
    }
}
