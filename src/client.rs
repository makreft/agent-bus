//! Blocking HTTP client for the controller, used by the MCP adapter, the hook
//! commands and Marco's commands.

use std::fmt;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::Config;
use crate::model::*;
use crate::util;

#[derive(Debug, Clone)]
pub struct ClientError {
    /// HTTP status, `None` when the controller was not reachable at all.
    pub status: Option<u16>,
    pub code: String,
    pub message: String,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    pub fn is_unauthorized(&self) -> bool {
        self.status == Some(401)
    }
}

pub struct Client {
    http: ureq::Agent,
    base: String,
}

impl Client {
    pub fn new(base: String, timeout: Duration) -> Client {
        let http: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build()
            .into();
        Client {
            http,
            base: base.trim_end_matches('/').to_string(),
        }
    }

    pub fn from_config(cfg: &Config, timeout: Duration) -> Client {
        Client::new(cfg.base_url(), timeout)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn finish<R: DeserializeOwned>(
        &self,
        resp: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    ) -> Result<R, ClientError> {
        let mut resp = resp.map_err(|e| ClientError {
            status: None,
            code: "unreachable".into(),
            message: format!("Controller unter {} nicht erreichbar: {e}", self.base),
        })?;
        let status = resp.status().as_u16();
        let text = resp.body_mut().read_to_string().map_err(|e| ClientError {
            status: Some(status),
            code: "read".into(),
            message: format!("Antwort des Controllers nicht lesbar: {e}"),
        })?;
        if (200..300).contains(&status) {
            return serde_json::from_str(&text).map_err(|e| ClientError {
                status: Some(status),
                code: "decode".into(),
                message: format!("Unerwartete Antwort des Controllers: {e}"),
            });
        }
        Err(match serde_json::from_str::<ErrorBody>(&text) {
            Ok(b) => ClientError {
                status: Some(status),
                code: b.error,
                message: b.message,
            },
            Err(_) => ClientError {
                status: Some(status),
                code: "http".into(),
                message: format!("HTTP {status}: {}", text.trim()),
            },
        })
    }

    fn get<R: DeserializeOwned>(&self, path: &str, token: &str) -> Result<R, ClientError> {
        self.finish(
            self.http
                .get(self.url(path))
                .header("Authorization", format!("Bearer {token}"))
                .call(),
        )
    }

    fn post<B: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        token: &str,
        body: &B,
    ) -> Result<R, ClientError> {
        self.finish(
            self.http
                .post(self.url(path))
                .header("Authorization", format!("Bearer {token}"))
                .send_json(body),
        )
    }

    pub fn health(&self) -> Result<serde_json::Value, ClientError> {
        self.finish(self.http.get(self.url("/health")).call())
    }

    // ----- agent side -----

    pub fn register(
        &self,
        join_token: &str,
        req: &RegisterRequest,
    ) -> Result<RegisterResponse, ClientError> {
        self.post("/v1/register", join_token, req)
    }

    pub fn busy(&self, token: &str) -> Result<OkResponse, ClientError> {
        self.post(
            "/v1/busy",
            token,
            &serde_json::json!({ "at_ms": util::now_ms() }),
        )
    }

    pub fn ready(&self, token: &str, req: &ReadyRequest) -> Result<ReadyResponse, ClientError> {
        self.post("/v1/ready", token, req)
    }

    pub fn bye(&self, token: &str) -> Result<OkResponse, ClientError> {
        self.post("/v1/bye", token, &serde_json::json!({}))
    }

    pub fn send(&self, token: &str, to: &str, body: &str) -> Result<SendResponse, ClientError> {
        self.post(
            "/v1/send",
            token,
            &SendRequest {
                to: to.into(),
                body: body.into(),
            },
        )
    }

    pub fn inbox(&self, token: &str) -> Result<InboxResponse, ClientError> {
        self.get("/v1/inbox", token)
    }

    pub fn sent(&self, token: &str, limit: usize) -> Result<SentResponse, ClientError> {
        self.get(&format!("/v1/sent?limit={limit}"), token)
    }

    pub fn agents(&self, token: &str) -> Result<AgentsResponse, ClientError> {
        self.get("/v1/agents", token)
    }

    // ----- operator side -----

    pub fn admin_agents(&self, op: &str) -> Result<AgentsResponse, ClientError> {
        self.get("/v1/admin/agents", op)
    }

    pub fn admin_events(
        &self,
        op: &str,
        after: u64,
        wait_secs: u64,
    ) -> Result<EventsResponse, ClientError> {
        self.get(
            &format!("/v1/admin/events?after={after}&wait={wait_secs}"),
            op,
        )
    }

    pub fn admin_messages(&self, op: &str, limit: usize) -> Result<MessagesResponse, ClientError> {
        self.get(&format!("/v1/admin/messages?limit={limit}"), op)
    }

    pub fn admin_mute(
        &self,
        op: &str,
        agent: &str,
        muted: bool,
    ) -> Result<OkResponse, ClientError> {
        self.post(
            "/v1/admin/mute",
            op,
            &MuteRequest {
                agent: agent.into(),
                muted,
            },
        )
    }
}
