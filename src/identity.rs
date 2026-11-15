//! Who this agent session is. The launcher sets it through environment variables;
//! the model never picks its own name.
//!
//! - `AGENT_BUS_NAME`: name, for example `jo` (required)
//! - `AGENT_BUS_PROJECT`: project, for example `agent-bus` (required)
//! - `AGENT_BUS_ROLE`: role, defaults to the name
//! - `AGENT_BUS_TOOL`: agent tool, for example `claude-code` or `hermes`

use crate::client::{Client, ClientError};
use crate::config::Config;
use crate::model::RegisterRequest;
use crate::util;

#[derive(Clone, Debug)]
pub struct Identity {
    pub name: String,
    pub project: String,
    pub role: Option<String>,
    pub tool: Option<String>,
}

pub const NOT_CONFIGURED: &str = "agent-bus ist für diese Session nicht eingerichtet: \
AGENT_BUS_NAME und AGENT_BUS_PROJECT fehlen. Marco legt die Identität beim Start der Session fest.";

impl Identity {
    pub fn from_env() -> Option<Identity> {
        let get = |k: &str| {
            std::env::var(k)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Some(Identity {
            name: get("AGENT_BUS_NAME")?,
            project: get("AGENT_BUS_PROJECT")?,
            role: get("AGENT_BUS_ROLE"),
            tool: get("AGENT_BUS_TOOL"),
        })
    }

    /// Registers (idempotent) and returns the agent token.
    pub fn register(&self, cfg: &Config, client: &Client) -> Result<String, ClientError> {
        let join = util::read_secret(&cfg.join_token_path()).map_err(|e| ClientError {
            status: None,
            code: "join_token".into(),
            message: e.to_string(),
        })?;
        let req = RegisterRequest {
            name: self.name.clone(),
            project: self.project.clone(),
            role: self.role.clone(),
            tool: self.tool.clone(),
        };
        Ok(client.register(&join, &req)?.token)
    }
}
