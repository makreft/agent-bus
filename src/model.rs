//! Data types shared by the controller, the HTTP API and the clients.

use serde::{Deserialize, Serialize};

/// Maximum length of names, projects, roles and tool labels.
pub const IDENT_MAX: usize = 32;

/// Lowercase ASCII letters, digits, `-` and `_`, starting with a letter or digit.
pub fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= IDENT_MAX
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

/// An agent is addressed as `name@project`.
pub fn agent_key(name: &str, project: &str) -> String {
    format!("{name}@{project}")
}

/// Persistent record of an agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Agent {
    pub name: String,
    pub project: String,
    pub role: String,
    pub tool: String,
    pub token: String,
    pub registered_at: u64,
    pub last_seen: u64,
    /// Highest message id this agent has read through the inbox.
    pub read_cursor: u64,
    /// Highest message id this agent has been woken for.
    pub notified_cursor: u64,
    pub muted: bool,
}

impl Agent {
    pub fn key(&self) -> String {
        agent_key(&self.name, &self.project)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub body: String,
    pub sent_at: u64,
    /// When the recipient was woken for this message (read receipt, part 1).
    #[serde(default)]
    pub woken_at: Option<u64>,
    /// When the recipient read it through the inbox (read receipt, part 2).
    #[serde(default)]
    pub read_at: Option<u64>,
}

/// What an agent is doing right now, as far as the controller knows.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Activity {
    /// No contact for a while, or the agent said goodbye.
    Offline,
    /// A turn is running. Messages wait in the inbox.
    Busy,
    /// Done and holding a ready connection: a message wakes it right away.
    Ready,
    /// Done, but nothing is waiting on the controller: messages wait for the next turn.
    Idle,
}

/// Entry in the live event log that Marco can follow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub at: u64,
    pub event: EventKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    Registered {
        agent: String,
        tool: String,
        role: String,
    },
    Activity {
        agent: String,
        activity: Activity,
    },
    Message {
        id: u64,
        from: String,
        to: String,
        body: String,
    },
    Woken {
        agent: String,
        count: usize,
    },
    Read {
        agent: String,
        ids: Vec<u64>,
    },
    Rejected {
        from: String,
        to: String,
        reason: String,
    },
    Muted {
        agent: String,
        muted: bool,
    },
    Expired {
        agents: Vec<String>,
        messages: usize,
    },
}

// ----- HTTP API -----

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub project: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub tool: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub agent: String,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendRequest {
    pub to: String,
    pub body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendResponse {
    pub id: u64,
    pub to: String,
    pub recipient_activity: Activity,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboxResponse {
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentInfo {
    pub agent: String,
    pub role: String,
    pub tool: String,
    pub activity: Activity,
    pub last_seen: u64,
    pub muted: bool,
    pub unread: usize,
    /// Of `unread`, how many come from whoever asked. 0 in Marco's view.
    #[serde(default)]
    pub unread_from_you: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentsResponse {
    pub agents: Vec<AgentInfo>,
}

/// Identifies one ready process of an agent. A later `started_ms` wins.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WaiterId {
    pub started_ms: u64,
    pub nonce: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadyMode {
    /// Wake only for messages the agent has not been woken for yet.
    New,
    /// Wake for every unread message, also ones announced before (after a restart).
    Unread,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadyRequest {
    pub waiter: WaiterId,
    pub mode: ReadyMode,
    /// How long the controller holds the request open. 0 checks once and returns.
    pub timeout_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReadyResponse {
    /// Wake up: there are messages. The bodies come from the inbox.
    Messages { count: usize, from: Vec<String> },
    /// Nothing arrived in time. Ask again.
    Timeout,
    /// A newer ready process of the same agent took over. Exit quietly.
    Superseded,
    /// The agent started a turn after this ready process began. Exit quietly.
    Busy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MuteRequest {
    pub agent: String,
    pub muted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsResponse {
    /// Changes when the controller restarts, so followers can reset.
    pub boot: String,
    pub events: Vec<Event>,
}

/// A message as its sender sees it: where it stands.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SentInfo {
    pub id: u64,
    pub to: String,
    pub sent_at: u64,
    pub woken_at: Option<u64>,
    pub read_at: Option<u64>,
    /// The beginning of the body, so the sender knows which message is meant.
    pub preview: String,
    pub recipient_activity: Activity,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SentResponse {
    pub messages: Vec<SentInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagesResponse {
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OkResponse {
    pub ok: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idents() {
        assert!(valid_ident("jo"));
        assert!(valid_ident("agent-bus"));
        assert!(valid_ident("claude_code2"));
        assert!(!valid_ident(""));
        assert!(!valid_ident("Jo"));
        assert!(!valid_ident("-jo"));
        assert!(!valid_ident("jo@x"));
        assert!(!valid_ident("jo bus"));
        assert!(!valid_ident(&"a".repeat(33)));
    }

    #[test]
    fn waiter_order() {
        let a = WaiterId {
            started_ms: 1,
            nonce: "z".into(),
        };
        let b = WaiterId {
            started_ms: 2,
            nonce: "a".into(),
        };
        assert!(a < b);
    }
}
