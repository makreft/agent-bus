//! The controller's core: registry, agent activity, mailboxes and guardrails.
//!
//! Synchronous and free of HTTP, so it can be tested directly. The HTTP layer in
//! `server.rs` only translates requests and holds ready connections open.
//!
//! Activity model, independent of any agent tool:
//! - `register`: "I am here".
//! - `busy`: "I am working". Messages wait in the inbox.
//! - `ready`: "I am done". The agent keeps a request open; when a message arrives the
//!   controller answers it, and the tool adapter on the agent side wakes the agent.
//!
//! Ordering between `busy` and `ready` uses the agent machine's clock (`at_ms`,
//! `started_ms`), so a late network packet cannot flip an agent back to ready.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

use crate::config::Limits;
use crate::display;
use crate::model::*;
use crate::store::Store;
use crate::util::{self, BoxError};

const EVENT_BUFFER: usize = 2000;
/// Longest time the controller holds one ready request open.
pub const READY_POLL_MAX_SECS: u64 = 50;
/// A ready process counts as connected this long after its last poll.
const READY_GRACE_MS: u64 = (READY_POLL_MAX_SECS + 20) * 1000;

#[derive(Debug, Clone, PartialEq)]
pub enum BusError {
    Unauthorized,
    Forbidden(String),
    NotFound(String),
    BadRequest(String),
    TooLarge(String),
    RateLimited { retry_after_secs: u64 },
    Unavailable,
    Internal(String),
}

impl BusError {
    pub fn code(&self) -> &'static str {
        match self {
            BusError::Unauthorized => "unauthorized",
            BusError::Forbidden(_) => "forbidden",
            BusError::NotFound(_) => "not_found",
            BusError::BadRequest(_) => "bad_request",
            BusError::TooLarge(_) => "too_large",
            BusError::RateLimited { .. } => "rate_limited",
            BusError::Unavailable => "unavailable",
            BusError::Internal(_) => "internal",
        }
    }

    pub fn message(&self) -> String {
        match self {
            BusError::Unauthorized => "Nicht angemeldet oder ungültiges Token.".into(),
            BusError::Forbidden(m)
            | BusError::NotFound(m)
            | BusError::BadRequest(m)
            | BusError::TooLarge(m) => m.clone(),
            BusError::RateLimited { retry_after_secs } => format!(
                "Zu viele Nachrichten. Die nächste geht frühestens in {retry_after_secs} s. \
                 Schreib nur, wenn es etwas zu koordinieren gibt."
            ),
            BusError::Unavailable => "Der Controller fährt gerade herunter.".into(),
            BusError::Internal(m) => format!("Interner Fehler im Controller: {m}"),
        }
    }
}

impl From<BoxError> for BusError {
    fn from(e: BoxError) -> Self {
        BusError::Internal(e.to_string())
    }
}

/// Result of one check of a ready request.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadyCheck {
    Fire { count: usize, from: Vec<String> },
    Superseded,
    Busy,
    Nothing,
}

#[derive(Default)]
struct Runtime {
    busy: bool,
    /// Agent clock (ms) of the last busy report or wake.
    busy_mark: u64,
    bye: bool,
    waiter: Option<WaiterId>,
    /// Controller clock (ms) of the last poll by the current waiter.
    waiter_last_poll: u64,
}

struct Bucket {
    tokens: f64,
    last: u64,
}

struct State {
    agents: HashMap<String, Agent>,
    by_token: HashMap<String, String>,
    runtime: HashMap<String, Runtime>,
    messages: BTreeMap<u64, Message>,
    next_id: u64,
    events: VecDeque<Event>,
    next_event: u64,
    buckets: HashMap<String, Bucket>,
}

pub struct Bus {
    state: Mutex<State>,
    store: Store,
    limits: Limits,
    join_token: String,
    operator_token: String,
    boot: String,
    changed: watch::Sender<u64>,
    closing: AtomicBool,
    log: bool,
}

impl Bus {
    pub fn new(
        store: Store,
        limits: Limits,
        join_token: String,
        operator_token: String,
        log: bool,
    ) -> Result<Bus, BoxError> {
        let loaded = store.load()?;
        let mut agents = HashMap::new();
        let mut by_token = HashMap::new();
        for a in loaded.agents {
            by_token.insert(a.token.clone(), a.key());
            agents.insert(a.key(), a);
        }
        let messages: BTreeMap<u64, Message> =
            loaded.messages.into_iter().map(|m| (m.id, m)).collect();
        let next_id = loaded
            .next_id
            .max(messages.keys().next_back().map_or(1, |id| id + 1));
        let (changed, _) = watch::channel(0);
        Ok(Bus {
            state: Mutex::new(State {
                agents,
                by_token,
                runtime: HashMap::new(),
                messages,
                next_id,
                events: VecDeque::new(),
                next_event: 1,
                buckets: HashMap::new(),
            }),
            store,
            limits,
            join_token,
            operator_token,
            boot: util::random_token()[..16].to_string(),
            changed,
            closing: AtomicBool::new(false),
            log,
        })
    }

    /// Notified on every change that could matter to a waiting request.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn shutdown(&self) {
        self.closing.store(true, Ordering::SeqCst);
        self.bump();
    }

    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn push_event(&self, st: &mut State, now: u64, kind: EventKind) {
        if self.log {
            eprintln!(
                "{}  {}",
                util::clock(now, true),
                display::event_line(&kind, false)
            );
        }
        let id = st.next_event;
        st.next_event += 1;
        st.events.push_back(Event {
            id,
            at: now,
            event: kind,
        });
        while st.events.len() > EVENT_BUFFER {
            st.events.pop_front();
        }
    }

    // ----- identity -----

    pub fn register(
        &self,
        join_token: &str,
        req: RegisterRequest,
        now: u64,
    ) -> Result<RegisterResponse, BusError> {
        if !util::ct_eq(join_token, &self.join_token) {
            return Err(BusError::Unauthorized);
        }
        for (what, v) in [
            ("Name", Some(&req.name)),
            ("Projekt", Some(&req.project)),
            ("Rolle", req.role.as_ref()),
            ("Tool", req.tool.as_ref()),
        ] {
            if let Some(v) = v.filter(|v| !valid_ident(v)) {
                return Err(BusError::BadRequest(format!(
                    "{what} {v:?} ist ungültig: nur a-z, 0-9, - und _, höchstens {IDENT_MAX} Zeichen."
                )));
            }
        }
        let key = agent_key(&req.name, &req.project);
        let mut st = self.lock();
        // A session that leaves role or tool out keeps what is known, so a hook
        // without AGENT_BUS_TOOL does not overwrite the MCP adapter's value.
        let known = st.agents.get(&key);
        let role = req
            .role
            .clone()
            .or_else(|| known.map(|a| a.role.clone()))
            .unwrap_or_else(|| req.name.clone());
        let tool = req
            .tool
            .clone()
            .or_else(|| known.map(|a| a.tool.clone()))
            .unwrap_or_else(|| "unbekannt".into());
        let token = match st.agents.get_mut(&key) {
            Some(a) => {
                let changed = a.role != role || a.tool != tool;
                a.role = role.clone();
                a.tool = tool.clone();
                a.last_seen = now;
                let a = a.clone();
                self.store.put_agent(&a)?;
                if changed {
                    self.push_event(
                        &mut st,
                        now,
                        EventKind::Registered {
                            agent: key.clone(),
                            tool,
                            role,
                        },
                    );
                }
                a.token
            }
            None => {
                let a = Agent {
                    name: req.name,
                    project: req.project,
                    role: role.clone(),
                    tool: tool.clone(),
                    token: util::random_token(),
                    registered_at: now,
                    last_seen: now,
                    read_cursor: 0,
                    notified_cursor: 0,
                    muted: false,
                };
                self.store.put_agent(&a)?;
                st.by_token.insert(a.token.clone(), key.clone());
                let token = a.token.clone();
                st.agents.insert(key.clone(), a);
                self.push_event(
                    &mut st,
                    now,
                    EventKind::Registered {
                        agent: key.clone(),
                        tool,
                        role,
                    },
                );
                token
            }
        };
        st.runtime.entry(key.clone()).or_default().bye = false;
        drop(st);
        self.bump();
        Ok(RegisterResponse { agent: key, token })
    }

    /// Maps an agent token to the agent key and records the contact.
    pub fn authenticate(&self, token: &str, now: u64) -> Result<String, BusError> {
        let mut st = self.lock();
        let key = st
            .by_token
            .get(token)
            .cloned()
            .ok_or(BusError::Unauthorized)?;
        if let Some(a) = st.agents.get_mut(&key) {
            a.last_seen = now;
        }
        Ok(key)
    }

    pub fn check_operator(&self, token: &str) -> Result<(), BusError> {
        if util::ct_eq(token, &self.operator_token) {
            Ok(())
        } else {
            Err(BusError::Unauthorized)
        }
    }

    // ----- activity -----

    /// "I am working": a turn started. Ends any ready request of this agent.
    pub fn busy(&self, key: &str, at_ms: u64, now: u64) -> Result<(), BusError> {
        let mut st = self.lock();
        let rt = st.runtime.entry(key.to_string()).or_default();
        let was = rt.busy && !rt.bye;
        rt.busy = true;
        rt.bye = false;
        rt.busy_mark = rt.busy_mark.max(at_ms);
        if !was {
            self.push_event(
                &mut st,
                now,
                EventKind::Activity {
                    agent: key.into(),
                    activity: Activity::Busy,
                },
            );
        }
        drop(st);
        self.bump();
        Ok(())
    }

    /// "I am leaving": the session ends. Messages stay in the inbox.
    pub fn bye(&self, key: &str, now: u64) -> Result<(), BusError> {
        let mut st = self.lock();
        let rt = st.runtime.entry(key.to_string()).or_default();
        rt.bye = true;
        rt.busy = false;
        rt.waiter = None;
        self.push_event(
            &mut st,
            now,
            EventKind::Activity {
                agent: key.into(),
                activity: Activity::Offline,
            },
        );
        drop(st);
        self.bump();
        Ok(())
    }

    /// One check of a ready request ("I am done, wake me"). The HTTP layer repeats
    /// it whenever something changes, until it fires or times out.
    pub fn ready_check(
        &self,
        key: &str,
        req: &ReadyRequest,
        now: u64,
    ) -> Result<ReadyCheck, BusError> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let agent = st.agents.get_mut(key).ok_or(BusError::Unauthorized)?;
        let rt = st.runtime.entry(key.to_string()).or_default();
        if rt.waiter.as_ref().is_some_and(|cur| req.waiter < *cur) {
            return Ok(ReadyCheck::Superseded);
        }
        // A fresh session (start, resume, clear, compact) has not seen earlier wake
        // notices. Forget them before anything else, so even if its first turn makes
        // this request moot, the next "done" announces every unread message.
        if req.mode == ReadyMode::Unread
            && rt.waiter.as_ref() != Some(&req.waiter)
            && agent.notified_cursor > agent.read_cursor
        {
            agent.notified_cursor = agent.read_cursor;
            self.store.put_agent(agent)?;
        }
        if rt.busy && rt.busy_mark >= req.waiter.started_ms {
            return Ok(ReadyCheck::Busy);
        }
        let transition = rt.waiter.as_ref() != Some(&req.waiter) || rt.busy || rt.bye;
        rt.waiter = Some(req.waiter.clone());
        rt.waiter_last_poll = now;
        rt.busy = false;
        rt.bye = false;
        agent.last_seen = now;

        let threshold = match req.mode {
            ReadyMode::Unread => agent.read_cursor,
            ReadyMode::New => agent.read_cursor.max(agent.notified_cursor),
        };
        let mut ids = Vec::new();
        let mut from: Vec<String> = Vec::new();
        for (id, m) in st.messages.range(threshold + 1..) {
            if m.to == key {
                ids.push(*id);
                if !from.contains(&m.from) {
                    from.push(m.from.clone());
                }
            }
        }
        let count = ids.len();
        let max_id = ids.last().copied().unwrap_or(0);
        if count == 0 {
            if transition {
                self.push_event(
                    st,
                    now,
                    EventKind::Activity {
                        agent: key.into(),
                        activity: Activity::Ready,
                    },
                );
                drop(guard);
                // Lets an older ready process of this agent notice it was replaced.
                self.bump();
            }
            return Ok(ReadyCheck::Nothing);
        }
        agent.notified_cursor = agent.notified_cursor.max(max_id);
        let snapshot = agent.clone();
        rt.busy = true;
        rt.busy_mark = rt.busy_mark.max(req.waiter.started_ms);
        self.store.put_agent(&snapshot)?;
        // Read receipt, part 1: the first wake per message counts.
        let mut woken = Vec::new();
        for id in &ids {
            if let Some(m) = st.messages.get_mut(id).filter(|m| m.woken_at.is_none()) {
                m.woken_at = Some(now);
                woken.push(m.clone());
            }
        }
        self.store.put_messages(&woken)?;
        self.push_event(
            st,
            now,
            EventKind::Woken {
                agent: key.into(),
                count,
            },
        );
        drop(guard);
        self.bump();
        Ok(ReadyCheck::Fire { count, from })
    }

    fn activity_of(&self, agent: &Agent, rt: Option<&Runtime>, now: u64) -> Activity {
        let offline_ms = self.limits.offline_after_minutes * 60_000;
        let Some(rt) = rt else {
            return if now.saturating_sub(agent.last_seen) > offline_ms {
                Activity::Offline
            } else {
                Activity::Idle
            };
        };
        if rt.bye {
            return Activity::Offline;
        }
        if !rt.busy
            && rt.waiter.is_some()
            && now.saturating_sub(rt.waiter_last_poll) <= READY_GRACE_MS
        {
            return Activity::Ready;
        }
        if now.saturating_sub(agent.last_seen) > offline_ms {
            return Activity::Offline;
        }
        if rt.busy {
            Activity::Busy
        } else {
            Activity::Idle
        }
    }

    // ----- messages -----

    pub fn send(&self, from: &str, req: SendRequest, now: u64) -> Result<SendResponse, BusError> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let sender = st.agents.get(from).ok_or(BusError::Unauthorized)?.clone();
        let to = resolve_address(&req.to, &sender.project)?;
        let reject = |bus: &Bus, st: &mut State, reason: &str| {
            bus.push_event(
                st,
                now,
                EventKind::Rejected {
                    from: from.into(),
                    to: to.clone(),
                    reason: reason.into(),
                },
            );
        };

        if sender.muted {
            reject(self, st, "stummgeschaltet");
            return Err(BusError::Forbidden(
                "Marco hat dich auf dem Bus stummgeschaltet. Deine Nachricht wurde nicht zugestellt.".into(),
            ));
        }
        if req.body.trim().is_empty() {
            return Err(BusError::BadRequest("Die Nachricht ist leer.".into()));
        }
        if req.body.len() > self.limits.max_body_bytes {
            reject(self, st, "zu groß");
            return Err(BusError::TooLarge(format!(
                "Die Nachricht hat {} Bytes, erlaubt sind {}. Fass dich kürzer oder verweis auf Issue oder PR.",
                req.body.len(),
                self.limits.max_body_bytes
            )));
        }
        if to == from {
            return Err(BusError::BadRequest(
                "Nachrichten an dich selbst gehen nicht.".into(),
            ));
        }
        if !st.agents.contains_key(&to) {
            reject(self, st, "unbekannter Empfänger");
            return Err(BusError::NotFound(format!(
                "Unbekannter Empfänger {to}. Wer angemeldet ist, zeigt das Tool agents."
            )));
        }
        if let Err(retry_after_secs) = self.take_token(st, from, now) {
            reject(self, st, "Ratenlimit");
            return Err(BusError::RateLimited { retry_after_secs });
        }

        let id = st.next_id;
        st.next_id += 1;
        let msg = Message {
            id,
            from: from.into(),
            to: to.clone(),
            body: req.body,
            sent_at: now,
            woken_at: None,
            read_at: None,
        };
        self.store.put_message(&msg, st.next_id)?;
        st.messages.insert(id, msg.clone());
        self.push_event(
            st,
            now,
            EventKind::Message {
                id,
                from: msg.from,
                to: to.clone(),
                body: msg.body,
            },
        );
        self.enforce_cap(st)?;
        let recipient_activity = {
            let a = &st.agents[&to];
            self.activity_of(a, st.runtime.get(&to), now)
        };
        drop(guard);
        self.bump();
        Ok(SendResponse {
            id,
            to,
            recipient_activity,
        })
    }

    /// Token bucket per sender. Err carries the seconds until the next token.
    fn take_token(&self, st: &mut State, from: &str, now: u64) -> Result<(), u64> {
        let rate_per_ms = self.limits.messages_per_minute as f64 / 60_000.0;
        let cap = self.limits.burst as f64;
        let b = st.buckets.entry(from.to_string()).or_insert(Bucket {
            tokens: cap,
            last: now,
        });
        b.tokens = (b.tokens + now.saturating_sub(b.last) as f64 * rate_per_ms).min(cap);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err((((1.0 - b.tokens) / rate_per_ms) / 1000.0).ceil().max(1.0) as u64)
        }
    }

    fn enforce_cap(&self, st: &mut State) -> Result<(), BusError> {
        let excess = st.messages.len().saturating_sub(self.limits.max_messages);
        if excess == 0 {
            return Ok(());
        }
        let ids: Vec<u64> = st.messages.keys().take(excess).copied().collect();
        self.store.delete_messages(&ids)?;
        for id in ids {
            st.messages.remove(&id);
        }
        Ok(())
    }

    /// Unread messages for `key`, oldest first. Without `peek` they count as read.
    pub fn inbox(&self, key: &str, peek: bool, now: u64) -> Result<Vec<Message>, BusError> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let agent = st.agents.get_mut(key).ok_or(BusError::Unauthorized)?;
        agent.last_seen = now;
        let ids: Vec<u64> = st
            .messages
            .range(agent.read_cursor + 1..)
            .filter(|(_, m)| m.to == key)
            .map(|(id, _)| *id)
            .collect();
        let Some(&last) = ids.last() else {
            return Ok(Vec::new());
        };
        if peek {
            return Ok(ids.iter().map(|id| st.messages[id].clone()).collect());
        }
        agent.read_cursor = last;
        agent.notified_cursor = agent.notified_cursor.max(last);
        let snapshot = agent.clone();
        self.store.put_agent(&snapshot)?;
        // Read receipt, part 2.
        let mut read = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(m) = st.messages.get_mut(id) {
                m.read_at = Some(now);
                read.push(m.clone());
            }
        }
        self.store.put_messages(&read)?;
        self.push_event(
            st,
            now,
            EventKind::Read {
                agent: key.into(),
                ids,
            },
        );
        drop(guard);
        self.bump();
        Ok(read)
    }

    /// What `key` sent recently and where each message stands, oldest first.
    pub fn sent_by(&self, key: &str, limit: usize, now: u64) -> Result<Vec<SentInfo>, BusError> {
        let st = self.lock();
        if !st.agents.contains_key(key) {
            return Err(BusError::Unauthorized);
        }
        let mut out: Vec<SentInfo> = st
            .messages
            .values()
            .rev()
            .filter(|m| m.from == key)
            .take(limit)
            .map(|m| SentInfo {
                id: m.id,
                to: m.to.clone(),
                sent_at: m.sent_at,
                woken_at: m.woken_at,
                read_at: m.read_at,
                preview: preview(&m.body, 60),
                recipient_activity: st
                    .agents
                    .get(&m.to)
                    .map(|a| self.activity_of(a, st.runtime.get(&m.to), now))
                    .unwrap_or(Activity::Offline),
            })
            .collect();
        out.reverse();
        Ok(out)
    }

    /// Facts about agent `a`. With a `viewer`, also how many of the unread
    /// messages the viewer sent.
    fn info(&self, st: &State, a: &Agent, viewer: Option<&str>, now: u64) -> AgentInfo {
        let key = a.key();
        let mut unread = 0;
        let mut unread_from_you = 0;
        for (_, m) in st.messages.range(a.read_cursor + 1..) {
            if m.to == key {
                unread += 1;
                if viewer == Some(m.from.as_str()) {
                    unread_from_you += 1;
                }
            }
        }
        AgentInfo {
            activity: self.activity_of(a, st.runtime.get(&key), now),
            agent: key,
            role: a.role.clone(),
            tool: a.tool.clone(),
            last_seen: a.last_seen,
            muted: a.muted,
            unread,
            unread_from_you,
        }
    }

    /// Agents in the same project as `key`.
    pub fn agents_for(&self, key: &str, now: u64) -> Result<Vec<AgentInfo>, BusError> {
        let st = self.lock();
        let project = st
            .agents
            .get(key)
            .ok_or(BusError::Unauthorized)?
            .project
            .clone();
        let mut out: Vec<AgentInfo> = st
            .agents
            .values()
            .filter(|a| a.project == project)
            .map(|a| self.info(&st, a, Some(key), now))
            .collect();
        out.sort_by(|a, b| a.agent.cmp(&b.agent));
        Ok(out)
    }

    // ----- operator (Marco) -----

    pub fn all_agents(&self, now: u64) -> Vec<AgentInfo> {
        let st = self.lock();
        let mut out: Vec<AgentInfo> = st
            .agents
            .values()
            .map(|a| self.info(&st, a, None, now))
            .collect();
        out.sort_by(|a, b| a.agent.cmp(&b.agent));
        out
    }

    pub fn set_muted(&self, agent: &str, muted: bool, now: u64) -> Result<(), BusError> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let a = st
            .agents
            .get_mut(agent)
            .ok_or_else(|| BusError::NotFound(format!("Unbekannter Agent {agent}.")))?;
        if a.muted != muted {
            a.muted = muted;
            let snapshot = a.clone();
            self.store.put_agent(&snapshot)?;
            self.push_event(
                st,
                now,
                EventKind::Muted {
                    agent: agent.into(),
                    muted,
                },
            );
        }
        drop(guard);
        self.bump();
        Ok(())
    }

    pub fn events_after(&self, after: u64) -> EventsResponse {
        let st = self.lock();
        EventsResponse {
            boot: self.boot.clone(),
            events: st.events.iter().filter(|e| e.id > after).cloned().collect(),
        }
    }

    pub fn recent_messages(&self, limit: usize) -> Vec<Message> {
        let st = self.lock();
        let mut v: Vec<Message> = st.messages.values().rev().take(limit).cloned().collect();
        v.reverse();
        v
    }

    // ----- housekeeping -----

    /// Deletes old messages and long gone agents. Returns (agents, messages) removed.
    pub fn cleanup(&self, now: u64) -> Result<(usize, usize), BusError> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let cutoff = now.saturating_sub(self.limits.retention_hours * 3_600_000);
        let old: Vec<u64> = st
            .messages
            .values()
            .filter(|m| m.sent_at < cutoff)
            .map(|m| m.id)
            .collect();
        self.store.delete_messages(&old)?;
        for id in &old {
            st.messages.remove(id);
        }
        let agent_cutoff = now.saturating_sub(self.limits.agent_expiry_days * 86_400_000);
        let gone: Vec<String> = st
            .agents
            .values()
            .filter(|a| a.last_seen < agent_cutoff)
            .map(|a| a.key())
            .collect();
        self.store.delete_agents(&gone)?;
        for key in &gone {
            if let Some(a) = st.agents.remove(key) {
                st.by_token.remove(&a.token);
            }
            st.runtime.remove(key);
            st.buckets.remove(key);
        }
        if !old.is_empty() || !gone.is_empty() {
            self.push_event(
                st,
                now,
                EventKind::Expired {
                    agents: gone.clone(),
                    messages: old.len(),
                },
            );
        }
        Ok((gone.len(), old.len()))
    }
}

/// The first `max` characters, on one line.
fn preview(body: &str, max: usize) -> String {
    let flat: String = body.split_whitespace().collect::<Vec<_>>().join(" ");
    match flat.char_indices().nth(max) {
        Some((cut, _)) => format!("{}...", &flat[..cut]),
        None => flat,
    }
}

/// `name` means the sender's project, `name@project` any project.
pub fn resolve_address(to: &str, own_project: &str) -> Result<String, BusError> {
    let to = to.trim();
    let (name, project) = match to.split_once('@') {
        Some((n, p)) => (n, p),
        None => (to, own_project),
    };
    if !valid_ident(name) || !valid_ident(project) {
        return Err(BusError::BadRequest(format!(
            "Empfänger {to:?} ist ungültig. Schreib name oder name@projekt."
        )));
    }
    Ok(agent_key(name, project))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bus_with(limits: Limits) -> (Bus, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("bus.redb")).unwrap();
        (
            Bus::new(store, limits, "join".into(), "op".into(), false).unwrap(),
            dir,
        )
    }

    fn reg(bus: &Bus, name: &str, project: &str) -> String {
        bus.register(
            "join",
            RegisterRequest {
                name: name.into(),
                project: project.into(),
                role: None,
                tool: Some("test".into()),
            },
            1_000,
        )
        .unwrap();
        agent_key(name, project)
    }

    fn send(
        bus: &Bus,
        from: &str,
        to: &str,
        body: &str,
        now: u64,
    ) -> Result<SendResponse, BusError> {
        bus.send(
            from,
            SendRequest {
                to: to.into(),
                body: body.into(),
            },
            now,
        )
    }

    fn waiter(ms: u64) -> WaiterId {
        WaiterId {
            started_ms: ms,
            nonce: "n".into(),
        }
    }

    fn ready(bus: &Bus, key: &str, w: u64, mode: ReadyMode, now: u64) -> ReadyCheck {
        bus.ready_check(
            key,
            &ReadyRequest {
                waiter: waiter(w),
                mode,
                timeout_secs: 0,
            },
            now,
        )
        .unwrap()
    }

    #[test]
    fn address_resolution() {
        assert_eq!(
            resolve_address("ada", "agent-bus").unwrap(),
            "ada@agent-bus"
        );
        assert_eq!(
            resolve_address("jo@hermes", "agent-bus").unwrap(),
            "jo@hermes"
        );
        assert!(resolve_address("Ada", "agent-bus").is_err());
        assert!(resolve_address("ada@", "agent-bus").is_err());
    }

    #[test]
    fn register_is_idempotent_and_needs_join_token() {
        let (bus, _d) = bus_with(Limits::default());
        let req = RegisterRequest {
            name: "jo".into(),
            project: "p".into(),
            role: None,
            tool: None,
        };
        let a = bus.register("join", req.clone(), 1).unwrap();
        let b = bus.register("join", req.clone(), 2).unwrap();
        assert_eq!(a.token, b.token);
        assert!(matches!(
            bus.register("wrong", req, 3),
            Err(BusError::Unauthorized)
        ));
    }

    #[test]
    fn register_without_tool_keeps_known_tool() {
        let (bus, _d) = bus_with(Limits::default());
        let key = reg(&bus, "jo", "p");
        let bare = RegisterRequest {
            name: "jo".into(),
            project: "p".into(),
            role: None,
            tool: None,
        };
        bus.register("join", bare, 2).unwrap();
        let info = bus
            .all_agents(3)
            .into_iter()
            .find(|a| a.agent == key)
            .unwrap();
        assert_eq!(info.tool, "test");
        assert_eq!(info.role, "jo");
        let bad = RegisterRequest {
            name: "jo".into(),
            project: "p".into(),
            role: Some("Chef".into()),
            tool: None,
        };
        assert!(matches!(
            bus.register("join", bad, 4),
            Err(BusError::BadRequest(_))
        ));
    }

    #[test]
    fn message_flow_and_cursors() {
        let (bus, _d) = bus_with(Limits::default());
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        send(&bus, &jo, "ada", "hallo", 2_000).unwrap();
        assert_eq!(
            bus.inbox(&ada, true, 3_000).unwrap().len(),
            1,
            "peek keeps it unread"
        );
        let got = bus.inbox(&ada, false, 3_000).unwrap();
        assert_eq!(got[0].body, "hallo");
        assert_eq!(got[0].from, "jo@p");
        assert!(bus.inbox(&ada, false, 3_000).unwrap().is_empty());
        assert!(
            bus.inbox(&jo, false, 3_000).unwrap().is_empty(),
            "not the sender's mail"
        );
    }

    #[test]
    fn guardrails() {
        let limits = Limits {
            max_body_bytes: 10,
            burst: 2,
            messages_per_minute: 1,
            ..Limits::default()
        };
        let (bus, _d) = bus_with(limits);
        let jo = reg(&bus, "jo", "p");
        reg(&bus, "ada", "p");
        assert!(matches!(
            send(&bus, &jo, "ada", "12345678901", 1),
            Err(BusError::TooLarge(_))
        ));
        assert!(matches!(
            send(&bus, &jo, "ada", "  ", 1),
            Err(BusError::BadRequest(_))
        ));
        assert!(matches!(
            send(&bus, &jo, "jo", "x", 1),
            Err(BusError::BadRequest(_))
        ));
        assert!(matches!(
            send(&bus, &jo, "nobody", "x", 1),
            Err(BusError::NotFound(_))
        ));
        send(&bus, &jo, "ada", "1", 1).unwrap();
        send(&bus, &jo, "ada", "2", 1).unwrap();
        match send(&bus, &jo, "ada", "3", 1) {
            Err(BusError::RateLimited { retry_after_secs }) => {
                assert!((1..=60).contains(&retry_after_secs))
            }
            other => panic!("expected rate limit, got {other:?}"),
        }
        send(&bus, &jo, "ada", "4", 61_000).unwrap();
    }

    #[test]
    fn mute_blocks_sending_but_not_receiving() {
        let (bus, _d) = bus_with(Limits::default());
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        bus.set_muted(&jo, true, 1).unwrap();
        assert!(matches!(
            send(&bus, &jo, "ada", "x", 2),
            Err(BusError::Forbidden(_))
        ));
        send(&bus, &ada, "jo", "x", 2).unwrap();
        bus.set_muted(&jo, false, 3).unwrap();
        send(&bus, &jo, "ada", "x", 4).unwrap();
    }

    #[test]
    fn ready_fires_once_per_new_message() {
        let (bus, _d) = bus_with(Limits::default());
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        assert_eq!(
            ready(&bus, &ada, 10, ReadyMode::New, 10),
            ReadyCheck::Nothing
        );
        assert_eq!(
            bus.all_agents(11)
                .iter()
                .find(|a| a.agent == ada)
                .unwrap()
                .activity,
            Activity::Ready
        );
        send(&bus, &jo, "ada", "x", 12).unwrap();
        assert_eq!(
            ready(&bus, &ada, 10, ReadyMode::New, 13),
            ReadyCheck::Fire {
                count: 1,
                from: vec!["jo@p".into()]
            }
        );
        // The fired process is done; the agent now counts as working.
        assert_eq!(ready(&bus, &ada, 10, ReadyMode::New, 14), ReadyCheck::Busy);
        // Next turn ends without reading: no second wake for the same message ...
        assert_eq!(
            ready(&bus, &ada, 20, ReadyMode::New, 20),
            ReadyCheck::Nothing
        );
        // ... unless a restarted session asks for everything unread.
        assert_eq!(
            ready(&bus, &ada, 30, ReadyMode::Unread, 30),
            ReadyCheck::Fire {
                count: 1,
                from: vec!["jo@p".into()]
            }
        );
    }

    #[test]
    fn fresh_session_hears_about_unread_even_if_its_first_turn_wins() {
        let (bus, _d) = bus_with(Limits::default());
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        ready(&bus, &ada, 10, ReadyMode::New, 10);
        send(&bus, &jo, "ada", "x", 11).unwrap();
        assert!(matches!(
            ready(&bus, &ada, 10, ReadyMode::New, 12),
            ReadyCheck::Fire { .. }
        ));
        // The old session never read it. A new session starts: its start prompt
        // reports busy before the SessionStart ready request gets through.
        bus.busy(&ada, 30, 30).unwrap();
        assert_eq!(
            ready(&bus, &ada, 20, ReadyMode::Unread, 31),
            ReadyCheck::Busy
        );
        // At the end of that first turn the unread message is announced again.
        assert!(matches!(
            ready(&bus, &ada, 40, ReadyMode::New, 40),
            ReadyCheck::Fire { .. }
        ));
    }

    #[test]
    fn read_receipts_and_unread_counts() {
        let (bus, _d) = bus_with(Limits::default());
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        let lex = reg(&bus, "lex", "p");
        send(&bus, &jo, "ada", "Bitte   #12\nreviewen", 100).unwrap();
        send(&bus, &lex, "ada", "von lex", 101).unwrap();

        let s = bus.sent_by(&jo, 10, 102).unwrap();
        assert_eq!((s[0].woken_at, s[0].read_at), (None, None));
        assert_eq!(s[0].preview, "Bitte #12 reviewen");
        let view = bus.agents_for(&jo, 102).unwrap();
        let a = view.iter().find(|a| a.agent == ada).unwrap();
        assert_eq!((a.unread, a.unread_from_you), (2, 1));

        assert!(matches!(
            ready(&bus, &ada, 103, ReadyMode::New, 103),
            ReadyCheck::Fire { .. }
        ));
        assert_eq!(bus.sent_by(&jo, 10, 104).unwrap()[0].woken_at, Some(103));

        bus.inbox(&ada, false, 105).unwrap();
        let s = bus.sent_by(&jo, 10, 106).unwrap();
        assert_eq!((s[0].woken_at, s[0].read_at), (Some(103), Some(105)));
        assert_eq!(
            bus.agents_for(&jo, 106)
                .unwrap()
                .iter()
                .find(|a| a.agent == ada)
                .unwrap()
                .unread,
            0
        );
        assert!(bus.events_after(0).events.iter().any(|e| matches!(&e.event, EventKind::Read { agent, ids } if agent == &ada && ids.len() == 2)));
        assert!(bus.sent_by(&ada, 10, 107).unwrap().is_empty());
    }

    #[test]
    fn busy_and_supersede() {
        let (bus, _d) = bus_with(Limits::default());
        let ada = reg(&bus, "ada", "p");
        assert_eq!(
            ready(&bus, &ada, 100, ReadyMode::New, 100),
            ReadyCheck::Nothing
        );
        assert_eq!(
            ready(&bus, &ada, 200, ReadyMode::New, 200),
            ReadyCheck::Nothing
        );
        assert_eq!(
            ready(&bus, &ada, 100, ReadyMode::New, 201),
            ReadyCheck::Superseded
        );
        bus.busy(&ada, 250, 250).unwrap();
        assert_eq!(
            ready(&bus, &ada, 200, ReadyMode::New, 251),
            ReadyCheck::Busy
        );
        assert_eq!(bus.all_agents(252)[0].activity, Activity::Busy);
        // A ready process from before the busy report must not flip it back.
        assert_eq!(
            ready(&bus, &ada, 240, ReadyMode::New, 253),
            ReadyCheck::Busy
        );
        assert_eq!(
            ready(&bus, &ada, 300, ReadyMode::New, 300),
            ReadyCheck::Nothing
        );
        assert_eq!(bus.all_agents(301)[0].activity, Activity::Ready);
        bus.bye(&ada, 302).unwrap();
        assert_eq!(bus.all_agents(303)[0].activity, Activity::Offline);
    }

    #[test]
    fn ready_connection_expires() {
        let (bus, _d) = bus_with(Limits::default());
        let ada = reg(&bus, "ada", "p");
        ready(&bus, &ada, 10, ReadyMode::New, 10);
        assert_eq!(
            bus.all_agents(10 + READY_GRACE_MS)[0].activity,
            Activity::Ready
        );
        assert_eq!(
            bus.all_agents(11 + READY_GRACE_MS)[0].activity,
            Activity::Idle
        );
    }

    #[test]
    fn retention_cap_and_expiry() {
        let limits = Limits {
            max_messages: 2,
            retention_hours: 1,
            agent_expiry_days: 1,
            ..Limits::default()
        };
        let (bus, _d) = bus_with(limits);
        let jo = reg(&bus, "jo", "p");
        let ada = reg(&bus, "ada", "p");
        for i in 0..3 {
            send(&bus, &jo, "ada", &format!("m{i}"), 2_000 + i).unwrap();
        }
        assert_eq!(bus.recent_messages(10).len(), 2, "cap drops the oldest");
        bus.cleanup(2_002 + 3_600_001).unwrap();
        assert!(
            bus.recent_messages(10).is_empty(),
            "retention deletes old messages"
        );
        bus.authenticate(&bus_token(&bus, &jo), 86_400_000).unwrap();
        let (agents, _) = bus.cleanup(1_000 + 86_400_001).unwrap();
        assert_eq!(agents, 1);
        assert!(bus.all_agents(0).iter().all(|a| a.agent != ada));
    }

    fn bus_token(bus: &Bus, key: &str) -> String {
        bus.lock().agents[key].token.clone()
    }

    #[test]
    fn state_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bus.redb");
        let token;
        {
            let bus = Bus::new(
                Store::open(&path).unwrap(),
                Limits::default(),
                "join".into(),
                "op".into(),
                false,
            )
            .unwrap();
            let jo = reg(&bus, "jo", "p");
            reg(&bus, "ada", "p");
            send(&bus, &jo, "ada", "bleibt", 5).unwrap();
            token = bus_token(&bus, "ada@p");
        }
        let bus = Bus::new(
            Store::open(&path).unwrap(),
            Limits::default(),
            "join".into(),
            "op".into(),
            false,
        )
        .unwrap();
        let ada = bus.authenticate(&token, 6).unwrap();
        assert_eq!(bus.inbox(&ada, false, 7).unwrap()[0].body, "bleibt");
        let jo = "jo@p".to_string();
        assert_eq!(
            send(&bus, &jo, "ada", "neu", 8).unwrap().id,
            2,
            "ids continue"
        );
    }
}
