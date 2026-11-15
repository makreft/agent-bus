//! HTTP layer of the controller. Translates requests to `Bus` calls and holds ready
//! requests open until they fire.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Json, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;

use crate::bus::{Bus, BusError, READY_POLL_MAX_SECS, ReadyCheck};
use crate::config::Config;
use crate::model::*;
use crate::store::Store;
use crate::util::{self, BoxError};

type Shared = Arc<Bus>;

impl IntoResponse for BusError {
    fn into_response(self) -> Response {
        let status = match &self {
            BusError::Unauthorized => StatusCode::UNAUTHORIZED,
            BusError::Forbidden(_) => StatusCode::FORBIDDEN,
            BusError::NotFound(_) => StatusCode::NOT_FOUND,
            BusError::BadRequest(_) => StatusCode::BAD_REQUEST,
            BusError::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            BusError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            BusError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            BusError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let retry_after_secs = match &self {
            BusError::RateLimited { retry_after_secs } => Some(*retry_after_secs),
            _ => None,
        };
        let body = ErrorBody {
            error: self.code().into(),
            message: self.message(),
            retry_after_secs,
        };
        (status, Json(body)).into_response()
    }
}

fn bearer(headers: &HeaderMap) -> Result<String, BusError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .ok_or(BusError::Unauthorized)
}

fn agent(bus: &Bus, headers: &HeaderMap) -> Result<String, BusError> {
    bus.authenticate(&bearer(headers)?, util::now_ms())
}

fn operator(bus: &Bus, headers: &HeaderMap) -> Result<(), BusError> {
    bus.check_operator(&bearer(headers)?)
}

pub fn router(bus: Shared) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/register", post(register))
        .route("/v1/busy", post(busy))
        .route("/v1/ready", post(ready))
        .route("/v1/bye", post(bye))
        .route("/v1/send", post(send))
        .route("/v1/inbox", get(inbox))
        .route("/v1/sent", get(sent))
        .route("/v1/agents", get(agents))
        .route("/v1/admin/agents", get(admin_agents))
        .route("/v1/admin/events", get(admin_events))
        .route("/v1/admin/messages", get(admin_messages))
        .route("/v1/admin/mute", post(admin_mute))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(bus)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn register(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, BusError> {
    bus.register(&bearer(&headers)?, req, util::now_ms())
        .map(Json)
}

#[derive(Deserialize)]
struct BusyRequest {
    at_ms: u64,
}

async fn busy(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Json(req): Json<BusyRequest>,
) -> Result<Json<OkResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    bus.busy(&key, req.at_ms, util::now_ms())?;
    Ok(Json(OkResponse { ok: true }))
}

async fn bye(State(bus): State<Shared>, headers: HeaderMap) -> Result<Json<OkResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    bus.bye(&key, util::now_ms())?;
    Ok(Json(OkResponse { ok: true }))
}

/// "I am done": held open until a message arrives, the agent gets busy, a newer
/// ready process takes over, or the timeout passes.
async fn ready(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Json(req): Json<ReadyRequest>,
) -> Result<Json<ReadyResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    let timeout = Duration::from_secs(req.timeout_secs.min(READY_POLL_MAX_SECS));
    let deadline = tokio::time::Instant::now() + timeout;
    // Subscribe before checking, so no change between check and wait is missed.
    let mut changes = bus.subscribe();
    loop {
        if bus.is_closing() {
            return Err(BusError::Unavailable);
        }
        match bus.ready_check(&key, &req, util::now_ms())? {
            ReadyCheck::Fire { count, from } => {
                return Ok(Json(ReadyResponse::Messages { count, from }));
            }
            ReadyCheck::Superseded => return Ok(Json(ReadyResponse::Superseded)),
            ReadyCheck::Busy => return Ok(Json(ReadyResponse::Busy)),
            ReadyCheck::Nothing => {}
        }
        match tokio::time::timeout_at(deadline, changes.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return Err(BusError::Unavailable),
            Err(_) => return Ok(Json(ReadyResponse::Timeout)),
        }
    }
}

async fn send(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Json(req): Json<SendRequest>,
) -> Result<Json<SendResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    bus.send(&key, req, util::now_ms()).map(Json)
}

#[derive(Deserialize)]
struct InboxQuery {
    #[serde(default)]
    peek: bool,
}

async fn inbox(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<InboxQuery>,
) -> Result<Json<InboxResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    let messages = bus.inbox(&key, q.peek, util::now_ms())?;
    Ok(Json(InboxResponse { messages }))
}

#[derive(Deserialize)]
struct SentQuery {
    #[serde(default = "default_sent_limit")]
    limit: usize,
}

fn default_sent_limit() -> usize {
    10
}

async fn sent(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<SentQuery>,
) -> Result<Json<SentResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    let messages = bus.sent_by(&key, q.limit.min(100), util::now_ms())?;
    Ok(Json(SentResponse { messages }))
}

async fn agents(
    State(bus): State<Shared>,
    headers: HeaderMap,
) -> Result<Json<AgentsResponse>, BusError> {
    let key = agent(&bus, &headers)?;
    let agents = bus.agents_for(&key, util::now_ms())?;
    Ok(Json(AgentsResponse { agents }))
}

async fn admin_agents(
    State(bus): State<Shared>,
    headers: HeaderMap,
) -> Result<Json<AgentsResponse>, BusError> {
    operator(&bus, &headers)?;
    Ok(Json(AgentsResponse {
        agents: bus.all_agents(util::now_ms()),
    }))
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after: u64,
    #[serde(default)]
    wait: u64,
}

/// Marco's live view. Long-polls like `ready` when `wait` is set.
async fn admin_events(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> Result<Json<EventsResponse>, BusError> {
    operator(&bus, &headers)?;
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(q.wait.min(READY_POLL_MAX_SECS));
    let mut changes = bus.subscribe();
    loop {
        let resp = bus.events_after(q.after);
        if !resp.events.is_empty() || bus.is_closing() {
            return Ok(Json(resp));
        }
        match tokio::time::timeout_at(deadline, changes.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) | Err(_) => return Ok(Json(resp)),
        }
    }
}

#[derive(Deserialize)]
struct MessagesQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    50
}

async fn admin_messages(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<MessagesQuery>,
) -> Result<Json<MessagesResponse>, BusError> {
    operator(&bus, &headers)?;
    Ok(Json(MessagesResponse {
        messages: bus.recent_messages(q.limit.min(1000)),
    }))
}

async fn admin_mute(
    State(bus): State<Shared>,
    headers: HeaderMap,
    Json(req): Json<MuteRequest>,
) -> Result<Json<OkResponse>, BusError> {
    operator(&bus, &headers)?;
    bus.set_muted(&req.agent, req.muted, util::now_ms())?;
    Ok(Json(OkResponse { ok: true }))
}

/// Opens the store, creates the secrets on first start and builds the bus.
pub fn open_bus(cfg: &Config, log: bool) -> Result<Bus, BoxError> {
    util::ensure_private_dir(&cfg.data_dir)?;
    let join = util::ensure_secret(&cfg.join_token_path())?;
    let op = util::ensure_secret(&cfg.operator_token_path())?;
    let store = Store::open(&cfg.db_path())?;
    Bus::new(store, cfg.limits.clone(), join, op, log)
}

/// Serves until `shutdown` resolves. Housekeeping runs once a minute.
pub async fn run(
    listener: tokio::net::TcpListener,
    bus: Shared,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let housekeeping = {
        let bus = bus.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                if let Err(e) = bus.cleanup(util::now_ms()) {
                    eprintln!("Aufräumen fehlgeschlagen: {}", e.message());
                }
            }
        })
    };
    let bus_for_shutdown = bus.clone();
    let result = axum::serve(listener, router(bus))
        .with_graceful_shutdown(async move {
            shutdown.await;
            // Releases held ready requests right away instead of after their timeout.
            bus_for_shutdown.shutdown();
        })
        .await;
    housekeeping.abort();
    result
}

/// `agent-bus serve`.
pub async fn serve(cfg: Config) -> Result<(), BoxError> {
    let bus = Arc::new(open_bus(&cfg, true)?);
    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .map_err(|e| format!("{} nicht verfügbar: {e}", cfg.listen))?;
    eprintln!(
        "agent-bus {} hört auf {}, Daten in {}",
        env!("CARGO_PKG_VERSION"),
        cfg.listen,
        cfg.data_dir.display()
    );
    run(listener, bus, shutdown_signal()).await?;
    eprintln!("agent-bus beendet");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
