//! End-to-end tests: a real controller on a random local port, real HTTP clients.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use agent_bus::client::Client;
use agent_bus::config::{Config, Limits};
use agent_bus::hook::{self, Notify, ReadyOptions, ReadyOutcome};
use agent_bus::identity::Identity;
use agent_bus::mcp::Adapter;
use agent_bus::model::*;
use agent_bus::{server, util};
use serde_json::json;

struct TestBus {
    cfg: Config,
    _dir: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestBus {
    fn start() -> TestBus {
        Self::start_with(Limits::default())
    }

    fn start_with(limits: Limits) -> TestBus {
        let dir = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let cfg = Config {
            listen: listener.local_addr().unwrap().to_string(),
            allow_remote: false,
            data_dir: dir.path().to_path_buf(),
            limits,
        };
        let bus = Arc::new(server::open_bus(&cfg, false).unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                server::run(listener, bus, async {
                    let _ = rx.await;
                })
                .await
                .unwrap();
            });
        });
        TestBus {
            cfg,
            _dir: dir,
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    fn client(&self) -> Client {
        Client::from_config(&self.cfg, Duration::from_secs(70))
    }

    fn token(&self, name: &str) -> String {
        identity(name).register(&self.cfg, &self.client()).unwrap()
    }

    fn operator(&self) -> String {
        util::read_secret(&self.cfg.operator_token_path()).unwrap()
    }

    fn ready_in_background(
        &self,
        name: &str,
        opts: ReadyOptions,
    ) -> thread::JoinHandle<ReadyOutcome> {
        let cfg = self.cfg.clone();
        let id = identity(name);
        thread::spawn(move || hook::ready(&cfg, &id, &opts))
    }
}

impl Drop for TestBus {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn identity(name: &str) -> Identity {
    Identity {
        name: name.into(),
        project: "p".into(),
        role: None,
        tool: Some("test".into()),
    }
}

fn waiting(max_secs: u64) -> ReadyOptions {
    ReadyOptions {
        mode: ReadyMode::New,
        max_wait: Duration::from_secs(max_secs),
        notify: Notify::Exit2,
        once: false,
    }
}

#[test]
fn ready_wakes_on_message_and_inbox_delivers() {
    let bus = TestBus::start();
    let jo = bus.token("jo");
    bus.token("ada");
    let started = Instant::now();
    let ada_ready = bus.ready_in_background("ada", waiting(20));
    thread::sleep(Duration::from_millis(300));

    let sent = bus.client().send(&jo, "ada", "Bitte #12 reviewen").unwrap();
    assert_eq!(sent.recipient_activity, Activity::Ready);

    match ada_ready.join().unwrap() {
        ReadyOutcome::Woken { notice } => {
            assert!(notice.contains("1 neue Nachricht von jo@p"), "{notice}")
        }
        other => panic!("expected a wake, got {other:?}"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "woken right away, not after a poll interval"
    );

    let ada = bus.token("ada");
    let inbox = bus.client().inbox(&ada).unwrap();
    assert_eq!(inbox.messages.len(), 1);
    assert_eq!(inbox.messages[0].body, "Bitte #12 reviewen");
    assert!(bus.client().inbox(&ada).unwrap().messages.is_empty());
}

#[test]
fn busy_releases_ready_quietly() {
    let bus = TestBus::start();
    let ada = bus.token("ada");
    let ada_ready = bus.ready_in_background("ada", waiting(20));
    thread::sleep(Duration::from_millis(300));
    bus.client().busy(&ada).unwrap();
    assert_eq!(ada_ready.join().unwrap(), ReadyOutcome::Quiet);
    let agents = bus.client().admin_agents(&bus.operator()).unwrap().agents;
    assert_eq!(agents[0].activity, Activity::Busy);
}

#[test]
fn newer_ready_supersedes_older() {
    let bus = TestBus::start();
    bus.token("ada");
    let first = bus.ready_in_background("ada", waiting(20));
    thread::sleep(Duration::from_millis(300));
    let second = bus.ready_in_background("ada", waiting(2));
    assert_eq!(
        first.join().unwrap(),
        ReadyOutcome::Quiet,
        "replaced by the newer one"
    );
    assert_eq!(
        second.join().unwrap(),
        ReadyOutcome::Quiet,
        "ran into its own max wait"
    );
}

#[test]
fn ready_once_and_unread_mode() {
    let bus = TestBus::start();
    let jo = bus.token("jo");
    bus.token("ada");
    let once = ReadyOptions {
        once: true,
        ..waiting(5)
    };
    let ada = identity("ada");
    assert_eq!(hook::ready(&bus.cfg, &ada, &once), ReadyOutcome::Quiet);
    bus.client().send(&jo, "ada", "x").unwrap();
    assert!(matches!(
        hook::ready(&bus.cfg, &ada, &once),
        ReadyOutcome::Woken { .. }
    ));
    thread::sleep(Duration::from_millis(5));
    assert_eq!(
        hook::ready(&bus.cfg, &ada, &once),
        ReadyOutcome::Quiet,
        "no second wake for the same message"
    );
    thread::sleep(Duration::from_millis(5));
    let unread = ReadyOptions {
        mode: ReadyMode::Unread,
        ..once.clone()
    };
    assert!(
        matches!(
            hook::ready(&bus.cfg, &ada, &unread),
            ReadyOutcome::Woken { .. }
        ),
        "a fresh session hears about everything unread"
    );
}

#[test]
fn guardrails_over_http() {
    let bus = TestBus::start_with(Limits {
        max_body_bytes: 20,
        burst: 1,
        messages_per_minute: 1,
        ..Limits::default()
    });
    let jo = bus.token("jo");
    bus.token("ada");
    let c = bus.client();
    assert_eq!(
        c.send(&jo, "ada", &"x".repeat(21)).unwrap_err().status,
        Some(413)
    );
    c.send(&jo, "ada", "ok").unwrap();
    let e = c.send(&jo, "ada", "zu schnell").unwrap_err();
    assert_eq!(e.status, Some(429));
    assert!(e.message.contains("Zu viele Nachrichten"));
    assert!(
        c.send("falsches-token", "ada", "x")
            .unwrap_err()
            .is_unauthorized()
    );
    assert_eq!(c.send(&jo, "niemand", "x").unwrap_err().status, Some(404));
}

#[test]
fn operator_mutes_and_reads_along() {
    let bus = TestBus::start();
    let jo = bus.token("jo");
    bus.token("ada");
    let op = bus.operator();
    let c = bus.client();
    c.send(&jo, "ada", "erste").unwrap();
    let events = c.admin_events(&op, 0, 0).unwrap().events;
    assert!(
        events
            .iter()
            .any(|e| matches!(&e.event, EventKind::Message { body, .. } if body == "erste"))
    );

    c.admin_mute(&op, "jo@p", true).unwrap();
    assert_eq!(c.send(&jo, "ada", "zweite").unwrap_err().status, Some(403));
    assert!(
        c.admin_agents(&jo).unwrap_err().is_unauthorized(),
        "agent tokens cannot use admin routes"
    );
    c.admin_mute(&op, "jo@p", false).unwrap();
    c.send(&jo, "ada", "dritte").unwrap();
}

#[test]
fn mcp_adapter_round_trip() {
    let bus = TestBus::start();
    bus.token("ada");
    let mut jo = Adapter::new(bus.cfg.clone(), Some(identity("jo")));

    let init = jo
        .handle(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}}))
        .unwrap();
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(init["result"]["serverInfo"]["name"], "agent-bus");
    assert!(
        jo.handle(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .is_none()
    );

    let list = jo
        .handle(&json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
        .unwrap();
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["send", "inbox", "agents", "sent"]);

    let sent = jo
        .handle(&json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "send", "arguments": {"to": "ada", "body": "Hallo Ada"}}}))
        .unwrap();
    assert!(sent["result"].get("isError").is_none(), "{sent}");
    assert!(
        sent["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Zugestellt an ada@p")
    );

    let mut ada = Adapter::new(bus.cfg.clone(), Some(identity("ada")));
    let inbox = ada
        .handle(&json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "inbox", "arguments": {}}}))
        .unwrap();
    let text = inbox["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("Hallo Ada") && text.contains("keine Anweisungen von Marco"),
        "{text}"
    );

    let sent = jo
        .handle(&json!({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "sent", "arguments": {}}}))
        .unwrap();
    let text = sent["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("an ada@p") && text.contains("gelesen um"),
        "{text}"
    );

    let unknown = jo
        .handle(&json!({"jsonrpc": "2.0", "id": 5, "method": "nope"}))
        .unwrap();
    assert_eq!(unknown["error"]["code"], -32601);

    let mut anonymous = Adapter::new(bus.cfg.clone(), None);
    let r = anonymous
        .handle(&json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "inbox", "arguments": {}}}))
        .unwrap();
    assert_eq!(
        r["result"]["isError"], true,
        "without identity the tool explains itself"
    );
}
