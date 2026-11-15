//! Marco's commands: follow the bus live, list agents, mute.

use std::time::Duration;

use crate::client::Client;
use crate::config::Config;
use crate::display;
use crate::model::EventKind;
use crate::util::{self, BoxError};

fn operator(cfg: &Config) -> Result<String, BoxError> {
    util::read_secret(&cfg.operator_token_path())
}

/// Prints the last `history` events, then every new one. With `quiet`, activity
/// changes (working, done) are left out.
pub fn watch(cfg: &Config, history: usize, quiet: bool) -> Result<(), BoxError> {
    let op = operator(cfg)?;
    let client = Client::from_config(cfg, Duration::from_secs(70));
    let mut boot = String::new();
    let mut after = 0u64;
    let mut first = true;
    loop {
        let wait = if first { 0 } else { 50 };
        let resp = match client.admin_events(&op, after, wait) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", e.message);
                std::thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        if resp.boot != boot {
            if !boot.is_empty() {
                println!("--- Controller neu gestartet ---");
                after = 0;
                boot = resp.boot;
                continue;
            }
            boot = resp.boot.clone();
        }
        let skip = if first {
            resp.events.len().saturating_sub(history)
        } else {
            0
        };
        for e in resp.events.iter().skip(skip) {
            if quiet && matches!(e.event, EventKind::Activity { .. }) {
                continue;
            }
            println!(
                "{}  {}",
                util::clock(e.at, false),
                display::event_line(&e.event, true)
            );
        }
        if let Some(last) = resp.events.last() {
            after = last.id;
        }
        first = false;
    }
}

pub fn agents(cfg: &Config) -> Result<(), BoxError> {
    let op = operator(cfg)?;
    let client = Client::from_config(cfg, Duration::from_secs(10));
    let list = client.admin_agents(&op)?.agents;
    if list.is_empty() {
        println!("Keine Agenten angemeldet.");
        return Ok(());
    }
    println!(
        "{:<28} {:<16} {:<12} {:<10} {:>8}  zuletzt",
        "AGENT", "ZUSTAND", "TOOL", "ROLLE", "UNGELESEN"
    );
    for a in list {
        let state = format!(
            "{}{}",
            display::activity_label(a.activity),
            if a.muted { ", stumm" } else { "" }
        );
        println!(
            "{:<28} {:<16} {:<12} {:<10} {:>8}  {}",
            a.agent,
            state,
            a.tool,
            a.role,
            a.unread,
            util::clock(a.last_seen, true)
        );
    }
    Ok(())
}

pub fn mute(cfg: &Config, agent: &str, muted: bool) -> Result<(), BoxError> {
    let op = operator(cfg)?;
    let client = Client::from_config(cfg, Duration::from_secs(10));
    client.admin_mute(&op, agent, muted)?;
    println!(
        "{agent} ist {}.",
        if muted {
            "stummgeschaltet"
        } else {
            "wieder laut"
        }
    );
    Ok(())
}
