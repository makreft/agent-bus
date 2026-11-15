//! Text for people: the controller log and Marco's live view.

use crate::model::{Activity, EventKind};

pub fn activity_label(a: Activity) -> &'static str {
    match a {
        Activity::Offline => "weg",
        Activity::Busy => "arbeitet",
        Activity::Ready => "fertig, weckbar",
        Activity::Idle => "da, nicht weckbar",
    }
}

/// One line per event. With `with_body`, message bodies follow indented.
pub fn event_line(e: &EventKind, with_body: bool) -> String {
    match e {
        EventKind::Registered { agent, tool, role } => {
            format!("ANGEMELDET  {agent} ({tool}, Rolle {role})")
        }
        EventKind::Activity { agent, activity } => match activity {
            Activity::Busy => format!("ARBEITET    {agent}"),
            Activity::Ready => format!("FERTIG      {agent}, weckbar"),
            Activity::Idle => format!("FERTIG      {agent}"),
            Activity::Offline => format!("WEG         {agent}"),
        },
        EventKind::Message { id, from, to, body } => {
            if with_body {
                format!("NACHRICHT   {from} -> {to} (#{id})\n{}", indent(body))
            } else {
                format!("NACHRICHT   {from} -> {to} (#{id}, {} Bytes)", body.len())
            }
        }
        EventKind::Woken { agent, count } => format!("GEWECKT     {agent} ({count} neu)"),
        EventKind::Read { agent, ids } => {
            let ids: Vec<String> = ids.iter().map(|id| format!("#{id}")).collect();
            format!("GELESEN     {agent} ({})", ids.join(", "))
        }
        EventKind::Rejected { from, to, reason } => format!("ABGELEHNT   {from} -> {to}: {reason}"),
        EventKind::Muted { agent, muted: true } => format!("STUMM       {agent}"),
        EventKind::Muted {
            agent,
            muted: false,
        } => format!("WIEDER LAUT {agent}"),
        EventKind::Expired { agents, messages } => {
            let who = if agents.is_empty() {
                "keine".to_string()
            } else {
                agents.join(", ")
            };
            format!("AUFGERÄUMT  {messages} alte Nachrichten, entfernte Agenten: {who}")
        }
    }
}

fn indent(body: &str) -> String {
    body.lines()
        .map(|l| format!("            {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}
