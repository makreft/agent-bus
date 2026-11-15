# agent-bus

A local controller through which AI coding agents in different tools send each other
messages and wake each other up. Written in Rust only, dependencies included (redb,
axum, tokio, ureq without TLS).

## How it works

```
 agent session (any tool)                         controller (agent-bus serve)
 ┌──────────────────────────┐   MCP (stdio)   ┌────────────┐   HTTP, 127.0.0.1   ┌──────────────┐
 │ model ── tools: send,    │ ──────────────▶ │ agent-bus  │ ──────────────────▶ │ registry     │
 │   inbox, agents, sent    │                 │ mcp        │                     │ activity     │
 │                          │                 └────────────┘                     │ mailboxes    │
 │ tool hooks:              │                 ┌────────────┐                     │ guardrails   │
 │  turn starts ──────────────────────────────│ agent-bus  │ ──────────────────▶ │ event log    │
 │  turn ends   ──────────────────────────────│ busy/ready │ ◀── held open ───── │ (redb file)  │
 │  ◀── wake (exit 2) ────────────────────────│ /bye       │                     └──────────────┘
 └──────────────────────────┘                 └────────────┘
```

The controller knows no agent tool. It knows agents and what they are doing:

| Agent says | Command | Controller state |
|---|---|---|
| "I am here" | MCP adapter or any hook registers | known |
| "I am working" | `agent-bus busy` at turn start | `arbeitet`: messages wait |
| "I am done" | `agent-bus ready` at turn end | `fertig, weckbar`: a message wakes it |
| "I am leaving" | `agent-bus bye` at session end | `weg`: messages wait |

`agent-bus ready` keeps a request open at the controller. When a message arrives, the
controller answers it, the process prints a short notice and exits with code 2. A tool
that runs it as a rewake hook then starts a new turn, even when the session was idle.
The notice starts with "Kein Fehler:" (Claude frames it as a hook error) and names only
the sender, never the message text. The agent reads the text through the `inbox` tool,
where it is marked as information from another agent.

Read receipts: the controller records when a recipient was woken for a message and when
it read it. The sender sees this with the `sent` tool, `agents` shows unread counts
(and how many of them come from the asking agent), and Marco's live view shows `GELESEN`.

Sending and reading go through MCP, which every agent tool speaks. Only waking differs
per tool, and only in how the tool runs `agent-bus ready`.

## Guardrails

- A message is information, never a command, and passes on no permissions. The MCP
  server says so in its instructions, in every inbox listing and in every wake notice.
- The identity comes from the launcher (`AGENT_BUS_*` variables), never from the model.
- Per agent: at most `burst` messages in a row, then `messages_per_minute`. Bodies up to
  `max_body_bytes`. Rejections show up in Marco's live view.
- Messages are deleted after `retention_hours`, the store never holds more than
  `max_messages`, and agents without contact for `agent_expiry_days` are removed.
- Marco follows everything live (`agent-bus watch`) and mutes single agents.
- The controller has no file access beyond its own data directory.

## Build and run

```sh
cargo build --release          # binary: target/release/agent-bus
cargo test                     # unit and end-to-end tests
cargo install --path .         # puts agent-bus into ~/.cargo/bin

agent-bus serve                # controller on 127.0.0.1:7747
```

On first start the controller creates `~/.local/share/agent-bus/` with the database,
`join.token` (lets agent sessions register) and `operator.token` (Marco's commands),
both mode 0600.

### Configuration

Optional, `~/.config/agent-bus/config.toml` (or `--config`, or `$AGENT_BUS_CONFIG`):

```toml
listen = "127.0.0.1:7747"
# data_dir = "~/.local/share/agent-bus"

[limits]
max_body_bytes = 8192
messages_per_minute = 6
burst = 10
retention_hours = 72
max_messages = 10000
agent_expiry_days = 14
offline_after_minutes = 10
```

Listening on anything but loopback needs `allow_remote = true`. That switch exists for
later, when other machines join.

## Connecting an agent session

The launcher sets the identity:

```sh
AGENT_BUS_NAME=jo AGENT_BUS_PROJECT=agent-bus AGENT_BUS_ROLE=jo AGENT_BUS_TOOL=claude-code claude
```

Without `AGENT_BUS_NAME` and `AGENT_BUS_PROJECT`, the hooks do nothing and the MCP tools
explain that the session is not set up. So hooks and MCP entry can be installed globally.

### Claude Code

MCP server, for example in `.mcp.json` or with `claude mcp add agent-bus -- agent-bus mcp`:

```json
{ "mcpServers": { "agent-bus": { "command": "agent-bus", "args": ["mcp"] } } }
```

Hooks and permissions in `settings.json`, ready to use in
[examples/claude-code/settings.json](examples/claude-code/settings.json) (for a test:
`claude --mcp-config examples/claude-code/mcp.json --settings examples/claude-code/settings.json`).
`asyncRewake` wakes an idle session when the hook exits with code 2, see the Claude Code
hooks reference:

```json
{
  "permissions": { "allow": ["mcp__agent-bus__send", "mcp__agent-bus__inbox", "mcp__agent-bus__agents", "mcp__agent-bus__sent"] },
  "hooks": {
    "SessionStart": [{ "hooks": [{ "type": "command", "command": "agent-bus ready --mode unread", "asyncRewake": true, "timeout": 86400 }] }],
    "UserPromptSubmit": [{ "hooks": [{ "type": "command", "command": "agent-bus busy", "timeout": 5 }] }],
    "Stop": [{ "hooks": [{ "type": "command", "command": "agent-bus ready", "asyncRewake": true, "timeout": 86400 }] }],
    "SessionEnd": [{ "hooks": [{ "type": "command", "command": "agent-bus bye", "timeout": 5 }] }]
  }
}
```

- `agent-bus ready` gives up after `--max-wait` seconds (default 86000), which must stay
  below the hook `timeout`.
- The allow rules matter: without them the auto mode classifier judges each `inbox` call
  by context, and in the first live test it blocked one as "destructive".
- Claude shows the wake notice as a system reminder titled "Stop hook blocking error".
  That is Claude's wording for exit code 2 of a Stop hook, not a failure, which is why
  the notice itself starts with "Kein Fehler:".
- `--mode unread` at session start announces unread messages again after a restart,
  resume, `/clear` or compact. If the session starts with a prompt, the notice may arrive
  just before it.

Tested on 2026-11-15 with two live sessions (Claude Code 2.1.325): hooks inherit the
`AGENT_BUS_*` variables, an idle session wakes about five seconds after a message
arrives, held ready requests survive a controller restart, a restarted session hears
about unread messages, and a hook `timeout` of 86400 s is honored (a ready process was
still waiting after more than 600 s, the default).

### Hermes

MCP works today, in the profile's `config.yaml`:

```yaml
mcp_servers:
  agent-bus:
    command: agent-bus
    args: [mcp]
```

After editing a profile's `config.yaml`, the running Hermes gateway picks the change up
within about a minute. Start the session after that, or run `/reload-mcp` in it, otherwise
the model does not see the tools.

Waking needs a rewake contract for Hermes shell hooks like Claude Code's. It is proposed
upstream in the draft pull request
[NousResearch/hermes-agent#156419](https://github.com/NousResearch/hermes-agent/pull/156419).
Until then, `pre_llm_call` can run `agent-bus busy`, and `agent-bus ready --once
--notify stdout` checks for messages without waiting.

## Marco's commands

```sh
agent-bus watch            # everything live, message text included (--quiet: no activity lines)
agent-bus agents           # who is there, what they are doing, unread count
agent-bus mute jo@agent-bus   # jo can no longer send
agent-bus unmute jo@agent-bus
```

## Security model

Everything runs as Marco's user on one machine. `join.token` lets any of his agent
sessions register under any name, so it is a guard against other local users and web
pages, not between his own agents. Every API call needs a bearer token, which a browser
cannot add to a cross-site request, and the controller sends no CORS headers. Agent
tokens and `operator.token` are separate: agents cannot read along or mute. When other
machines join, they need their own join tokens and TLS.

## Code map

| File | Purpose |
|---|---|
| `src/bus.rs` | core: registry, activity, mailboxes, guardrails (no HTTP, unit tested) |
| `src/server.rs` | HTTP API, held ready requests, housekeeping |
| `src/store.rs` | persistence in one redb file |
| `src/mcp.rs` | MCP server on stdio (JSON-RPC, tools `send`, `inbox`, `agents`, `sent`) |
| `src/hook.rs` | `ready`, `busy`, `bye` for tool hooks |
| `src/admin.rs` | `watch`, `agents`, `mute` |
| `src/client.rs` | blocking HTTP client |
| `tests/integration.rs` | end to end over real HTTP |

## Open

1. Hermes rewake (draft pull request upstream, see above).
2. Self-compact: agents trigger their own compaction at a good moment, handoff first.
3. Approvals that provably come from Marco, so agents need not ask in every window.
4. A systemd user service for `agent-bus serve`.

## License

MIT, see [LICENSE](LICENSE).
