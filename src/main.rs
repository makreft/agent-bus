use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

use agent_bus::config::Config;
use agent_bus::hook::{self, Notify, ReadyOptions};
use agent_bus::model::ReadyMode;
use agent_bus::{admin, mcp, server};

#[derive(Parser)]
#[command(
    name = "agent-bus",
    version,
    about = "Nachrichten und Wecken zwischen KI-Agenten in verschiedenen Tools"
)]
struct Cli {
    /// Konfigurationsdatei (Standard: ~/.config/agent-bus/config.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Startet den Controller
    Serve,
    /// MCP-Server auf stdio für eine Agenten-Session (Identität aus AGENT_BUS_*)
    Mcp,
    /// Hook "ich bin fertig": wartet auf Nachrichten und weckt den Agenten
    Ready {
        /// new: nur für noch nicht gemeldete Nachrichten, unread: für alle ungelesenen
        #[arg(long, value_enum, default_value_t = ModeArg::New)]
        mode: ModeArg,
        /// Längste Wartezeit in Sekunden (kleiner als der Hook-Timeout des Tools)
        #[arg(long, default_value_t = 86_000)]
        max_wait: u64,
        /// exit2: Hinweis auf stderr und Exit 2 (asyncRewake), stdout: Hinweis auf stdout
        #[arg(long, value_enum, default_value_t = NotifyArg::Exit2)]
        notify: NotifyArg,
        /// Nur einmal nachsehen statt zu warten
        #[arg(long)]
        once: bool,
    },
    /// Hook "ich arbeite": ein Turn beginnt
    Busy,
    /// Hook "ich gehe": die Session endet
    Bye,
    /// Verfolgt den ganzen Verkehr live (Marco)
    Watch {
        /// So viele vergangene Ereignisse zuerst zeigen
        #[arg(long, default_value_t = 30)]
        history: usize,
        /// Ohne die Meldungen "arbeitet" und "fertig"
        #[arg(long)]
        quiet: bool,
    },
    /// Zeigt alle Agenten und ihren Zustand (Marco)
    Agents,
    /// Schaltet einen Agenten stumm, z.B. jo@agent-bus (Marco)
    Mute { agent: String },
    /// Hebt die Stummschaltung auf (Marco)
    Unmute { agent: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeArg {
    New,
    Unread,
}

#[derive(Clone, Copy, ValueEnum)]
enum NotifyArg {
    Exit2,
    Stdout,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let is_hook = matches!(cli.cmd, Cmd::Ready { .. } | Cmd::Busy | Cmd::Bye);
    let cfg = match Config::load(cli.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("agent-bus: {e}");
            // A broken config must not break an agent's turn.
            return if is_hook {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
    };
    let code = match cli.cmd {
        Cmd::Ready {
            mode,
            max_wait,
            notify,
            once,
        } => {
            let opts = ReadyOptions {
                mode: match mode {
                    ModeArg::New => ReadyMode::New,
                    ModeArg::Unread => ReadyMode::Unread,
                },
                max_wait: Duration::from_secs(max_wait),
                notify: match notify {
                    NotifyArg::Exit2 => Notify::Exit2,
                    NotifyArg::Stdout => Notify::Stdout,
                },
                once,
            };
            return ExitCode::from(hook::ready_cli(&cfg, &opts) as u8);
        }
        Cmd::Busy => return ExitCode::from(hook::busy_cli(&cfg) as u8),
        Cmd::Bye => return ExitCode::from(hook::bye_cli(&cfg) as u8),
        Cmd::Serve => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| e.into())
            .and_then(|rt| rt.block_on(server::serve(cfg))),
        Cmd::Mcp => mcp::run(&cfg),
        Cmd::Watch { history, quiet } => admin::watch(&cfg, history, quiet),
        Cmd::Agents => admin::agents(&cfg),
        Cmd::Mute { agent } => admin::mute(&cfg, &agent, true),
        Cmd::Unmute { agent } => admin::mute(&cfg, &agent, false),
    };
    match code {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("agent-bus: {e}");
            ExitCode::FAILURE
        }
    }
}
