//! Configuration: `~/.config/agent-bus/config.toml`, every field optional.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::util::BoxError;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Address the controller listens on.
    pub listen: String,
    /// Listening on anything but loopback needs this switch. Meant for later, when
    /// other machines join.
    pub allow_remote: bool,
    /// Database and secrets. Nothing else is ever written by the controller.
    pub data_dir: PathBuf,
    pub limits: Limits,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Maximum size of one message body in bytes.
    pub max_body_bytes: usize,
    /// Sustained sending rate per agent.
    pub messages_per_minute: u32,
    /// Messages an agent may send in a row before the rate applies.
    pub burst: u32,
    /// Messages are deleted after this many hours, read or not.
    pub retention_hours: u64,
    /// Hard cap on stored messages. The oldest go first.
    pub max_messages: usize,
    /// Agents without contact for this many days are removed.
    pub agent_expiry_days: u64,
    /// Without contact for this many minutes an agent counts as offline.
    pub offline_after_minutes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "127.0.0.1:7747".into(),
            allow_remote: false,
            data_dir: default_data_dir(),
            limits: Limits::default(),
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_body_bytes: 8 * 1024,
            messages_per_minute: 6,
            burst: 10,
            retention_hours: 72,
            max_messages: 10_000,
            agent_expiry_days: 14,
            offline_after_minutes: 10,
        }
    }
}

impl Config {
    /// Loads the given file, or `$AGENT_BUS_CONFIG`, or the default path. A missing
    /// file means defaults.
    pub fn load(path: Option<&Path>) -> Result<Config, BoxError> {
        let path = match path {
            Some(p) => p.to_path_buf(),
            None => match std::env::var_os("AGENT_BUS_CONFIG") {
                Some(p) => PathBuf::from(p),
                None => default_config_path(),
            },
        };
        let mut cfg = match std::fs::read_to_string(&path) {
            Ok(text) => {
                toml::from_str::<Config>(&text).map_err(|e| format!("{}: {e}", path.display()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Config::default(),
            Err(e) => return Err(format!("{}: {e}", path.display()).into()),
        };
        cfg.data_dir = expand_home(&cfg.data_dir);
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), BoxError> {
        let addr: SocketAddr = self
            .listen
            .parse()
            .map_err(|e| format!("listen = {:?} ist keine Adresse: {e}", self.listen))?;
        if !addr.ip().is_loopback() && !self.allow_remote {
            return Err(format!(
                "listen = {} ist nicht lokal. Das braucht allow_remote = true.",
                self.listen
            )
            .into());
        }
        let l = &self.limits;
        if l.max_body_bytes == 0
            || l.messages_per_minute == 0
            || l.burst == 0
            || l.retention_hours == 0
            || l.max_messages == 0
            || l.agent_expiry_days == 0
            || l.offline_after_minutes == 0
        {
            return Err("Alle Werte unter [limits] müssen größer als 0 sein.".into());
        }
        Ok(())
    }

    /// Where clients reach the controller. `$AGENT_BUS_URL` overrides.
    pub fn base_url(&self) -> String {
        std::env::var("AGENT_BUS_URL").unwrap_or_else(|_| format!("http://{}", self.listen))
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("bus.redb")
    }

    /// Lets agents register. Every agent session of Marco's user can read it.
    pub fn join_token_path(&self) -> PathBuf {
        self.data_dir.join("join.token")
    }

    /// Marco's token: read along, mute, list everything.
    pub fn operator_token_path(&self) -> PathBuf {
        self.data_dir.join("operator.token")
    }
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

fn expand_home(p: &Path) -> PathBuf {
    match p.strip_prefix("~") {
        Ok(rest) => home().join(rest),
        Err(_) => p.to_path_buf(),
    }
}

fn default_data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/share"));
    base.join("agent-bus")
}

fn default_config_path() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".config"));
    base.join("agent-bus/config.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_local() {
        let c = Config::default();
        c.validate().unwrap();
        assert!(c.listen.starts_with("127.0.0.1:"));
    }

    #[test]
    fn remote_needs_switch() {
        let mut c = Config {
            listen: "0.0.0.0:7747".into(),
            ..Config::default()
        };
        assert!(c.validate().is_err());
        c.allow_remote = true;
        c.validate().unwrap();
    }

    #[test]
    fn parses_partial_file() {
        let c: Config = toml::from_str("[limits]\nburst = 3\n").unwrap();
        assert_eq!(c.limits.burst, 3);
        assert_eq!(c.limits.max_body_bytes, 8 * 1024);
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<Config>("lissen = \"x\"\n").is_err());
    }
}
