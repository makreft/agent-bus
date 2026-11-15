//! Small helpers: time, tokens, secret files.

use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 32 random bytes as 64 hex characters.
pub fn random_token() -> String {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).expect("operating system random source failed");
    hex(&buf)
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

/// Comparison that does not stop at the first differing byte.
pub fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Creates the directory (and parents) readable only by the owner.
pub fn ensure_private_dir(path: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

/// Reads a secret file, creating it with a fresh token (mode 0600) when missing.
pub fn ensure_secret(path: &Path) -> std::io::Result<String> {
    match fs::read_to_string(path) {
        Ok(s) if !s.trim().is_empty() => Ok(s.trim().to_string()),
        Ok(_) | Err(_) => {
            let token = random_token();
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(path)?;
            f.write_all(token.as_bytes())?;
            f.write_all(b"\n")?;
            Ok(token)
        }
    }
}

pub fn read_secret(path: &Path) -> Result<String, BoxError> {
    let s = fs::read_to_string(path).map_err(|e| {
        format!(
            "{} nicht lesbar ({e}). Ist der Controller (agent-bus serve) schon einmal gelaufen?",
            path.display()
        )
    })?;
    Ok(s.trim().to_string())
}

/// Local wall clock time for people, UTC if the local offset is unknown.
pub fn clock(ms: u64, with_date: bool) -> String {
    use time::macros::format_description;
    let Ok(dt) = time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000) else {
        return ms.to_string();
    };
    let (dt, suffix) = match time::UtcOffset::current_local_offset() {
        Ok(off) => (dt.to_offset(off), ""),
        Err(_) => (dt, " UTC"),
    };
    let text = if with_date {
        dt.format(format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second]"
        ))
    } else {
        dt.format(format_description!("[hour]:[minute]:[second]"))
    };
    match text {
        Ok(t) => format!("{t}{suffix}"),
        Err(_) => ms.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_hex_and_distinct() {
        let a = random_token();
        let b = random_token();
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn constant_time_eq() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "abcd"));
    }

    #[test]
    fn secret_file_is_private_and_stable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s.token");
        let a = ensure_secret(&p).unwrap();
        let b = ensure_secret(&p).unwrap();
        assert_eq!(a, b);
        let mode = fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
