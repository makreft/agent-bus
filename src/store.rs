//! Persistence in a single redb file. The controller keeps its working state in
//! memory and writes every change through to this store, so a restart loses nothing
//! but the live event log.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::model::{Agent, Message};
use crate::util::BoxError;

const AGENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("agents");
const MESSAGES: TableDefinition<u64, &[u8]> = TableDefinition::new("messages");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

pub struct Store {
    db: Database,
}

pub struct Loaded {
    pub agents: Vec<Agent>,
    pub messages: Vec<Message>,
    pub next_id: u64,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store, BoxError> {
        let db = Database::create(path)?;
        let tx = db.begin_write()?;
        {
            tx.open_table(AGENTS)?;
            tx.open_table(MESSAGES)?;
            tx.open_table(META)?;
        }
        tx.commit()?;
        Ok(Store { db })
    }

    pub fn load(&self) -> Result<Loaded, BoxError> {
        let tx = self.db.begin_read()?;
        let mut agents = Vec::new();
        for row in tx.open_table(AGENTS)?.iter()? {
            let (_, v) = row?;
            agents.push(serde_json::from_slice(v.value())?);
        }
        let mut messages = Vec::new();
        for row in tx.open_table(MESSAGES)?.iter()? {
            let (_, v) = row?;
            messages.push(serde_json::from_slice(v.value())?);
        }
        let next_id = tx
            .open_table(META)?
            .get("next_id")?
            .map(|v| v.value())
            .unwrap_or(1);
        Ok(Loaded {
            agents,
            messages,
            next_id,
        })
    }

    pub fn put_agent(&self, agent: &Agent) -> Result<(), BoxError> {
        let bytes = serde_json::to_vec(agent)?;
        let tx = self.db.begin_write()?;
        tx.open_table(AGENTS)?
            .insert(agent.key().as_str(), bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_agents(&self, keys: &[String]) -> Result<(), BoxError> {
        if keys.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(AGENTS)?;
            for k in keys {
                t.remove(k.as_str())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_message(&self, msg: &Message, next_id: u64) -> Result<(), BoxError> {
        let bytes = serde_json::to_vec(msg)?;
        let tx = self.db.begin_write()?;
        {
            tx.open_table(MESSAGES)?.insert(msg.id, bytes.as_slice())?;
            tx.open_table(META)?.insert("next_id", next_id)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Writes changed messages (read receipts) in one transaction.
    pub fn put_messages(&self, msgs: &[Message]) -> Result<(), BoxError> {
        if msgs.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(MESSAGES)?;
            for m in msgs {
                let bytes = serde_json::to_vec(m)?;
                t.insert(m.id, bytes.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn delete_messages(&self, ids: &[u64]) -> Result<(), BoxError> {
        if ids.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(MESSAGES)?;
            for id in ids {
                t.remove(*id)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}
