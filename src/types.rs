use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use uuid::Uuid;

use Zeevum_protocol::{ClientMsg, ServerMsg};

#[derive(Clone, Copy, PartialEq)]
pub enum LogColor {
    White = 0,
    Green = 1,
    Red = 2,
    Yellow = 3,
    Blue = 4,
    Magenta = 5,
}

impl From<LogColor> for i32 {
    fn from(c: LogColor) -> Self {
        c as i32
    }
}

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub message_id: Uuid,
    pub sender_chat_id: i64,
    pub timestamp: i64,
    pub content: String,
    pub is_read: bool,
}

pub enum UiEvent {
    Server(ServerMsg),
    HistoryBatch {
        peer_chat_id: i64,
        entries: Vec<HistoryEntry>,
    },
    Disconnected(String),
}

pub type CmdSender = Arc<Mutex<Option<mpsc::UnboundedSender<ClientMsg>>>>;
