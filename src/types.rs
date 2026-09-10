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
    #[allow(dead_code)] // TODO: show message time in the UI
    pub timestamp: i64,
    pub content: String,
    pub is_read: bool,
}

/// Delivery state of a message, ordered from "not sent yet" to "read by peer".
///
/// The numeric values are part of the UI contract: `MessageEntry.status` in
/// `ui/app.slint` is an `int` with the same meaning.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryStatus {
    /// Locally queued, the server has not acknowledged it yet.
    #[default]
    Sending = 0,
    /// Stored by the server (server sent `msg_ack`).
    Sent = 1,
    /// Read by the peer (server sent `msg_read`).
    Read = 2,
}

/// A message stored in the local conversation cache.
#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub id: Uuid,
    #[allow(dead_code)]
    pub sender_chat_id: i64,
    pub text: String,
    pub outgoing: bool,
    pub status: DeliveryStatus,
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
