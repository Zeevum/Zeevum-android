use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::mpsc;
use uuid::Uuid;

use zeevum_protocol::{ClientMsg, ServerMsg};

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub message_id: Uuid,
    pub sender_user_id: i64,
    pub timestamp: i64,
    pub content: String,
    pub is_read: bool,
}

/// The numeric values are part of the UI contract, `MessageEntry.status` in
/// `ui/app.slint` is an `int` with the same meaning
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryStatus {
    /// Locally queued, the server has not acknowledged it yet
    #[default]
    Sending = 0,
    /// Stored by the server, it sent `msg_ack`
    Sent = 1,
    /// Read by the peer, the server sent `msg_read`
    Read = 2,
    /// Given up on, the server never acknowledged it
    Failed = 3,
}

/// An outgoing message the server has not acknowledged yet. Kept in the
/// state, not in the network task: the task is recreated on every
/// reconnect, and a send that was in flight has to survive that.
#[derive(Debug, Clone)]
pub struct PendingSend {
    pub conv_id: Uuid,
    pub content: String,
    /// Sends already made. The first one happened before this was created.
    pub attempts: u32,
    pub next_attempt_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub id: Uuid,
    #[allow(dead_code)]
    pub sender_user_id: i64,
    pub text: String,
    pub timestamp: i64,
    pub outgoing: bool,
    pub status: DeliveryStatus,
}

pub enum UiEvent {
    /// The link went down, and trying again could work.
    Disconnected(String),
    /// Trying again cannot help: a bad address, no trusted certificates, a
    /// token the server refuses, a frame we cannot parse. Retrying these
    /// hides the reason, and never succeeds.
    Fatal(String),
    Server(ServerMsg),
    HistoryBatch {
        conv_id: Uuid,
        entries: Vec<HistoryEntry>,
    },
}

pub type CmdSender = Arc<Mutex<Option<mpsc::UnboundedSender<ClientMsg>>>>;
