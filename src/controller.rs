use chrono::{DateTime, FixedOffset, Local, Utc};
use slint::{ComponentHandle, Model, SharedString, VecModel, Weak};
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::network::network_task;
use crate::settings;
use crate::types::{ChatMessage, CmdSender, DeliveryStatus, PendingSend, UiEvent};
use crate::{FriendEntry, MainWindow, MessageEntry};

use zeevum_protocol::{ClientMsg, ErrorCode, ServerMsg, UnreadEntry, UserBrief, MAX_MESSAGE_LEN};

#[derive(Clone)]
pub struct AppController {
    pub ui: Weak<MainWindow>,
    pub sender_slot: CmdSender,
    state: Arc<Mutex<ChatState>>,
    link: Arc<Mutex<Link>>,
    /// Held because attempts are started from the link thread, which is not
    /// inside the runtime and has no reactor of its own.
    runtime: Handle,
}

pub struct ChatState {
    pub my_user_id: i64,
    /// Peer of the open dialog. `None` - nothing open. The UI addresses a
    /// dialog by person, so this is what goes in `active-peer-id`.
    pub active_peer_id: Option<i64>,
    /// Login of the peer of the open dialog. Needed so `NotFriends` can
    /// offer to send a request, naming the person.
    pub active_peer_login: String,
    /// Conversation of the open dialog. `None` until the server answers
    /// `ResolveDm`. Not the same thing as `active_peer_id`, a person and a
    /// conversation are different.
    pub active_conv: Option<Uuid>,
    /// Person to conversation. Cached so reopening a dialog does not ask
    /// the server again.
    pub conv: HashMap<i64, Uuid>,
    pub server_addr: String,
    pub login: String,
    pub friends: Vec<UserBrief>,
    /// Messages by conversation, not by peer.
    pub messages: HashMap<Uuid, Vec<ChatMessage>>,
    pub incoming_reqs: Vec<UserBrief>,
    /// Lives in the state, not in the UI model. The friend list model is
    /// rebuilt from that state, so a counter kept in a widget would be lost
    /// on every rebuild.
    pub unread: HashMap<i64, usize>,
    /// Sent, not yet acknowledged, by message id. Survives a reconnect, the
    /// retry thread keeps its own clock and asks this map what is due.
    pub pending: HashMap<Uuid, PendingSend>,
}

/// Who owns the attempts to be connected. Kept apart from `ChatState`, it
/// describes the link, not the conversation, and one does outlive the other:
/// the chats stay on screen while the link is down.
#[derive(Default)]
pub struct Link {
    /// False once the reader has had enough, or once a failure came back
    /// that no retry can fix. Nothing reconnects behind their back.
    pub wanted: bool,
    pub connected: bool,
    pub connecting: bool,
    /// Attempts made since the last success. Drives the delay, nothing else.
    pub attempts: u32,
    pub next_attempt_at: Option<Instant>,
}

/// Quick retries, long enough to cover a hiccup or a server restart, short
/// enough that nobody notices them.
const FAST_ATTEMPTS: u32 = 3;
const SLOW_ATTEMPTS: u32 = 3;
const FAST_DELAY: Duration = Duration::from_secs(3);
const SLOW_DELAY: Duration = Duration::from_secs(10);
const IDLE_DELAY: Duration = Duration::from_secs(30);
/// How often the link is looked at.
const LINK_TICK: Duration = Duration::from_millis(250);

/// How long to wait after the attempt numbered `index`, counting from zero.
/// Three quick ones, three patient ones, then one every half a minute for as
/// long as it takes.
fn next_delay(index: u32) -> Duration {
    if index < FAST_ATTEMPTS {
        FAST_DELAY
    } else if index < FAST_ATTEMPTS + SLOW_ATTEMPTS {
        SLOW_DELAY
    } else {
        IDLE_DELAY
    }
}

/// Whether waiting has become long enough that the reader wants a button.
fn is_slow(attempts: u32) -> bool {
    attempts > FAST_ATTEMPTS
}

/// 0 idle, 1 connecting, 2 waiting, 3 online. The numbers are the contract
/// with `link-state` in `ui/app.slint`.
fn link_status(link: &Link) -> i32 {
    if link.connected {
        3
    } else if !link.wanted {
        0
    } else if is_slow(link.attempts) {
        2
    } else {
        1
    }
}

/// Retries the link. One thread for the life of the app, so a dropped
/// connection is an event, not the end of the session. The decision is taken
/// under the lock and acted on after it, never the other way round: the
/// attempt itself reaches for the state lock, and something already holding
/// that lock reaches for this one.
/// The supervisor has to outlive anything that happens inside one attempt,
/// so a poisoned lock is taken back instead of killing it.
fn lock_link(link: &Mutex<Link>) -> MutexGuard<'_, Link> {
    link.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn spawn_link_task(controller: AppController, link: Arc<Mutex<Link>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(LINK_TICK);

        let due = {
            let link_lock = lock_link(&link);
            !link_lock.wanted
                || link_lock.connected
                || link_lock.connecting
                || link_lock
                    .next_attempt_at
                    .is_none_or(|at| at > Instant::now())
        };
        if due {
            continue;
        }

        {
            let mut link_lock = lock_link(&link);
            link_lock.connecting = true;
            link_lock.next_attempt_at = None;
        }

        // A panic inside an attempt ends the attempt, not the supervisor.
        // Otherwise one bad try would stop every later one, the link would
        // never be tried again, and the reader would watch the corner sit on
        // "Connecting..." for as long as the app ran.
        if panic::catch_unwind(AssertUnwindSafe(|| controller.start_attempt())).is_err() {
            let mut link_lock = lock_link(&link);
            link_lock.connecting = false;
            link_lock.attempts += 1;
            link_lock.next_attempt_at = Some(Instant::now() + next_delay(link_lock.attempts - 1));
            drop(link_lock);
            controller.set_link_status();
        }
    });
}

impl AppController {
    pub fn new(ui: Weak<MainWindow>) -> Self {
        let controller = Self {
            ui,
            sender_slot: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(ChatState {
                my_user_id: 0,
                active_peer_id: None,
                active_peer_login: String::new(),
                active_conv: None,
                conv: HashMap::new(),
                server_addr: String::new(),
                login: String::new(),
                friends: Vec::new(),
                messages: HashMap::new(),
                incoming_reqs: Vec::new(),
                unread: HashMap::new(),
                pending: HashMap::new(),
            })),
            link: Arc::new(Mutex::new(Link::default())),
            runtime: Handle::current(),
        };

        spawn_retry_task(
            controller.state.clone(),
            controller.sender_slot.clone(),
            controller.ui.clone(),
        );
        spawn_link_task(controller.clone(), controller.link.clone());
        controller
    }

    pub fn handle_check_password_strength(&self, password: SharedString) {
        if let Some(ui) = self.ui.upgrade() {
            let pass_str = password.to_string();
            if pass_str.len() < 8 {
                ui.set_password_strength(0);
                ui.set_password_strength_text("Too short (min 8 chars)".into());
                return;
            }

            if pass_str.is_empty() {
                ui.set_password_strength(0);
                ui.set_password_strength_text("".into());
                return;
            }

            let entropy = zxcvbn::zxcvbn(&pass_str, &[]);
            let score = entropy.score() as i32;

            let text = match score {
                0 => "Too weak (needs improvement)",
                1 => "Weak (needs at least Fair)",
                2 => "Fair (minimum required)",
                3 => "Strong",
                4 => "Excellent",
                _ => "",
            };

            ui.set_password_strength(score);
            ui.set_password_strength_text(text.into());
        }
    }

    pub fn handle_connect(
        &self,
        addr: SharedString,
        login: SharedString,
        password: SharedString,
        is_register: bool,
    ) {
        let addr_str: String = addr.to_string();
        let login_str: String = login.to_string();
        let pass_str: String = password.to_string();

        {
            let mut state_lock = self.state.lock().unwrap();
            state_lock.server_addr = addr_str.clone();
            state_lock.login = login_str.clone();
        }

        let existing = settings::load_settings();
        settings::save_session(
            &addr_str,
            &login_str,
            existing.token.as_deref().unwrap_or(""),
            existing.user_id.unwrap_or(0),
            existing.expires_at.unwrap_or(0),
        );

        if let Some(ui) = self.ui.upgrade() {
            ui.set_status_message("Connecting...".into());
        }

        self.spawn_network_task(addr_str, login_str, pass_str, is_register);
    }

    /// One attempt at the link, whoever asked for it. The first one is not a
    /// retry, so it is not made to wait.
    fn spawn_network_task(&self, addr: String, login: String, password: String, is_register: bool) {
        let in_flight = self.sender_slot.lock().unwrap().is_some();

        {
            let mut link_lock = self.link.lock().unwrap();
            link_lock.wanted = true;
            if in_flight {
                // An attempt is already running, it owns the flag.
                link_lock.connecting = false;
            } else {
                link_lock.connecting = true;
                link_lock.next_attempt_at = None;
            }
        }
        self.set_link_status();

        if in_flight {
            return;
        }

        let controller_clone = self.clone();
        let (tx_cmd, rx_cmd) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut guard = self.sender_slot.lock().unwrap();
            *guard = Some(tx_cmd);
        }

        // On the handle, not with `tokio::spawn`: this runs on the link
        // thread, which is outside the runtime, and there is no reactor to
        // wake there. Outside the lock above, so that a failure cannot
        // poison it.
        self.runtime.spawn(async move {
            network_task(controller_clone, addr, login, password, is_register, rx_cmd).await;
        });
    }

    /// A reconnect attempt. The password is long gone by now and there is
    /// nobody to ask for it again, so this is a token login or nothing.
    pub(crate) fn start_attempt(&self) {
        let settings = settings::load_settings();

        let Some(addr) = settings.server_address else {
            self.handle_ui_event(UiEvent::Fatal("No server address saved.".into()));
            return;
        };

        let alive = settings
            .token
            .as_deref()
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false)
            && settings
                .expires_at
                .map(|e| e > Utc::now().timestamp())
                .unwrap_or(false);

        if !alive {
            self.handle_ui_event(UiEvent::Fatal("Session expired, log in again.".into()));
            return;
        }

        self.spawn_network_task(
            addr,
            settings.login.unwrap_or_default(),
            String::new(),
            false,
        );
    }

    /// The reader is not willing to wait out the timer. One attempt, now.
    pub fn handle_reconnect_now(&self) {
        let mut link_lock = self.link.lock().unwrap();
        if link_lock.connected || link_lock.connecting || !link_lock.wanted {
            return;
        }
        link_lock.next_attempt_at = Some(Instant::now());
    }

    /// 0 idle, 1 connecting, 2 waiting, 3 online.
    fn set_link_status(&self) {
        let status = link_status(&lock_link(&self.link));
        let weak = self.ui.clone();
        slint::invoke_from_event_loop(move || {
            if let Some(ui) = weak.upgrade() {
                ui.set_link_state(status);
            }
        })
        .ok();
    }

    /// No link, and no attempt to get one back. For a failure that retrying
    /// cannot fix, and for the reader giving up on purpose.
    fn give_up(&self, state_lock: &mut ChatState, msg: &str) {
        {
            let mut link_lock = self.link.lock().unwrap();
            link_lock.wanted = false;
            link_lock.connected = false;
            link_lock.connecting = false;
            link_lock.next_attempt_at = None;
        }

        state_lock.active_peer_id = None;
        state_lock.active_peer_login.clear();
        state_lock.active_conv = None;
        state_lock.conv.clear();
        state_lock.my_user_id = 0;
        state_lock.friends.clear();

        let weak = self.ui.clone();
        let msg = msg.to_string();
        slint::invoke_from_event_loop(move || {
            if let Some(ui) = weak.upgrade() {
                ui.set_status_message(msg.into());
                ui.set_current_screen(0);
                ui.set_active_peer_id(-1);
                ui.set_active_peer_login("".into());
            }
        })
        .ok();
        sync_friends(&self.ui, state_lock);
        sync_messages(&self.ui, state_lock);
        self.set_link_status();
    }

    pub fn try_auto_login(&self) {
        let settings = settings::load_settings();

        if let Some(ui) = self.ui.upgrade() {
            if let Some(addr) = settings.server_address.clone() {
                ui.set_server_address(addr.into());
            }
            if let Some(login) = settings.login.clone() {
                ui.set_login_text(login.into());
            }
        }

        let has_token = settings
            .token
            .as_deref()
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false);
        let not_expired = settings
            .expires_at
            .map(|e| e > chrono::Utc::now().timestamp())
            .unwrap_or(false);

        if let (Some(addr), true, true) = (settings.server_address, has_token, not_expired) {
            self.handle_connect(
                addr.into(),
                settings.login.unwrap_or_default().into(),
                "".into(),
                false,
            );
        }
    }

    pub fn handle_send_msg(&self, text: SharedString) {
        let text_str = text.to_string();

        if text_str.len() > MAX_MESSAGE_LEN {
            let ui_weak = self.ui.clone();
            slint::invoke_from_event_loop(move || {
                if let Some(ui) = ui_weak.upgrade() {
                    ui.set_status_message(
                        format!("Message is too long: {MAX_MESSAGE_LEN} bytes maximum.").into(),
                    );
                }
            })
            .ok();
            return;
        }

        let mut state_lock = self.state.lock().unwrap();

        let Some(conv_id) = state_lock.active_conv else {
            return;
        };

        if let Some(ui) = self.ui.upgrade() {
            let msg_uuid = Uuid::new_v4();
            let my_id = state_lock.my_user_id;
            state_lock
                .messages
                .entry(conv_id)
                .or_default()
                .push(ChatMessage {
                    id: msg_uuid,
                    sender_user_id: my_id,
                    text: text_str.clone(),
                    timestamp: Utc::now().timestamp(),
                    outgoing: true,
                    status: DeliveryStatus::Sending,
                });

            // The first attempt is now, the clock for the next one starts here.
            state_lock.pending.insert(
                msg_uuid,
                PendingSend {
                    conv_id,
                    content: text_str.clone(),
                    attempts: 1,
                    next_attempt_at: Instant::now() + SEND_TIMEOUT,
                },
            );

            // A sent message always ends at the newest, wherever the reader was.
            end_at_newest(&ui);
            sync_messages_now(&ui, &state_lock);

            let guard = self.sender_slot.lock().unwrap();
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(ClientMsg::SendMsg {
                    message_id: msg_uuid,
                    conv_id,
                    content: text_str,
                });
            }
        }
    }

    pub fn handle_open_chat(&self, peer_user_id: i32, login: SharedString) {
        let mut state_lock = self.state.lock().unwrap();
        let peer = peer_user_id as i64;
        let ui_weak = self.ui.clone();

        state_lock.active_peer_id = Some(peer);
        state_lock.active_peer_login = login.to_string();
        state_lock.active_conv = state_lock.conv.get(&peer).copied();
        state_lock.unread.insert(peer, 0);

        if let Some(ui) = self.ui.upgrade() {
            ui.set_active_peer_id(peer_user_id);
            ui.set_active_peer_login(login);
            // Opening a chat starts at the newest message, even if the one
            // open before was being read from the middle.
            end_at_newest(&ui);
            sync_messages_now(&ui, &state_lock);
        }

        {
            let guard = self.sender_slot.lock().unwrap();
            if let Some(tx) = guard.as_ref() {
                match state_lock.active_conv {
                    Some(conv_id) => {
                        let _ = tx.send(ClientMsg::HistoryReq { conv_id });
                        for id in unread_message_ids(&state_lock) {
                            let _ = tx.send(ClientMsg::MarkRead { message_id: id });
                        }
                    }
                    None => {
                        let _ = tx.send(ClientMsg::ResolveDm { peer_user_id: peer });
                    }
                }
            }
        }

        sync_friends(&ui_weak, &state_lock);
    }

    pub fn handle_search_user(&self, login: SharedString) {
        let login_str = login.to_string();
        let guard = self.sender_slot.lock().unwrap();
        if let Some(tx) = guard.as_ref() {
            let _ = tx.send(ClientMsg::SearchUser { login: login_str });
        }
    }

    pub fn handle_disconnect(&self) {
        {
            let mut guard = self.sender_slot.lock().unwrap();
            *guard = None;
        }
        // Nothing reconnects after the reader asked to leave.
        {
            let mut link_lock = self.link.lock().unwrap();
            link_lock.wanted = false;
            link_lock.connected = false;
            link_lock.connecting = false;
            link_lock.next_attempt_at = None;
        }
        settings::clear_session();
        {
            let mut state_lock = self.state.lock().unwrap();
            state_lock.active_peer_id = None;
            state_lock.active_peer_login.clear();
            state_lock.active_conv = None;
            state_lock.conv.clear();
            state_lock.my_user_id = 0;
            state_lock.messages.clear();
            state_lock.unread.clear();
            // Nobody is going to acknowledge these, and there is nobody left
            // to show them to.
            state_lock.pending.clear();
        }
        let state_lock = self.state.lock().unwrap();
        if let Some(ui) = self.ui.upgrade() {
            ui.set_current_screen(0);
            ui.set_active_peer_id(-1);
            ui.set_active_peer_login("".into());
            sync_messages_now(&ui, &state_lock);
        }
    }

    /// Revokes the session on the server, then forgets it locally.
    ///
    /// The frame has to go out before the token is erased. Without it the row
    /// in `sessions` survives and the token stays valid on the server until it
    /// expires -- a logout that never logged out.
    ///
    /// Deliberately not folded into `handle_disconnect`, which drops the
    /// connection and the saved token but leaves the session alive upstream.
    pub fn handle_logout(&self) {
        {
            let guard = self.sender_slot.lock().unwrap();
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(ClientMsg::Logout {
                    all_sessions: false,
                });
            }
        }
        self.handle_disconnect();
    }

    /// Screen 5. The old password is asked for even though the session is
    /// already proven: the server rejects the change without it, so a stolen
    /// token is not enough to take the account over.
    pub fn handle_change_password(&self, old_password: SharedString, new_password: SharedString) {
        if let Some(ui) = self.ui.upgrade() {
            ui.set_status_message("".into());
        }
        let guard = self.sender_slot.lock().unwrap();
        if let Some(tx) = guard.as_ref() {
            let _ = tx.send(ClientMsg::ChangePassword {
                old_password: old_password.to_string(),
                new_password: new_password.to_string(),
            });
        }
    }

    pub fn handle_save_settings(&self, addr: SharedString) {
        let addr_str = addr.to_string();
        let login_str = self
            .ui
            .upgrade()
            .map(|ui| ui.get_login_text().to_string())
            .unwrap_or_default();
        let settings = settings::load_settings();
        settings::save_session(
            &addr_str,
            &login_str,
            settings.token.unwrap_or_default().as_str(),
            settings.user_id.unwrap_or(0),
            settings.expires_at.unwrap_or(0),
        );
    }

    pub fn handle_ui_event(&self, event: UiEvent) {
        let mut state_lock = self.state.lock().unwrap();
        let sender_slot = self.sender_slot.clone();
        let ui_weak = self.ui.clone();

        match event {
            UiEvent::Disconnected(msg) => {
                let wanted = {
                    let mut link_lock = self.link.lock().unwrap();
                    link_lock.connected = false;
                    link_lock.connecting = false;
                    link_lock.attempts += 1;
                    link_lock.next_attempt_at = if link_lock.wanted {
                        Some(Instant::now() + next_delay(link_lock.attempts - 1))
                    } else {
                        None
                    };
                    link_lock.wanted
                };

                if wanted {
                    // The conversation stays on screen. Only the corner says
                    // the link is down, and it comes back on its own.
                    let weak = ui_weak.clone();
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = weak.upgrade() {
                            ui.set_status_message(msg.into());
                        }
                    })
                    .ok();
                } else {
                    self.give_up(&mut state_lock, &msg);
                }
                self.set_link_status();
            }
            UiEvent::Fatal(msg) => {
                self.give_up(&mut state_lock, &msg);
            }
            UiEvent::HistoryBatch { conv_id, entries } => {
                let mut stored = Vec::new();
                for e in entries {
                    let is_out = e.sender_user_id == state_lock.my_user_id;
                    let status = match (is_out, e.is_read) {
                        (_, true) => DeliveryStatus::Read,
                        (true, false) => DeliveryStatus::Sent,
                        (false, false) => DeliveryStatus::Sending,
                    };
                    stored.push(ChatMessage {
                        id: e.message_id,
                        sender_user_id: e.sender_user_id,
                        text: e.content,
                        timestamp: e.timestamp,
                        outgoing: is_out,
                        status,
                    });
                }
                state_lock.messages.insert(conv_id, stored);
                sync_messages_at_bottom(&ui_weak, &state_lock);

                if state_lock.active_conv == Some(conv_id) {
                    let guard = sender_slot.lock().unwrap();
                    if let Some(tx) = guard.as_ref() {
                        for id in unread_message_ids(&state_lock) {
                            let _ = tx.send(ClientMsg::MarkRead { message_id: id });
                        }
                    }
                }
            }
            UiEvent::Server(msg) => match msg {
                ServerMsg::AuthOk {
                    user_id,
                    token,
                    expires_at,
                    must_change_password,
                } => {
                    state_lock.my_user_id = user_id;
                    settings::save_session(
                        &state_lock.server_addr,
                        &state_lock.login,
                        &token,
                        user_id,
                        expires_at,
                    );
                    {
                        let mut link_lock = self.link.lock().unwrap();
                        link_lock.connected = true;
                        link_lock.connecting = false;
                        link_lock.attempts = 0;
                        link_lock.next_attempt_at = None;
                    }
                    self.set_link_status();
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            // Neither the temporary nor the just-set password
                            // may survive the switch, whichever way it goes.
                            if must_change_password || ui.get_current_screen() == 5 {
                                ui.set_old_password_text("".into());
                                ui.set_password_text("".into());
                                ui.set_confirm_password_text("".into());
                                ui.set_status_message("".into());
                            }
                            ui.set_current_screen(if must_change_password { 5 } else { 1 });
                        }
                    })
                    .ok();
                }
                ServerMsg::Error {
                    code,
                    detail: _detail,
                } => {
                    let connected = self.link.lock().unwrap().connected;
                    if !connected {
                        // There is no session yet, so this can only be about
                        // the login. Retrying would hide it.
                        self.give_up(&mut state_lock, &code.to_string());
                        return;
                    }
                    match code {
                        ErrorCode::NotFriends => {
                            let login = state_lock.active_peer_login.clone();
                            forget_conversation(&mut state_lock);
                            sync_messages(&ui_weak, &state_lock);
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_search_result(
                                        format!("You are not friends with {login}. Search for '{login}' to send a request.")
                                            .into(),
                                    );
                                }
                            })
                                .ok();
                        }
                        ErrorCode::NotAMember => {
                            forget_conversation(&mut state_lock);
                            sync_messages(&ui_weak, &state_lock);
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_status_message(
                                        "You are no longer a member of this conversation.".into(),
                                    );
                                }
                            })
                            .ok();
                        }
                        ErrorCode::ConversationNotFound => {
                            forget_conversation(&mut state_lock);
                            let peer = state_lock.active_peer_id;
                            sync_messages(&ui_weak, &state_lock);
                            if let Some(peer) = peer {
                                let guard = sender_slot.lock().unwrap();
                                if let Some(tx) = guard.as_ref() {
                                    let _ = tx.send(ClientMsg::ResolveDm { peer_user_id: peer });
                                }
                            }
                        }
                        ErrorCode::AlreadyFriends
                        | ErrorCode::NoPendingRequest
                        | ErrorCode::CannotTargetYourself
                        | ErrorCode::UserNotFound => {
                            let text = code.to_string();
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_search_result(text.into());
                                }
                            })
                            .ok();
                        }
                        ErrorCode::MessageTooLong => {
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_status_message("Message is too long.".into());
                                }
                            })
                            .ok();
                        }
                        // While connected these three can only be about a
                        // password change: the temporary password did not
                        // match, the new one is too weak, or some other frame
                        // was refused because the account is still locked.
                        // The link stays up and so does the screen, the way
                        // out is through the same form.
                        ErrorCode::MustChangePassword
                        | ErrorCode::InvalidCredentials
                        | ErrorCode::RegistrationFailed => {
                            let text = code.to_string();
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_status_message(text.into());
                                    ui.set_current_screen(5);
                                }
                            })
                            .ok();
                        }
                        ErrorCode::UnsupportedProtocolVersion { .. }
                        | ErrorCode::MalformedFrame
                        | ErrorCode::Internal => {
                            let text = code.to_string();
                            slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_weak.upgrade() {
                                    ui.set_status_message(text.into());
                                    ui.set_current_screen(0);
                                }
                            })
                            .ok();
                        }
                    }
                }

                ServerMsg::FriendList { entries } => {
                    state_lock.unread.clear();
                    state_lock.friends = entries;
                    sync_friends(&ui_weak, &state_lock);
                }
                ServerMsg::PendingReqs { entries } => {
                    let names: Vec<String> = entries.iter().map(|u| u.login.clone()).collect();
                    state_lock.incoming_reqs = entries;
                    if !names.is_empty() {
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                ui.set_search_result(
                                    format!("Pending friend requests from: {}", names.join(", "))
                                        .into(),
                                );
                            }
                        })
                        .ok();
                    }
                }
                ServerMsg::IncomingReq { from } => {
                    let login = from.login.clone();
                    state_lock.incoming_reqs.push(from);
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_search_result(
                                format!(
                                    "Incoming request from: {}. Search for '{}' to accept!",
                                    login, login
                                )
                                .into(),
                            );
                        }
                    })
                    .ok();
                }
                ServerMsg::FriendAdded { user } => {
                    let chat_id = user.user_id;
                    if !state_lock.friends.iter().any(|f| f.user_id == chat_id) {
                        state_lock.friends.push(user);
                        sync_friends(&ui_weak, &state_lock);
                    }
                }
                ServerMsg::UnreadSummary { entries } => {
                    state_lock.unread = unread_from_summary(entries);
                    sync_friends(&ui_weak, &state_lock);
                }
                ServerMsg::UserFound { user } => {
                    let (chat_id, login) = (user.user_id, user.login.clone());
                    let is_incoming = state_lock
                        .incoming_reqs
                        .iter()
                        .any(|f| f.user_id == chat_id);
                    let is_friend = state_lock.friends.iter().any(|f| f.user_id == chat_id);
                    {
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            if !is_friend {
                                let _ = if is_incoming {
                                    state_lock.incoming_reqs.retain(|f| f.user_id != chat_id);
                                    tx.send(ClientMsg::AcceptFriend {
                                        target_user_id: chat_id,
                                    })
                                } else {
                                    tx.send(ClientMsg::FriendReq {
                                        target_user_id: chat_id,
                                    })
                                };
                            }
                        }
                    }
                    let msg_text = if is_friend {
                        format!("{} is already your friend.", login)
                    } else if is_incoming {
                        format!("Accepted friend request from {}.", login)
                    } else {
                        format!("Sent friend request to {}.", login)
                    };
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_search_result(msg_text.into());
                        }
                    })
                    .ok();
                }
                ServerMsg::DmResolved { conv_id, peer } => {
                    state_lock.conv.insert(peer.user_id, conv_id);
                    if state_lock.active_peer_id == Some(peer.user_id) {
                        state_lock.active_conv = Some(conv_id);
                        sync_messages(&ui_weak, &state_lock);
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(ClientMsg::HistoryReq { conv_id });
                        }
                    }
                }
                ServerMsg::UserNotFound => {
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_search_result("User not found.".into());
                        }
                    })
                    .ok();
                }
                ServerMsg::MsgAck {
                    message_id,
                    conv_id,
                } => {
                    state_lock.pending.remove(&message_id);
                    if let Some(msgs) = state_lock.messages.get_mut(&conv_id) {
                        if let Some(m) = msgs.iter_mut().find(|m| m.id == message_id) {
                            // Below Read, not below Sent: an acknowledgement
                            // that arrives after the retries ran out still
                            // means the server stored the message.
                            if m.status < DeliveryStatus::Read {
                                m.status = DeliveryStatus::Sent;
                            }
                        }
                    }
                    sync_messages(&ui_weak, &state_lock);
                }
                ServerMsg::MsgRead {
                    message_id,
                    conv_id,
                } => {
                    if let Some(msgs) = state_lock.messages.get_mut(&conv_id) {
                        if let Some(m) = msgs.iter_mut().find(|m| m.id == message_id) {
                            if m.status < DeliveryStatus::Read {
                                m.status = DeliveryStatus::Read;
                            }
                        }
                    }
                    sync_messages(&ui_weak, &state_lock);
                }
                ServerMsg::RecvMsg {
                    message_id,
                    conv_id,
                    sender_user_id,
                    timestamp,
                    content,
                } => {
                    let message = ChatMessage {
                        id: message_id,
                        sender_user_id,
                        text: content.clone(),
                        timestamp,
                        outgoing: false,
                        status: DeliveryStatus::Sending,
                    };
                    state_lock
                        .messages
                        .entry(conv_id)
                        .or_default()
                        .push(message);

                    if state_lock.active_conv == Some(conv_id) {
                        push_incoming_message(&ui_weak, &state_lock);
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(ClientMsg::MarkRead { message_id });
                        }
                    } else {
                        *state_lock.unread.entry(sender_user_id).or_insert(0) += 1;
                        sync_friends(&ui_weak, &state_lock);
                    }
                }
                _ => {}
            },
        }
    }
}

fn build_model_friends(state: &ChatState) -> Vec<FriendEntry> {
    state
        .friends
        .iter()
        .map(|f| FriendEntry {
            login: f.login.clone().into(),
            user_id: f.user_id as i32,
            unread: state.unread.get(&f.user_id).copied().unwrap_or(0) as i32,
        })
        .collect()
}

/// Nothing open, or the conversation is not known yet.
fn build_model_active(state: &ChatState) -> Vec<MessageEntry> {
    state
        .active_conv
        .map(|conv_id| build_model_msgs(state, conv_id))
        .unwrap_or_default()
}

/// The dialog itself stays open, it gets reopened, or `ConversationNotFound`
/// arrives and a new `ResolveDm` goes out.
fn forget_conversation(state: &mut ChatState) {
    if let Some(conv_id) = state.active_conv.take() {
        state.messages.remove(&conv_id);
    }
    if let Some(peer) = state.active_peer_id {
        state.conv.remove(&peer);
    }
}

fn unread_message_ids(state: &ChatState) -> Vec<Uuid> {
    state
        .active_conv
        .and_then(|conv_id| state.messages.get(&conv_id))
        .map(|msgs| {
            msgs.iter()
                .filter(|m| !m.outgoing && m.status < DeliveryStatus::Read)
                .map(|m| m.id)
                .collect()
        })
        .unwrap_or_default()
}

/// The only place that writes the friend list model.
/// How long to wait for an acknowledgement before sending again.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// The first send plus this many retries.
const MAX_SEND_ATTEMPTS: u32 = 3;
/// How often the pending sends are looked at.
const RETRY_TICK: Duration = Duration::from_secs(1);

/// A step called retry that never retries would be a lie. Checked by the
/// compiler, there is nothing here to wait for at run time.
const _: () = assert!(MAX_SEND_ATTEMPTS >= 2);

/// What to do with one pending send, seen at `now`.
#[derive(Debug, PartialEq)]
enum RetryAction {
    Wait,
    Resend,
    GiveUp,
}

/// `attempts` counts the sends already made, so the first one is behind us by
/// the time a send reaches this map. Give up only after the deadline of the
/// last allowed attempt has passed.
fn retry_action(attempts: u32, next_attempt_at: Instant, now: Instant) -> RetryAction {
    if now < next_attempt_at {
        RetryAction::Wait
    } else if attempts >= MAX_SEND_ATTEMPTS {
        RetryAction::GiveUp
    } else {
        RetryAction::Resend
    }
}

/// Resends what the server never acknowledged, and gives up on it once the
/// retries run out. A duplicate is harmless: the server keys messages by the
/// id the client generated, so it answers a second copy of the same frame
/// instead of storing it twice.
fn spawn_retry_task(state: Arc<Mutex<ChatState>>, sender_slot: CmdSender, ui: Weak<MainWindow>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(RETRY_TICK);

        let mut state_lock = state.lock().unwrap();
        let now = Instant::now();
        let mut resend = Vec::new();
        let mut given_up = Vec::new();

        for (id, pending) in state_lock.pending.iter_mut() {
            match retry_action(pending.attempts, pending.next_attempt_at, now) {
                RetryAction::Wait => {}
                RetryAction::Resend => {
                    pending.attempts += 1;
                    pending.next_attempt_at = now + SEND_TIMEOUT;
                    resend.push(ClientMsg::SendMsg {
                        message_id: *id,
                        conv_id: pending.conv_id,
                        content: pending.content.clone(),
                    });
                }
                RetryAction::GiveUp => given_up.push((*id, pending.conv_id)),
            }
        }

        for (id, conv_id) in &given_up {
            state_lock.pending.remove(id);
            if let Some(msgs) = state_lock.messages.get_mut(conv_id) {
                if let Some(m) = msgs.iter_mut().find(|m| m.id == *id) {
                    m.status = DeliveryStatus::Failed;
                }
            }
        }

        if !resend.is_empty() {
            let guard = sender_slot.lock().unwrap();
            if let Some(tx) = guard.as_ref() {
                for frame in resend {
                    let _ = tx.send(frame);
                }
            }
        }

        if !given_up.is_empty() {
            sync_messages(&ui, &state_lock);
        }
    });
}

/// Counts arrive per conversation, the friend list keys them by peer. The
/// conversation id is not kept: opening the chat resolves it again.
fn unread_from_summary(entries: Vec<UnreadEntry>) -> HashMap<i64, usize> {
    entries
        .into_iter()
        .map(|entry| (entry.peer.user_id, entry.count as usize))
        .collect()
}

fn sync_friends(ui_weak: &Weak<MainWindow>, state: &ChatState) {
    let entries = build_model_friends(state);
    let ui_weak = ui_weak.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(ui) = ui_weak.upgrade() {
            let model = ui.get_friends_list();
            if let Some(model) = model.as_any().downcast_ref::<VecModel<FriendEntry>>() {
                model.set_vec(entries);
            }
        }
    })
    .ok();
}

/// The only place that writes the message model, shared by `sync_messages` and `sync_messages_now`.
fn write_messages(ui: &MainWindow, peer_i32: i32, entries: Vec<MessageEntry>) {
    if ui.get_active_peer_id() != peer_i32 {
        return;
    }
    let model = ui.get_active_chat_messages();
    if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>() {
        model.set_vec(entries);
    }
}

/// For events from the network task. The model updates once the queue reaches the UI thread.
fn sync_messages(ui_weak: &Weak<MainWindow>, state: &ChatState) {
    push_messages(ui_weak, state, false);
}

/// For a history batch. The newest message is what a reader opening a chat
/// came for, so the view goes there even if they had scrolled up before.
fn sync_messages_at_bottom(ui_weak: &Weak<MainWindow>, state: &ChatState) {
    push_messages(ui_weak, state, true);
}

fn push_messages(ui_weak: &Weak<MainWindow>, state: &ChatState, follow_newest: bool) {
    let entries = build_model_active(state);
    let peer_i32 = state.active_peer_id.unwrap_or(-1) as i32;
    let ui_weak = ui_weak.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(ui) = ui_weak.upgrade() {
            if follow_newest {
                end_at_newest(&ui);
            }
            write_messages(&ui, peer_i32, entries);
        }
    })
    .ok();
}

/// For a message that has just arrived. The view is left where it is when
/// the reader is up in the history, and the message is counted instead.
fn push_incoming_message(ui_weak: &Weak<MainWindow>, state: &ChatState) {
    let entries = build_model_active(state);
    let peer_i32 = state.active_peer_id.unwrap_or(-1) as i32;
    let ui_weak = ui_weak.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(ui) = ui_weak.upgrade() {
            // Read before the model grows: after it, the same geometry calls
            // a reader who was at the bottom scrolled up.
            let missed = next_missed_count(ui.get_missed_count(), ui.get_chat_at_bottom());
            ui.set_missed_count(missed);
            write_messages(&ui, peer_i32, entries);
        }
    })
    .ok();
}

/// Puts the list at the newest message. Used where the reader must end up
/// there whatever they were reading: sending, opening a chat, a history batch.
/// Asked for again a moment later. The list lays its rows out after the
/// model changes, so the height it reports at the moment of the request is
/// not the height it settles on, and one pin lands short. These are long
/// enough for the layout to finish and short enough that the reader does not
/// see the list walk down.
const PIN_AGAIN_AFTER: [u64; 3] = [50, 150, 400];

fn end_at_newest(ui: &MainWindow) {
    ui.set_pin_requests(ui.get_pin_requests() + 1);

    for ms in PIN_AGAIN_AFTER {
        let weak = ui.as_weak();
        slint::Timer::single_shot(Duration::from_millis(ms), move || {
            if let Some(ui) = weak.upgrade() {
                // The reader may have scrolled away in the meantime, and then
                // the newest message is no longer what they asked to see.
                if ui.get_chat_at_bottom() {
                    ui.set_pin_requests(ui.get_pin_requests() + 1);
                }
            }
        });
    }
}

/// A message counted only while the reader is away from the bottom. Reaching
/// the bottom clears whatever piled up, which the list also does on its own.
fn next_missed_count(missed: i32, at_bottom: bool) -> i32 {
    if at_bottom {
        0
    } else {
        missed + 1
    }
}

/// For handlers called from the UI thread. They need the model current by
/// the time they return, or the caller cannot set `chat-at-bottom` before
/// the write that would raise `changed content-height`.
fn sync_messages_now(ui: &MainWindow, state: &ChatState) {
    write_messages(
        ui,
        state.active_peer_id.unwrap_or(-1) as i32,
        build_model_active(state),
    );
}

fn build_model_msgs(state: &ChatState, conv_id: Uuid) -> Vec<MessageEntry> {
    state
        .messages
        .get(&conv_id)
        .map(|msgs| {
            msgs.iter()
                .map(|m| MessageEntry {
                    text: m.text.clone().into(),
                    time: format_time(m.timestamp),
                    is_outgoing: m.outgoing,
                    status: m.status as i32,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The stamp is an instant, the reader sees their own wall clock. Kept
/// separate from the system zone so the conversion can be tested with a
/// fixed one.
fn format_time_in(timestamp: i64, offset: FixedOffset) -> String {
    DateTime::from_timestamp_secs(timestamp)
        .map(|dt| dt.with_timezone(&offset).format("%H:%M").to_string())
        .unwrap_or_default()
}

fn format_time(timestamp: i64) -> SharedString {
    format_time_in(timestamp, *Local::now().offset()).into()
}

/// Distance from the newest message, in the list's own coordinates, where
/// `content-y` is negative and reaches `height - content-height` at the
/// bottom. Zero means the newest message is on screen.
pub(crate) fn distance_to_bottom(content_height: f32, view_height: f32, content_y: f32) -> f32 {
    (content_height - view_height + content_y).max(0.0)
}

/// Slack for a list that sits a rounding error away from the bottom.
const BOTTOM_SLACK: f32 = 4.0;

pub(crate) fn is_at_bottom(content_height: f32, view_height: f32, content_y: f32) -> bool {
    distance_to_bottom(content_height, view_height, content_y) <= BOTTOM_SLACK
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, FixedOffset};
    use std::time::{Duration, Instant};
    use uuid::Uuid;
    use zeevum_protocol::{UnreadEntry, UserBrief};

    /// The worry this covers: a sender two hours ahead must not make the
    /// reader see the sender's clock. The stamp is an instant, so the same
    /// moment renders differently for every reader.
    #[test]
    fn the_same_instant_follows_the_readers_zone() {
        let noon_utc = DateTime::parse_from_rfc3339("2026-09-22T12:00:00Z")
            .unwrap()
            .timestamp();

        let zone = |hours: i32| FixedOffset::east_opt(hours * 3600).unwrap();

        assert_eq!(super::format_time_in(noon_utc, zone(0)), "12:00");
        assert_eq!(super::format_time_in(noon_utc, zone(2)), "14:00");
        assert_eq!(super::format_time_in(noon_utc, zone(-5)), "07:00");
    }

    /// Catches the zone being hardcoded instead of taken from the system.
    /// Only bites on a machine whose zone is not UTC, which is the point.
    #[test]
    fn format_time_uses_the_system_zone() {
        let noon_utc = DateTime::parse_from_rfc3339("2026-09-22T12:00:00Z")
            .unwrap()
            .timestamp();
        let expected = DateTime::from_timestamp_secs(noon_utc)
            .unwrap()
            .with_timezone(&chrono::Local)
            .format("%H:%M")
            .to_string();
        assert_eq!(super::format_time(noon_utc), expected);
    }

    /// An hour later is an hour later, whatever the zone does to the clock.
    #[test]
    fn an_hour_later_shows_an_hour_later() {
        let noon_utc = DateTime::parse_from_rfc3339("2026-09-22T12:00:00Z")
            .unwrap()
            .timestamp();
        let zone = FixedOffset::east_opt(2 * 3600).unwrap();

        assert_eq!(super::format_time_in(noon_utc, zone), "14:00");
        assert_eq!(super::format_time_in(noon_utc + 3600, zone), "15:00");
    }

    /// The coordinate the list uses. `content-y` is negative and reaches
    /// `height - content-height` at the bottom, so a list of 1200 in a view
    /// of 400 sits at the newest message when it is -800.
    #[test]
    fn the_bottom_of_a_long_list_is_the_newest_message() {
        assert_eq!(super::distance_to_bottom(1200.0, 400.0, -800.0), 0.0);
        assert!(super::is_at_bottom(1200.0, 400.0, -800.0));
    }

    /// The bug this covers. A reader who has scrolled up is not at the
    /// bottom, so a message arriving while they read must not move the view.
    #[test]
    fn a_reader_scrolled_up_is_not_at_the_bottom() {
        assert_eq!(super::distance_to_bottom(1200.0, 400.0, -750.0), 50.0);
        assert!(!super::is_at_bottom(1200.0, 400.0, -750.0));
        assert!(!super::is_at_bottom(1200.0, 400.0, 0.0));
    }

    /// A few pixels off the bottom is still the bottom, ten is not. Past
    /// the bottom the list cannot go at all, so that clamps to zero too.
    #[test]
    fn the_slack_absorbs_a_rounding_error() {
        assert_eq!(super::distance_to_bottom(1200.0, 400.0, -797.0), 3.0);
        assert!(super::is_at_bottom(1200.0, 400.0, -797.0));
        assert!(!super::is_at_bottom(1200.0, 400.0, -790.0));
        assert!(super::is_at_bottom(1200.0, 400.0, -810.0));
    }

    /// A short list has nowhere to scroll, so it is always at the newest.
    #[test]
    fn a_list_shorter_than_the_view_is_at_the_bottom() {
        assert!(super::is_at_bottom(200.0, 400.0, 0.0));
        assert!(super::is_at_bottom(0.0, 0.0, 0.0));
    }

    /// Three quick retries, three patient ones, then one every half a minute
    /// for as long as it takes.
    #[test]
    fn the_wait_grows_in_three_steps() {
        assert_eq!(super::next_delay(0), Duration::from_secs(3));
        assert_eq!(super::next_delay(2), Duration::from_secs(3));
        assert_eq!(super::next_delay(3), Duration::from_secs(10));
        assert_eq!(super::next_delay(5), Duration::from_secs(10));
        assert_eq!(super::next_delay(6), Duration::from_secs(30));
        assert_eq!(super::next_delay(500), Duration::from_secs(30));
    }

    /// The button is offered once waiting is long enough to notice.
    #[test]
    fn the_button_appears_after_the_quick_retries() {
        assert!(!super::is_slow(0));
        assert!(!super::is_slow(3));
        assert!(super::is_slow(4));
    }

    /// What the corner shows. A reader who has given up sees nothing, and
    /// one who is connected sees the tick whatever the attempt count says.
    #[test]
    fn the_corner_shows_the_state_of_the_link() {
        use super::Link;
        let online = Link {
            wanted: true,
            connected: true,
            attempts: 7,
            ..Link::default()
        };
        assert_eq!(super::link_status(&online), 3);

        assert_eq!(super::link_status(&Link::default()), 0);

        let quick = Link {
            wanted: true,
            attempts: 2,
            ..Link::default()
        };
        assert_eq!(super::link_status(&quick), 1);

        let waiting = Link {
            wanted: true,
            attempts: 4,
            ..Link::default()
        };
        assert_eq!(super::link_status(&waiting), 2);
    }

    /// The deadline has not passed, so nothing happens yet.
    #[test]
    fn a_send_still_within_its_deadline_waits() {
        let now = Instant::now();
        assert_eq!(
            super::retry_action(1, now + Duration::from_secs(10), now),
            super::RetryAction::Wait
        );
    }

    #[test]
    fn a_send_past_its_deadline_is_sent_again() {
        let now = Instant::now();
        assert_eq!(
            super::retry_action(1, now - Duration::from_secs(1), now),
            super::RetryAction::Resend
        );
    }

    /// The last allowed attempt has already been made and has timed out.
    #[test]
    fn a_send_past_the_last_attempt_is_given_up_on() {
        let now = Instant::now();
        let past = now - Duration::from_secs(1);
        assert_eq!(
            super::retry_action(super::MAX_SEND_ATTEMPTS - 1, past, now),
            super::RetryAction::Resend
        );
        assert_eq!(
            super::retry_action(super::MAX_SEND_ATTEMPTS, past, now),
            super::RetryAction::GiveUp
        );
    }

    /// The friend list shows a badge per friend, and the summary arrives
    /// keyed by conversation.
    #[test]
    fn unread_counts_are_keyed_by_peer() {
        let entries = vec![
            UnreadEntry {
                conv_id: Uuid::new_v4(),
                peer: UserBrief {
                    user_id: 7,
                    login: "alice".into(),
                },
                count: 3,
            },
            UnreadEntry {
                conv_id: Uuid::new_v4(),
                peer: UserBrief {
                    user_id: 9,
                    login: "bob".into(),
                },
                count: 1,
            },
        ];

        let counts = super::unread_from_summary(entries);
        assert_eq!(counts.len(), 2);
        assert_eq!(counts.get(&7), Some(&3));
        assert_eq!(counts.get(&9), Some(&1));
    }

    /// A message is counted only while the reader is away from the bottom.
    /// Coming back clears the count.
    #[test]
    fn only_messages_away_from_the_bottom_are_counted() {
        assert_eq!(super::next_missed_count(0, true), 0);
        assert_eq!(super::next_missed_count(0, false), 1);
        assert_eq!(super::next_missed_count(2, false), 3);
        assert_eq!(super::next_missed_count(2, true), 0);
    }
}
