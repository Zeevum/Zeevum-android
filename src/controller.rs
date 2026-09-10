use slint::{Model, SharedString, VecModel, Weak};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::network::network_task;
use crate::settings;
use crate::types::{ChatMessage, CmdSender, DeliveryStatus, UiEvent};
use crate::{FriendEntry, MainWindow, MessageEntry};

use Zeevum_protocol::{ClientMsg, ServerMsg};

#[derive(Clone)]
pub struct AppController {
    pub ui: Weak<MainWindow>,
    pub sender_slot: CmdSender,
    state: Arc<Mutex<ChatState>>,
}

pub struct ChatState {
    pub my_chat_id: i64,
    pub active_chat_id: i64,
    pub server_addr: String,
    pub login: String,
    pub friends: Vec<(i64, String)>,
    pub messages: HashMap<i64, Vec<ChatMessage>>,
    pub incoming_reqs: Vec<(i64, String)>,
}

impl AppController {
    pub fn new(ui: Weak<MainWindow>) -> Self {
        Self {
            ui,
            sender_slot: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(ChatState {
                my_chat_id: 0,
                active_chat_id: -1,
                server_addr: String::new(),
                login: String::new(),
                friends: Vec::new(),
                messages: HashMap::new(),
                incoming_reqs: Vec::new(),
            })),
        }
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

        let controller_clone = self.clone();
        let sender_slot = self.sender_slot.clone();

        let existing = settings::load_settings();
        settings::save_session(
            &addr_str,
            &login_str,
            existing.token.as_deref().unwrap_or(""),
            existing.chat_id.unwrap_or(0),
            existing.expires_at.unwrap_or(0),
        );

        if let Some(ui) = self.ui.upgrade() {
            ui.set_status_message("Connecting...".into());
        }

        {
            let mut guard = sender_slot.lock().unwrap();
            if guard.is_some() {
                return;
            }

            let (tx_cmd, rx_cmd) = tokio::sync::mpsc::unbounded_channel();
            *guard = Some(tx_cmd);

            tokio::spawn(async move {
                network_task(
                    controller_clone,
                    addr_str,
                    login_str,
                    pass_str,
                    is_register,
                    rx_cmd,
                )
                .await;
            });
        }
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
        let mut state_lock = self.state.lock().unwrap();

        if let Some(ui) = self.ui.upgrade() {
            let active_chat = ui.get_active_chat_id() as i64;
            if active_chat != -1 {
                let msg_uuid = Uuid::new_v4();

                let my_id = state_lock.my_chat_id;
                state_lock
                    .messages
                    .entry(active_chat)
                    .or_default()
                    .push(ChatMessage {
                        id: msg_uuid,
                        sender_chat_id: my_id,
                        text: text_str.clone(),
                        outgoing: true,
                        status: DeliveryStatus::Sending,
                    });

                request_scroll(&self.ui);

                let model = ui.get_active_chat_messages();
                if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>() {
                    model.push(MessageEntry {
                        text: text_str.clone().into(),
                        is_outgoing: true,
                        status: 0,
                    });
                }

                let guard = self.sender_slot.lock().unwrap();
                if let Some(tx) = guard.as_ref() {
                    let _ = tx.send(ClientMsg::SendMsg {
                        message_id: msg_uuid,
                        peer_chat_id: active_chat,
                        content: text_str,
                    });
                }
            }
        }
    }

    pub fn handle_open_chat(&self, chat_id: i32, login: SharedString) {
        let mut state_lock = self.state.lock().unwrap();
        let chat_id_i64 = chat_id as i64;
        let ui_weak = self.ui.clone();

        state_lock.active_chat_id = chat_id_i64;

        if let Some(ui) = self.ui.upgrade() {
            ui.set_active_chat_id(chat_id);
            ui.set_active_chat_login(login);

            let model = ui.get_active_chat_messages();
            if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>() {
                model.set_vec(Vec::new());

                if let Some(msgs) = state_lock.messages.get(&chat_id_i64) {
                    let mut unread_ids = Vec::new();
                    for msg in msgs {
                        model.push(MessageEntry {
                            text: msg.text.clone().into(),
                            is_outgoing: msg.outgoing,
                            status: msg.status as i32,
                        });
                        if !msg.outgoing && msg.status < DeliveryStatus::Read {
                            unread_ids.push(msg.id);
                        }
                    }
                    let guard = self.sender_slot.lock().unwrap();
                    if let Some(tx) = guard.as_ref() {
                        for id in unread_ids {
                            let _ = tx.send(ClientMsg::MarkRead { message_id: id });
                        }
                    }
                }

                let guard = self.sender_slot.lock().unwrap();
                if let Some(tx) = guard.as_ref() {
                    let _ = tx.send(ClientMsg::HistoryReq {
                        peer_chat_id: chat_id_i64,
                    });
                }
            }

            if let Some(idx) = state_lock
                .friends
                .iter()
                .position(|(id, _)| *id == chat_id_i64)
            {
                slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak.upgrade() {
                        let model = ui.get_friends_list();
                        if let Some(model) = model.as_any().downcast_ref::<VecModel<FriendEntry>>()
                        {
                            if let Some(mut entry) = model.row_data(idx) {
                                entry.unread = 0;
                                model.set_row_data(idx, entry);
                            }
                        }
                    }
                })
                .ok();
            }
        }
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
        settings::clear_session();
        {
            let mut state_lock = self.state.lock().unwrap();
            state_lock.active_chat_id = -1;
            state_lock.my_chat_id = 0;
            state_lock.messages.clear();
        }
        if let Some(ui) = self.ui.upgrade() {
            ui.set_current_screen(0);
            ui.set_active_chat_id(-1);
            ui.set_active_chat_login("".into());
            let model = ui.get_active_chat_messages();
            if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>() {
                model.set_vec(Vec::new());
            }
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
            settings.chat_id.unwrap_or(0),
            settings.expires_at.unwrap_or(0),
        );
    }

    pub fn handle_ui_event(&self, event: UiEvent) {
        let mut state_lock = self.state.lock().unwrap();
        let sender_slot = self.sender_slot.clone();
        let ui_weak = self.ui.clone();

        match event {
            UiEvent::Disconnected(msg) => {
                state_lock.active_chat_id = -1;
                state_lock.my_chat_id = 0;
                state_lock.messages.clear();
                slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak.upgrade() {
                        ui.set_status_message(msg.into());
                        ui.set_current_screen(0);
                        ui.set_active_chat_id(-1);
                        ui.set_active_chat_login("".into());
                        let model = ui.get_active_chat_messages();
                        if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>()
                        {
                            model.set_vec(Vec::new());
                        }
                    }
                })
                .ok();
            }
            UiEvent::HistoryBatch {
                peer_chat_id,
                entries,
            } => {
                let mut msgs = Vec::new();
                let mut stored = Vec::new();
                for e in entries {
                    let is_out = e.sender_chat_id == state_lock.my_chat_id;
                    let status = match (is_out, e.is_read) {
                        (_, true) => DeliveryStatus::Read,
                        (true, false) => DeliveryStatus::Sent,
                        (false, false) => DeliveryStatus::Sending,
                    };
                    msgs.push(MessageEntry {
                        text: e.content.clone().into(),
                        is_outgoing: is_out,
                        status: status as i32,
                    });
                    stored.push(ChatMessage {
                        id: e.message_id,
                        sender_chat_id: e.sender_chat_id,
                        text: e.content,
                        outgoing: is_out,
                        status,
                    });
                }
                state_lock.messages.insert(peer_chat_id, stored);
                let chat_id_i32 = peer_chat_id as i32;
                request_scroll(&self.ui);
                slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak.upgrade() {
                        if ui.get_active_chat_id() == chat_id_i32 {
                            let model = ui.get_active_chat_messages();
                            if let Some(model) =
                                model.as_any().downcast_ref::<VecModel<MessageEntry>>()
                            {
                                model.set_vec(msgs);
                            }
                        }
                    }
                })
                .ok();
            }
            UiEvent::Server(msg) => match msg {
                ServerMsg::AuthOk {
                    chat_id,
                    token,
                    expires_at,
                } => {
                    state_lock.my_chat_id = chat_id;
                    settings::save_session(
                        &state_lock.server_addr,
                        &state_lock.login,
                        &token,
                        chat_id,
                        expires_at,
                    );
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_current_screen(1);
                        }
                    })
                    .ok();
                }
                ServerMsg::AuthFailed { reason } => {
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_status_message(reason.into());
                            ui.set_current_screen(0);
                        }
                    })
                    .ok();
                }
                ServerMsg::FriendList { entries } => {
                    state_lock.friends = entries
                        .iter()
                        .map(|u| (u.chat_id, u.login.clone()))
                        .collect();
                    let list = entries.clone();
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            let model = ui.get_friends_list();
                            if let Some(model) =
                                model.as_any().downcast_ref::<VecModel<FriendEntry>>()
                            {
                                model.set_vec(
                                    list.iter()
                                        .map(|u| FriendEntry {
                                            login: u.login.clone().into(),
                                            chat_id: u.chat_id as i32,
                                            unread: 0,
                                        })
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                    })
                    .ok();
                }
                ServerMsg::PendingReqs { entries } => {
                    state_lock.incoming_reqs = entries
                        .iter()
                        .map(|u| (u.chat_id, u.login.clone()))
                        .collect();
                    let names: Vec<String> = entries.iter().map(|u| u.login.clone()).collect();
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
                    let (chat_id, login) = (from.chat_id, from.login.clone());
                    state_lock.incoming_reqs.push((chat_id, login.clone()));
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
                    let (chat_id, login) = (user.chat_id, user.login.clone());
                    if !state_lock.friends.iter().any(|(id, _)| *id == chat_id) {
                        state_lock.friends.push((chat_id, login.clone()));
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                let model = ui.get_friends_list();
                                if let Some(model) =
                                    model.as_any().downcast_ref::<VecModel<FriendEntry>>()
                                {
                                    model.push(FriendEntry {
                                        login: login.into(),
                                        chat_id: chat_id as i32,
                                        unread: 0,
                                    });
                                }
                            }
                        })
                        .ok();
                    }
                }
                ServerMsg::UserFound { user } => {
                    let (chat_id, login) = (user.chat_id, user.login.clone());
                    let is_incoming = state_lock
                        .incoming_reqs
                        .iter()
                        .any(|(id, _)| *id == chat_id);
                    let is_friend = state_lock.friends.iter().any(|(id, _)| *id == chat_id);
                    {
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            if !is_friend {
                                let _ = if is_incoming {
                                    state_lock.incoming_reqs.retain(|(id, _)| *id != chat_id);
                                    tx.send(ClientMsg::AcceptFriend {
                                        target_chat_id: chat_id,
                                    })
                                } else {
                                    tx.send(ClientMsg::FriendReq {
                                        target_chat_id: chat_id,
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
                ServerMsg::UserNotFound => {
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_search_result("User not found.".into());
                        }
                    })
                    .ok();
                }
                ServerMsg::Info { text } => {
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_search_result(text.into());
                        }
                    })
                    .ok();
                }
                ServerMsg::MsgAck { message_id } => {
                    'scan: for msgs in state_lock.messages.values_mut() {
                        for m in msgs.iter_mut() {
                            if m.id == message_id {
                                if m.status < DeliveryStatus::Sent {
                                    m.status = DeliveryStatus::Sent;
                                }
                                break 'scan;
                            }
                        }
                    }
                    let active = state_lock.active_chat_id;
                    if active != -1 {
                        let model_msgs = build_model_msgs(&state_lock, active);
                        let chat_id_i32 = active as i32;
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                if ui.get_active_chat_id() == chat_id_i32 {
                                    let model = ui.get_active_chat_messages();
                                    if let Some(model) =
                                        model.as_any().downcast_ref::<VecModel<MessageEntry>>()
                                    {
                                        model.set_vec(model_msgs);
                                    }
                                }
                            }
                        })
                        .ok();
                    }
                }
                ServerMsg::MsgRead { message_id } => {
                    'scan: for msgs in state_lock.messages.values_mut() {
                        for m in msgs.iter_mut() {
                            if m.id == message_id {
                                if m.status < DeliveryStatus::Read {
                                    m.status = DeliveryStatus::Read;
                                }
                                break 'scan;
                            }
                        }
                    }
                    let active = state_lock.active_chat_id;
                    if active != -1 {
                        let model_msgs = build_model_msgs(&state_lock, active);
                        let chat_id_i32 = active as i32;
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                if ui.get_active_chat_id() == chat_id_i32 {
                                    let model = ui.get_active_chat_messages();
                                    if let Some(model) =
                                        model.as_any().downcast_ref::<VecModel<MessageEntry>>()
                                    {
                                        model.set_vec(model_msgs);
                                    }
                                }
                            }
                        })
                        .ok();
                    }
                }
                ServerMsg::RecvMsg {
                    message_id,
                    chat_id: _proto_chat,
                    sender_chat_id,
                    timestamp: _ts,
                    content,
                } => {
                    let message = ChatMessage {
                        id: message_id,
                        sender_chat_id,
                        text: content.clone(),
                        outgoing: false,
                        status: DeliveryStatus::Sending,
                    };
                    state_lock
                        .messages
                        .entry(sender_chat_id)
                        .or_default()
                        .push(message);

                    if state_lock.active_chat_id == sender_chat_id {
                        let text_clone = content.clone();
                        request_scroll(&self.ui);
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                let model = ui.get_active_chat_messages();
                                if let Some(model) =
                                    model.as_any().downcast_ref::<VecModel<MessageEntry>>()
                                {
                                    model.push(MessageEntry {
                                        text: text_clone.into(),
                                        is_outgoing: false,
                                        status: 0,
                                    });
                                }
                            }
                        })
                        .ok();
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(ClientMsg::MarkRead { message_id });
                        }
                    } else {
                        let chat_id_i32 = sender_chat_id as i32;
                        slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                let model = ui.get_friends_list();
                                if let Some(model) =
                                    model.as_any().downcast_ref::<VecModel<FriendEntry>>()
                                {
                                    for i in 0..model.row_count() {
                                        if let Some(mut entry) = model.row_data(i) {
                                            if entry.chat_id == chat_id_i32 {
                                                entry.unread += 1;
                                                model.set_row_data(i, entry);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        })
                        .ok();
                    }
                }
                _ => {}
            },
        }
    }
}

fn build_model_msgs(state: &ChatState, chat_id: i64) -> Vec<MessageEntry> {
    state
        .messages
        .get(&chat_id)
        .map(|msgs| {
            msgs.iter()
                .map(|m| MessageEntry {
                    text: m.text.clone().into(),
                    is_outgoing: m.outgoing,
                    status: m.status as i32,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn request_scroll(ui_weak: &slint::Weak<MainWindow>) {
    let weak = ui_weak.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_scroll_pending(true);
        }
    });
    let weak = ui_weak.clone();
    slint::Timer::single_shot(std::time::Duration::from_millis(300), move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_scroll_pending(false);
        }
    });
}
