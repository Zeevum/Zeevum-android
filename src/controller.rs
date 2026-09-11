use slint::{Model, SharedString, VecModel, Weak};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::network::network_task;
use crate::settings;
use crate::types::{ChatMessage, CmdSender, DeliveryStatus, UiEvent};
use crate::{FriendEntry, MainWindow, MessageEntry};

use zeevum_protocol::{ClientMsg, ErrorCode, ServerMsg, UserBrief, MAX_MESSAGE_LEN};

#[derive(Clone)]
pub struct AppController {
    pub ui: Weak<MainWindow>,
    pub sender_slot: CmdSender,
    state: Arc<Mutex<ChatState>>,
}

pub struct ChatState {
    pub my_user_id: i64,
    /// Собеседник открытого диалога. `None` - ничего не открыто. UI адресует
    /// диалог по человеку (список друзей даёт `user_id`), поэтому это ровно
    /// то, что лежит в свойстве `MainWindow.active-peer-id`.
    pub active_peer_id: Option<i64>,
    /// Логин собеседника открытого диалога. Нужен затем же, зачем
    /// `active_peer_id`, чтобы на `NotFriends` можно было предложить
    /// отправить заявку, назвав человека по имени.
    pub active_peer_login: String,
    /// Разговор открытого диалога. `None`, пока сервер не ответил на
    /// `ResolveDm`. Путать с `active_peer_id` нельзя, человек и разговор -
    /// разные вещи, в групповых чатах это станет видно окончательно.
    pub active_conv: Option<Uuid>,
    /// человек → разговор. Кэш, чтобы не спрашивать сервер каждый раз,
    /// когда открываем уже открывавшийся диалог.
    pub conv: HashMap<i64, Uuid>,
    pub server_addr: String,
    pub login: String,
    pub friends: Vec<UserBrief>,
    /// Сообщения по разговорам, а не по собеседникам.
    pub messages: HashMap<Uuid, Vec<ChatMessage>>,
    pub incoming_reqs: Vec<UserBrief>,
    /// Непрочитанные входящие по собеседнику.
    ///
    /// Живёт в состоянии, а не в UI-модели: модель списка друзей целиком
    /// пересобирается из состояния (`sync_friends`), поэтому счётчик, который
    /// хранится в виджете, терялся бы при каждой пересборке.
    pub unread: HashMap<i64, usize>,
}

impl AppController {
    pub fn new(ui: Weak<MainWindow>) -> Self {
        Self {
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
            existing.user_id.unwrap_or(0),
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
                    outgoing: true,
                    status: DeliveryStatus::Sending,
                });

            sync_messages_now(&ui, &state_lock);
            request_scroll(&self.ui);

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
        }
        let state_lock = self.state.lock().unwrap();
        if let Some(ui) = self.ui.upgrade() {
            ui.set_current_screen(0);
            ui.set_active_peer_id(-1);
            ui.set_active_peer_login("".into());
            sync_messages_now(&ui, &state_lock);
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
                state_lock.active_peer_id = None;
                state_lock.active_peer_login.clear();
                state_lock.active_conv = None;
                state_lock.conv.clear();
                state_lock.my_user_id = 0;
                state_lock.messages.clear();
                state_lock.unread.clear();
                let weak = ui_weak.clone();
                slint::invoke_from_event_loop(move || {
                    if let Some(ui) = weak.upgrade() {
                        ui.set_status_message(msg.into());
                        ui.set_current_screen(0);
                        ui.set_active_peer_id(-1);
                        ui.set_active_peer_login("".into());
                    }
                })
                    .ok();
                sync_messages(&ui_weak, &state_lock);
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
                        outgoing: is_out,
                        status,
                    });
                }
                state_lock.messages.insert(conv_id, stored);
                sync_messages(&ui_weak, &state_lock);
                request_scroll(&self.ui);

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
                } => {
                    state_lock.my_user_id = user_id;
                    settings::save_session(
                        &state_lock.server_addr,
                        &state_lock.login,
                        &token,
                        user_id,
                        expires_at,
                    );
                    slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_current_screen(1);
                        }
                    })
                        .ok();
                }
                ServerMsg::Error {
                    code,
                    detail: _detail,
                } => match code {
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
                    ErrorCode::UnsupportedProtocolVersion { .. }
                    | ErrorCode::InvalidCredentials
                    | ErrorCode::RegistrationFailed
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
                },
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
                    if let Some(msgs) = state_lock.messages.get_mut(&conv_id) {
                        if let Some(m) = msgs.iter_mut().find(|m| m.id == message_id) {
                            if m.status < DeliveryStatus::Sent {
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
                    timestamp: _ts,
                    content,
                } => {
                    let message = ChatMessage {
                        id: message_id,
                        sender_user_id,
                        text: content.clone(),
                        outgoing: false,
                        status: DeliveryStatus::Sending,
                    };
                    state_lock
                        .messages
                        .entry(conv_id)
                        .or_default()
                        .push(message);

                    if state_lock.active_conv == Some(conv_id) {
                        sync_messages(&ui_weak, &state_lock);
                        request_scroll(&self.ui);
                        let guard = sender_slot.lock().unwrap();
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(ClientMsg::MarkRead { message_id });
                        }
                    } else {
                        // Чат не открыт — считаем непрочитанное в состоянии.
                        *state_lock.unread.entry(sender_user_id).or_insert(0) += 1;
                        sync_friends(&ui_weak, &state_lock);
                    }
                }
                _ => {}
            },
        }
    }
}

/// Модель списка друзей, целиком построенная из состояния.
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

/// Модель сообщений открытого диалога. Диалог не открыт или разговор ещё
/// не известен
fn build_model_active(state: &ChatState) -> Vec<MessageEntry> {
    state
        .active_conv
        .map(|conv_id| build_model_msgs(state, conv_id))
        .unwrap_or_default()
}

/// Выбрасывает разговор открытого диалога, он больше недействителен.
/// Сам диалог остаётся открытым - откроют заново или придёт
/// `ConversationNotFound`, уйдёт новый `ResolveDm`.
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

/// Единственная точка записи в модель списка друзей
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

/// Единственная точка записи в модель сообщений, общая для `sync_messages` и `sync_messages_now`
fn write_messages(ui: &MainWindow, peer_i32: i32, entries: Vec<MessageEntry>) {
    if ui.get_active_peer_id() != peer_i32 {
        return;
    }
    let model = ui.get_active_chat_messages();
    if let Some(model) = model.as_any().downcast_ref::<VecModel<MessageEntry>>() {
        model.set_vec(entries);
    }
}

/// Для событий из сетевого таска: модель обновится, когда очередь дойдёт до UI-потока
fn sync_messages(ui_weak: &Weak<MainWindow>, state: &ChatState) {
    let entries = build_model_active(state);
    let peer_i32 = state.active_peer_id.unwrap_or(-1) as i32;
    let ui_weak = ui_weak.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(ui) = ui_weak.upgrade() {
            write_messages(&ui, peer_i32, entries);
        }
    })
        .ok();
}

/// Для обработчиков, вызванных из UI-потока, им модель нужна уже актуальной
/// к моменту возврата - иначе `request_scroll` прокрутит список, в котором
/// нового сообщения ещё нет
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
