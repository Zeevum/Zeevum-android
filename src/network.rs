use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use rustls::ClientConfig;
use rustls_native_certs::load_native_certs;
use rustls_pki_types::ServerName;

use Zeevum_protocol::{
    AuthMethod, ClientMsg, ServerMsg, MAX_LINE_BYTES, PROTOCOL_VERSION, decode, encode, pow,
};

use crate::controller::AppController;
use crate::types::{HistoryEntry, UiEvent};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

pub async fn network_task(
    controller: AppController,
    server_addr: String,
    login: String,
    password: String,
    is_register: bool,
    mut cmd_rx: mpsc::UnboundedReceiver<ClientMsg>,
) {
    let sender_slot = controller.sender_slot.clone();

    let _task_logic = async {
        let is_auto_login = password.is_empty() && !is_register;

        let domain = match server_addr.split(':').next() {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
                controller.handle_ui_event(UiEvent::Disconnected("Invalid server address".into()));
                return;
            }
        };

        let mut roots = rustls::RootCertStore::empty();
        let mut added = 0usize;
        for cert in load_native_certs().certs {
            if roots.add(cert).is_ok() {
                added += 1;
            }
        }
        if added == 0 {
            controller.handle_ui_event(UiEvent::Disconnected("No trusted root certificates found".into()));
            return;
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));

        let tcp_stream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&server_addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                controller.handle_ui_event(UiEvent::Disconnected(format!("TCP failed: {e}")));
                return;
            }
            Err(_) => {
                controller.handle_ui_event(UiEvent::Disconnected("TCP timeout".into()));
                return;
            }
        };

        let server_name = match ServerName::try_from(domain.clone()) {
            Ok(n) => n,
            Err(e) => {
                controller.handle_ui_event(UiEvent::Disconnected(format!("Invalid domain: {e}")));
                return;
            }
        };

        let tls_stream = match timeout(CONNECT_TIMEOUT, connector.connect(server_name, tcp_stream)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                controller.handle_ui_event(UiEvent::Disconnected(format!("TLS failed: {e}")));
                return;
            }
            Err(_) => {
                controller.handle_ui_event(UiEvent::Disconnected("TLS timeout".into()));
                return;
            }
        };

        let (tls_reader, mut tls_writer) = split(tls_stream);
        let mut reader = BufReader::new(tls_reader);

        let method = if is_auto_login {
            match crate::settings::load_settings().token.filter(|t| !t.trim().is_empty()) {
                Some(token) => AuthMethod::Token { token },
                None => {
                    controller.handle_ui_event(UiEvent::Disconnected("No token for auto-login".into()));
                    return;
                }
            }
        } else if is_register {
            AuthMethod::Register { login: login.clone(), password: password.clone() }
        } else {
            AuthMethod::Login { login: login.clone(), password: password.clone() }
        };

        let auth_frame = encode(&ClientMsg::Auth {
            protocol_version: PROTOCOL_VERSION,
            method,
        })
            .expect("ClientMsg serialization cannot fail");

        if let Err(e) = write_frame(&mut tls_writer, &auth_frame).await {
            controller.handle_ui_event(UiEvent::Disconnected(format!("Write error: {e}")));
            return;
        }

        loop {
            let line = match timeout(HANDSHAKE_TIMEOUT, read_frame(&mut reader)).await {
                Ok(Ok(l)) => l,
                Ok(Err(e)) => {
                    controller.handle_ui_event(UiEvent::Disconnected(format!("Read error: {e}")));
                    return;
                }
                Err(_) => {
                    controller.handle_ui_event(UiEvent::Disconnected("Handshake timeout".into()));
                    return;
                }
            };
            if !line.ends_with('\n') {
                controller.handle_ui_event(UiEvent::Disconnected("Connection closed".into()));
                return;
            }

            let msg: ServerMsg = match decode(line.trim()) {
                Ok(m) => m,
                Err(_) => {
                    controller.handle_ui_event(UiEvent::Disconnected("Malformed handshake frame".into()));
                    return;
                }
            };

            match msg {
                ServerMsg::PowChallenge { challenge, difficulty_bits } => {
                    let solved = tokio::task::spawn_blocking(move || pow::solve(&challenge, difficulty_bits)).await;
                    match solved {
                        Ok(nonce) => {
                            let frame = encode(&ClientMsg::PowSolution { nonce })
                                .expect("ClientMsg serialization cannot fail");
                            if let Err(e) = write_frame(&mut tls_writer, &frame).await {
                                controller.handle_ui_event(UiEvent::Disconnected(format!("PoW write error: {e}")));
                                return;
                            }
                        }
                        Err(_) => {
                            controller.handle_ui_event(UiEvent::Disconnected("PoW task failed".into()));
                            return;
                        }
                    }
                }
                ServerMsg::AuthOk { chat_id, token, expires_at } => {
                    controller.handle_ui_event(UiEvent::Server(ServerMsg::AuthOk { chat_id, token, expires_at }));
                    break;
                }
                ServerMsg::AuthFailed { reason } => {
                    controller.handle_ui_event(UiEvent::Server(ServerMsg::AuthFailed { reason }));
                    return;
                }
                _ => {}
            }
        }

        let mut history_buf: Vec<HistoryEntry> = Vec::new();
        let mut history_chat_id: i64 = -1;

        loop {
            tokio::select! {
                read_res = read_frame(&mut reader) => {
                    match read_res {
                        Ok(line) if line.ends_with('\n') => {
                            let payload = line.trim();
                            if payload.is_empty() {
                                continue;
                            }
                            let msg: ServerMsg = match decode(payload) {
                                Ok(m) => m,
                                Err(_) => {
                                    controller.handle_ui_event(UiEvent::Disconnected(
                                        "Malformed frame from server".into(),
                                    ));
                                    break;
                                }
                            };
                            match msg {
                                ServerMsg::HistoryMsg { message_id, sender_chat_id, timestamp, content, is_read } => {
                                    if history_chat_id != -1 {
                                        history_buf.push(HistoryEntry {
                                            message_id,
                                            sender_chat_id,
                                            timestamp,
                                            content,
                                            is_read,
                                        });
                                    }
                                }
                                ServerMsg::HistoryEnd => {
                                    let peer = history_chat_id;
                                    let mut batch = std::mem::take(&mut history_buf);
                                    batch.reverse();
                                    history_chat_id = -1;
                                    if peer != -1 {
                                        controller.handle_ui_event(UiEvent::HistoryBatch { peer_chat_id: peer, entries: batch });
                                    }
                                }
                                other => {
                                    controller.handle_ui_event(UiEvent::Server(other));
                                }
                            }
                        }
                        Ok(_) => {
                            controller.handle_ui_event(UiEvent::Disconnected("Connection closed".into()));
                            break;
                        }
                        Err(e) => {
                            controller.handle_ui_event(UiEvent::Disconnected(format!("Read error: {e}")));
                            break;
                        }
                    }
                }
                cmd_opt = cmd_rx.recv() => {
                    match cmd_opt {
                        Some(ClientMsg::HistoryReq { peer_chat_id }) => {
                            history_chat_id = peer_chat_id;
                            history_buf.clear();
                            let frame = encode(&ClientMsg::HistoryReq { peer_chat_id })
                                .expect("ClientMsg serialization cannot fail");
                            if let Err(e) = write_frame(&mut tls_writer, &frame).await {
                                controller.handle_ui_event(UiEvent::Disconnected(format!("Write error: {e}")));
                                break;
                            }
                        }
                        Some(cmd) => {
                            let frame = encode(&cmd).expect("ClientMsg serialization cannot fail");
                            if let Err(e) = write_frame(&mut tls_writer, &frame).await {
                                controller.handle_ui_event(UiEvent::Disconnected(format!("Write error: {e}")));
                                break;
                            }
                        }
                        None => {
                            controller.handle_ui_event(UiEvent::Disconnected("Disconnected".into()));
                            break;
                        }
                    }
                }
            }
        }
    }
        .await;

    let mut guard = sender_slot.lock().unwrap();
    *guard = None;
}

async fn read_frame<S>(reader: &mut S) -> std::io::Result<String>
where
    S: AsyncBufReadExt + Unpin,
{
    let mut out: Vec<u8> = Vec::with_capacity(512);
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(String::from_utf8_lossy(&out).into_owned());
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            out.extend_from_slice(&available[..=pos]);
            reader.consume(pos + 1);
            if out.len() > MAX_LINE_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "frame exceeds MAX_LINE_BYTES",
                ));
            }
            return Ok(String::from_utf8_lossy(&out).into_owned());
        }
        out.extend_from_slice(available);
        let used = available.len();
        reader.consume(used);
        if out.len() > MAX_LINE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame exceeds MAX_LINE_BYTES",
            ));
        }
    }
}

async fn write_frame<W>(writer: &mut W, frame: &str) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(frame.as_bytes()).await?;
    writer.flush().await
}