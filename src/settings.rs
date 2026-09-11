use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Default)]
pub struct AppSettings {
    pub server_address: Option<String>,
    pub login: Option<String>,
    pub token: Option<String>,
    pub user_id: Option<i64>,
    pub expires_at: Option<i64>,
}

fn get_settings_path() -> PathBuf {
    let mut path = dirs::config_dir()
        .unwrap_or_else(|| std::env::current_dir().expect("no config dir and no cwd"));
    let profile = std::env::var("ZEEVUM_PROFILE").unwrap_or_default();
    let filename = if profile.trim().is_empty() {
        "zeevum_client_settings.json".to_string()
    } else {
        format!("zeevum_client_settings_{}.json", profile)
    };
    path.push(filename);
    path
}

pub fn load_settings() -> AppSettings {
    let path = get_settings_path();
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_session(server_addr: &str, login: &str, token: &str, user_id: i64, expires_at: i64) {
    let settings = AppSettings {
        server_address: Some(server_addr.to_string()),
        login: Some(login.to_string()),
        token: Some(token.to_string()),
        user_id: Some(user_id),
        expires_at: Some(expires_at),
    };
    if let Ok(json) = serde_json::to_string_pretty(&settings) {
        let _ = std::fs::write(get_settings_path(), json);
    }
}

pub fn clear_session() {
    let mut settings = load_settings();
    settings.token = None;
    settings.user_id = None;
    settings.expires_at = None;
    if let Ok(json) = serde_json::to_string_pretty(&settings) {
        let _ = std::fs::write(get_settings_path(), json);
    }
}
