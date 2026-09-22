use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

/// The file holds a session token, so it is created 0600 rather than left to
/// the umask, and the mode is set again after writing because it only applies
/// when the file is created.
#[cfg(unix)]
fn write_private(path: &Path, json: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(json.as_bytes())?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn write_private(path: &Path, json: &str) -> std::io::Result<()> {
    std::fs::write(path, json)
}

/// A file written by an older build stays 0644 until something writes it
/// again, and a user who is already logged in may never write it. Tightened
/// on read for the same reason.
#[cfg(unix)]
fn tighten_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn tighten_permissions(_path: &Path) {}

pub fn load_settings() -> AppSettings {
    let path = get_settings_path();
    tighten_permissions(&path);
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
        let _ = write_private(&get_settings_path(), &json);
    }
}

pub fn clear_session() {
    let mut settings = load_settings();
    settings.token = None;
    settings.user_id = None;
    settings.expires_at = None;
    if let Ok(json) = serde_json::to_string_pretty(&settings) {
        let _ = write_private(&get_settings_path(), &json);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn the_settings_file_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("zeevum-settings-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        super::write_private(&path, "{\"token\":\"secret\"}").unwrap();
        let created = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(created & 0o777, 0o600);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        super::tighten_permissions(&path);
        let tightened = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(tightened & 0o777, 0o600);

        let _ = std::fs::remove_file(&path);
    }
}
