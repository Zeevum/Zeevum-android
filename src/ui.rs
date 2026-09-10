use chrono::Local;
use slint::{Model, VecModel};

use crate::types::LogColor;
use crate::{LogEntry, MainWindow};

pub fn append_log(ui: &MainWindow, text: &str, color: LogColor) {
    let log_model = ui.get_log();
    if let Some(model) = log_model.as_any().downcast_ref::<VecModel<LogEntry>>() {
        model.push(LogEntry {
            text: format!("[{}] {text}", Local::now().format("%H:%M")).into(),
            color: color.into(),
        });
    }
}
