mod controller;
mod network;
mod settings;
mod types;

use anyhow::Result;
use slint::{include_modules, ModelRc, VecModel};
use std::rc::Rc;

include_modules!();

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = MainWindow::new()?;

    app.set_friends_list(ModelRc::from(Rc::new(VecModel::default())));
    app.set_active_chat_messages(ModelRc::from(Rc::new(VecModel::default())));

    let controller = controller::AppController::new(app.as_weak());

    controller.try_auto_login();

    {
        let controller = controller.clone();
        app.on_connect(move |addr, login, pass, is_reg| {
            controller.handle_connect(addr, login, pass, is_reg);
        });
    }

    {
        let controller = controller.clone();
        app.on_disconnect(move || {
            controller.handle_disconnect();
        });
    }

    {
        let controller = controller.clone();
        app.on_save_settings(move |addr| {
            controller.handle_save_settings(addr);
        });
    }

    {
        let controller = controller.clone();
        app.on_check_password_strength(move |pass| {
            controller.handle_check_password_strength(pass);
        });
    }

    {
        let controller = controller.clone();
        app.on_search_user(move |login| {
            controller.handle_search_user(login);
        });
    }

    {
        let controller = controller.clone();
        app.on_open_chat(move |chat_id, login| {
            controller.handle_open_chat(chat_id, login);
        });
    }

    {
        let controller = controller.clone();
        app.on_send_msg(move |text| {
            controller.handle_send_msg(text);
        });
    }

    app.run()?;

    Ok(())
}
