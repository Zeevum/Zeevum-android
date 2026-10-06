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

    if let Some(profile) = settings::profile_label() {
        app.set_window_title(format!("Zeevum [{profile}]").into());
    }

    app.set_active_chat_messages(ModelRc::from(Rc::new(VecModel::default())));
    app.set_conversations_list(ModelRc::from(Rc::new(VecModel::default())));
    app.set_contacts_friends(ModelRc::from(Rc::new(VecModel::default())));
    app.set_contacts_requests(ModelRc::from(Rc::new(VecModel::default())));
    app.set_group_members(ModelRc::from(Rc::new(VecModel::default())));
    app.set_group_addable(ModelRc::from(Rc::new(VecModel::default())));

    let controller = controller::AppController::new(app.as_weak());

    controller.try_auto_login();

    {
        let controller = controller.clone();
        app.on_connect(move |addr, login, pass, is_reg, invite| {
            controller.handle_connect(addr, login, pass, is_reg, invite);
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
        app.on_logout(move || {
            controller.handle_logout();
        });
    }

    {
        let controller = controller.clone();
        app.on_change_password(move |old_pass, new_pass| {
            controller.handle_change_password(old_pass, new_pass);
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

    app.on_is_at_bottom(|content_height, view_height, content_y| {
        controller::is_at_bottom(content_height, view_height, content_y)
    });

    {
        let controller = controller.clone();
        app.on_reconnect(move || {
            controller.handle_reconnect_now();
        });
    }

    {
        let controller = controller.clone();
        app.on_open_conversation(move |conv_id| {
            controller.handle_open_conversation(conv_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_show_contacts(move || {
            controller.handle_show_contacts();
        });
    }

    {
        let controller = controller.clone();
        app.on_contacts_back(move || {
            controller.handle_contacts_back();
        });
    }

    {
        let controller = controller.clone();
        app.on_open_friend_chat(move |user_id| {
            controller.handle_open_friend_chat(user_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_accept_request(move |user_id| {
            controller.handle_accept_request(user_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_toggle_friend(move |user_id| {
            controller.handle_toggle_friend(user_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_create_group(move |title| {
            controller.handle_create_group(title);
        });
    }

    {
        let controller = controller.clone();
        app.on_open_group_panel(move || {
            controller.handle_open_group_panel();
        });
    }

    {
        let controller = controller.clone();
        app.on_group_back(move || {
            controller.handle_group_back();
        });
    }

    {
        let controller = controller.clone();
        app.on_group_rename(move |title| {
            controller.handle_group_rename(title);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_add_member(move |login| {
            controller.handle_group_add_member(login);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_remove_member(move |user_id| {
            controller.handle_group_remove_member(user_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_select_member(move |user_id| {
            controller.handle_group_select_member(user_id);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_apply_rights(move |user_id, ci, inv, ban, adm| {
            controller.handle_group_apply_rights(user_id, ci, inv, ban, adm);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_leave(move |transfer| {
            controller.handle_group_leave(transfer);
        });
    }

    {
        let controller = controller.clone();
        app.on_group_delete(move || {
            controller.handle_group_delete();
        });
    }

    {
        let controller = controller.clone();
        app.on_group_join(move || {
            controller.handle_group_join();
        });
    }

    app.run()?;

    Ok(())
}
