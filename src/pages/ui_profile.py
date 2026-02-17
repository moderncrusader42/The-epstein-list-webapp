import gradio as gr
from src.pages.header import render_header, with_light_mode_head
from src.login_logic import get_user
from src.css.utils import load_css
from src.page_timing import timed_page_load


def _header_profile(request: gr.Request):
    return render_header(path="/profile", request=request)


def _user_info(request: gr.Request):
    user = get_user(request) or {}
    name = user.get("name") or user.get("email") or "User"
    email = user.get("email") or ""
    photo = (user.get("picture") or "").strip()

    # Simple inline HTML for user info
    if photo:
        avatar_html = f'<img alt="{name}" src="{photo}" style="width:64px;height:64px;border-radius:50%;border:1px solid #e5e7eb;object-fit:cover;" />'
    else:
        initial = (name or "?")[0:1].upper()
        avatar_html = f'<div style="width:64px;height:64px;border-radius:50%;background:#e5e7eb;color:#374151;display:flex;align-items:center;justify-content:center;font-weight:700;">{initial}</div>'

    html = f"""
        <div class="profile-wrap">
            <div class="profile-card">
                <div class="user-row">
                    <div class="avatar">{avatar_html}</div>
                    <div class="meta">
                        <div class="name">{name}</div>
                        <div class="email">{email}</div>
                    </div>
                </div>
            </div>
        </div>
    """
    return gr.update(value=html)


def make_profile_app() -> gr.Blocks:
    profile_css = load_css("profile_page.css")
    with gr.Blocks(
        title="The List Profile",
        css=profile_css,
        head=with_light_mode_head(None),
    ) as profile_app:
        hdr = gr.HTML()

        # Profile card with basic account info
        with gr.Row(elem_id="profile-top-row"):
            with gr.Column(scale=1):
                user_html = gr.HTML(elem_id="profile-user")

        gr.HTML(
            """
            <div class="profile-wrap">
              <div class="profile-placeholder">
                <h4>Profile setup coming soon</h4>
                <p>We'll use this page to manage personal details in a future update.</p>
              </div>
            </div>
            """
        )

        # Populate user info + header
        profile_app.load(timed_page_load("/profile", _user_info), outputs=[user_html])
        profile_app.load(timed_page_load("/profile", _header_profile), outputs=[hdr])

    return profile_app
