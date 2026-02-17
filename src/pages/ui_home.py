import gradio as gr

from src.pages.header import render_header, with_light_mode_head
from src.css.utils import load_css
from src.login_logic import get_user
from src.page_timing import timed_page_load


def _header_home(request: gr.Request):
    return render_header(path="/app", request=request)


def _welcome_text(request: gr.Request) -> str:
    user = get_user(request) or {}
    name = user.get("name") or user.get("email") or "user"
    return f"<h2>Welcome, {name}!</h2>"


def make_home_app() -> gr.Blocks:
    home_css = load_css("home_page.css")
    with gr.Blocks(
        title="The List Home",
        css=home_css,
        head=with_light_mode_head(None),
    ) as home_app:
        hdr = gr.HTML()

        with gr.Column(elem_id="home-shell"):
            hero = gr.HTML()
            gr.Markdown(
                """
                ### Main Panel
                This instance has been simplified for the next phase of the project.

                - Use **Privileges** to manage access.
                - Use **Administration** to review or edit available tables.
                """
            )

        home_app.load(timed_page_load("/app", _header_home), outputs=[hdr])
        home_app.load(timed_page_load("/app", _welcome_text), outputs=[hero])

    return home_app
