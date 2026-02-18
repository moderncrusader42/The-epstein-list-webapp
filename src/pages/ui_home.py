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
                ### How This Website Works

                1. **The List**
                The List tracks every person who appears in the files, whether they are guilty or not. Soon there will be a toggle to show only guilty people by default, with non-guilty entries available on demand.

                2. **Sources**
                The Sources page organizes evidence used to explain what people in The List did or what happened to them. A source can be simple (for example, "Michael knew him" with photos together) or more complex using markdown explanations.

                3. **Unsorted Files**
                Reliable official files are expected to appear in Unsorted Files first. From there, they can be reviewed and turned into Sources.

                4. **Theories**
                Theories are separate from The List but can be referenced from people entries. Example: a theory that someone was switched out, with comparison images and markdown explanation.

                5. **Permissions and Review Flow**
                Any signed-in user can submit proposals. Reviewers can accept or decline proposals. Trusted contributors can become Editors and bypass reviewer approval for their own edits, and can later become Reviewers. Admin permissions manage access and oversight. All proposal edits and decisions are traceable, so you can see who submitted, edited, and accepted each change.

                We are actively looking for contributors right now, so role assignments are currently being handled with flexibility.
                """
            )

        home_app.load(timed_page_load("/app", _header_home), outputs=[hdr])
        home_app.load(timed_page_load("/app", _welcome_text), outputs=[hero])

    return home_app
