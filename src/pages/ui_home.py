import gradio as gr
from html import escape

from src.pages.header import render_header, with_light_mode_head
from src.css.utils import load_css
from src.login_logic import get_user
from src.page_timing import timed_page_load


def _header_home(request: gr.Request):
    return render_header(path="/app", request=request)


def _welcome_text(request: gr.Request) -> str:
    user = get_user(request) or {}
    name = user.get("name") or user.get("email") or "user"
    safe_name = escape(str(name))
    return (
        "<section class='home-hero'>"
        "<p class='home-hero__eyebrow'>Statement of purpose</p>"
        f"<h1 class='home-hero__title'>Welcome, {safe_name}</h1>"
        "<p class='home-hero__lead'>"
        "This page was created because the Epstein files are spread across many releases and are difficult to review in full."
        "</p>"
        "<p class='home-hero__reason'>"
        "<strong>Purpose:</strong> Bring the case information into one organized place, track who appears to be guilty and who is not, "
        "and structure theories so they can be confirmed or debunked as evidence is reviewed."
        "</p>"
        "<div class='home-hero__chips'>"
        "<span class='home-chip'>People index</span>"
        "<span class='home-chip'>Evidence sources</span>"
        "<span class='home-chip'>Proposal review flow</span>"
        "</div>"
        "</section>"
    )


def _home_guide_text() -> str:
    return (
        "<section class='home-guide'>"
        "<article class='home-card'>"
        "<h3><span class='home-card__index'>1</span>The List</h3>"
        "<p>"
        "The List tracks every person who appears in the files, whether they are guilty or not."
        "</p>"
        "<p class='home-card__note'>"
        "Soon there will be a toggle to show guilty entries first, with non-guilty entries available on demand."
        "</p>"
        "</article>"
        "<article class='home-card'>"
        "<h3><span class='home-card__index'>2</span>Sources</h3>"
        "<p>"
        "The Sources page organizes evidence used to explain what people in The List did, or what happened to them."
        "</p>"
        "<p class='home-card__note'>"
        "A source can be simple (for example, &quot;Michael knew him&quot; with photos) or more complex with markdown explanations."
        "</p>"
        "</article>"
        "<article class='home-card'>"
        "<h3><span class='home-card__index'>3</span>Unsorted Files</h3>"
        "<p>"
        "Reliable official files are expected to appear in Unsorted Files first."
        "</p>"
        "<p class='home-card__note'>"
        "From there, they can be reviewed and turned into Sources."
        "</p>"
        "</article>"
        "<article class='home-card'>"
        "<h3><span class='home-card__index'>4</span>Theories</h3>"
        "<p>"
        "Theories are separate from The List but can be referenced from people entries."
        "</p>"
        "<p class='home-card__note'>"
        "Example: a theory that someone was switched out, with comparison images and a markdown explanation."
        "</p>"
        "</article>"
        "<article class='home-card home-card--wide'>"
        "<h3><span class='home-card__index'>5</span>Permissions and Review Flow</h3>"
        "<ul class='home-role-list'>"
        "<li><strong>User:</strong> can submit proposals.</li>"
        "<li><strong>Reviewer:</strong> can accept or decline proposals.</li>"
        "<li><strong>Editor:</strong> trusted contributor who can bypass reviewer approval for their own edits.</li>"
        "<li><strong>Admin:</strong> manages access and oversight.</li>"
        "</ul>"
        "<p class='home-card__note'>"
        "All proposal edits and decisions are traceable, so you can see who submitted, edited, and accepted each change."
        "</p>"
        "</article>"
        "</section>"
        "<section class='home-callout'>"
        "<h4>Contributors welcome</h4>"
        "<p>"
        "We are actively looking for people right now, so role assignments are currently being handled with flexibility."
        "</p>"
        "<p class='home-callout__repo'>"
        "Code repository: "
        "<a href='https://github.com/moderncrusader42/The-epstein-list-webapp/tree/main' target='_blank' rel='noopener noreferrer'>"
        "https://github.com/moderncrusader42/The-epstein-list-webapp/tree/main"
        "</a>"
        "</p>"
        "</section>"
    )


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
            gr.HTML(_home_guide_text())

        home_app.load(timed_page_load("/app", _header_home), outputs=[hdr])
        home_app.load(timed_page_load("/app", _welcome_text), outputs=[hero])

    return home_app
