from __future__ import annotations

import json
import logging
from pathlib import Path
from uuid import uuid4

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.theory_display.core_theory import (
    DEFAULT_MARKDOWN_VIEW,
    MARKDOWN_VIEW_PREVIEW,
    MARKDOWN_VIEW_RAW,
    _fetch_source_citation_options,
    _fetch_tag_catalog,
    _render_article_markdown,
    _render_person_hero,
    _role_flags_from_request,
    _submit_new_profile_proposal,
    _toggle_proposal_markdown_view,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "theory_display_page.css"
EDITOR_JS_PATH = ASSETS_DIR / "js" / "theory_editor.js"

DEFAULT_CREATE_NAME = "New theory"
DEFAULT_CREATE_TITLE = "Unassigned"
DEFAULT_CREATE_TAGS = ""
DEFAULT_CREATE_IMAGE_URL = "/images/Logo.png"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing theory create asset at %s", path)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_editor_js() -> str:
    script = _read_asset(EDITOR_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _header_theory_create(request: gr.Request):
    return render_header(path="/theories", request=request)


def _default_proposal_markdown() -> str:
    return (
        f"# {DEFAULT_CREATE_NAME}\n\n"
        "## Snapshot\n"
        f"- **Title:** {DEFAULT_CREATE_TITLE}\n"
        "- **Tags:** \n\n"
        "## Notes\n"
        "- Add context for reviewers here.\n"
    )


def _build_new_theory_seed() -> dict[str, str]:
    return {
        "slug": f"new-theory-{uuid4().hex[:8]}",
        "name": DEFAULT_CREATE_NAME,
        "title": DEFAULT_CREATE_TITLE,
        "tags": DEFAULT_CREATE_TAGS,
        "image_url": DEFAULT_CREATE_IMAGE_URL,
    }


def _build_detail_hero_from_seed(seed: dict[str, str]) -> str:
    tag_catalog = _fetch_tag_catalog()
    return _render_person_hero(
        {
            "slug": seed["slug"],
            "name": seed["name"],
            "title": seed["title"],
            "bucket": seed["title"],
            "image_url": seed["image_url"],
            "tags": [],
            "tag_catalog": tag_catalog,
        }
    )


def _reset_create_editor_state():
    seed = _build_new_theory_seed()
    markdown_template = _default_proposal_markdown()
    return (
        gr.update(value=_build_detail_hero_from_seed(seed), visible=True),
        seed["slug"],
        seed["name"],
        seed["title"],
        seed["tags"],
        "",
        "",
        "",
        "",
        gr.update(value=None),
        gr.update(value=""),
        markdown_template,
        _render_article_markdown(markdown_template),
        gr.update(value=DEFAULT_MARKDOWN_VIEW),
        "",
    )


def _load_theory_create_page(request: gr.Request):
    _, _, can_submit = _role_flags_from_request(request)
    intro_update = gr.update(value="", visible=False)
    if not can_submit:
        intro_update = gr.update(
            value="Your `base_user` privilege is currently disabled, so proposal submission is blocked.",
            visible=True,
        )
    citation_source_options = json.dumps(_fetch_source_citation_options(), ensure_ascii=True)

    return (
        "<h2>New Theory Card + Article Proposal</h2>",
        intro_update,
        gr.update(value="", visible=False),
        gr.update(value="", visible=False),
        *_reset_create_editor_state(),
        citation_source_options,
    )


def _cancel_theory_create_form():
    return _reset_create_editor_state()


def _submit_new_theory_proposal_with_refresh(
    proposal_note: str,
    proposal_name: str,
    proposal_bucket: str,
    proposal_tags: str,
    proposal_image: object,
    proposal_image_data: str,
    proposal_markdown: str,
    request: gr.Request,
):
    (
        message,
        next_note,
        next_name,
        next_bucket,
        next_tags,
        image_update,
        image_data_update,
        next_markdown,
        next_preview,
    ) = _submit_new_profile_proposal(
        proposal_note,
        proposal_name,
        proposal_bucket,
        proposal_tags,
        proposal_image,
        proposal_image_data,
        proposal_markdown,
        request,
    )

    if str(message).strip().startswith("✅"):
        reset_state = _reset_create_editor_state()
        return (*reset_state[:-1], message)

    return (
        gr.update(),
        gr.update(),
        gr.update(),
        gr.update(),
        gr.update(),
        next_note,
        next_name,
        next_bucket,
        next_tags,
        image_update,
        image_data_update,
        gr.update(value=next_markdown),
        gr.update(value=next_preview),
        gr.update(),
        message,
    )


def make_theory_create_app() -> gr.Blocks:
    stylesheet = _load_css()
    editor_js = _load_editor_js()
    with gr.Blocks(
        title="Create Theory Proposal",
        css=stylesheet or None,
        head=with_light_mode_head(editor_js),
    ) as app:
        hdr = gr.HTML()

        with gr.Column(elem_id="people-shell"):
            with gr.Row(elem_id="people-title-row"):
                title_md = gr.HTML("<h2>New Theory Card + Article Proposal</h2>", elem_id="people-title")
                gr.HTML(
                    "<a class='the-list-create-back-link' href='/theories/'>Back to Theories</a>",
                    elem_id="the-list-create-back-link",
                )

            detail_html = gr.HTML(visible=True, elem_id="person-detail-hero")

            current_slug = gr.Textbox(value="", visible=False, interactive=False, elem_id="the-list-current-slug")
            current_name = gr.Textbox(value="", visible=False, interactive=False, elem_id="the-list-current-name")
            current_bucket = gr.Textbox(value="", visible=False, interactive=False, elem_id="the-list-current-bucket")
            current_tags = gr.Textbox(value="", visible=False, interactive=False, elem_id="the-list-current-tags")

            with gr.Column(
                visible=True,
                elem_id="the-list-card-proposal-shell",
                elem_classes=["the-list-create-card-proposal-shell"],
            ):
                card_proposal_help = gr.Markdown(elem_id="the-list-card-proposal-help", visible=False)
                with gr.Row(elem_id="the-list-card-proposal-grid"):
                    card_proposal_name = gr.Textbox(label="Card name", elem_id="the-list-card-proposal-name")
                    card_proposal_bucket = gr.Textbox(
                        label="Card title",
                        elem_id="the-list-card-proposal-bucket",
                    )
                card_proposal_tags = gr.Textbox(
                    label="Card tags",
                    lines=2,
                    placeholder="Comma-separated tags (example: left-footed, crossing, stamina)",
                    elem_id="the-list-card-proposal-tags",
                )
                with gr.Row(elem_id="the-list-card-image-row"):
                    gr.Markdown("**Card image**")
                    card_proposal_image = gr.UploadButton(
                        "+",
                        file_types=["image"],
                        file_count="single",
                        elem_id="the-list-card-image-plus-btn",
                        scale=0,
                        min_width=40,
                    )
                card_proposal_image_data = gr.Textbox(
                    value="",
                    visible=False,
                    interactive=True,
                    elem_id="the-list-card-image-data",
                )

            create_intro = gr.Markdown(elem_id="the-list-create-intro", visible=False)

            with gr.Column(visible=True, elem_id="the-list-proposal-shell"):
                proposal_help = gr.Markdown(elem_id="the-list-proposal-help", visible=False)
                with gr.Row(elem_id="the-list-markdown-toolbar"):
                    gr.Markdown("**Proposed article markdown**")
                    with gr.Row(elem_id="the-list-markdown-toolbar-controls"):
                        gr.Button(
                            "Cite",
                            variant="secondary",
                            elem_id="the-list-proposal-cite-btn",
                            scale=0,
                            min_width=52,
                        )
                        gr.Button(
                            "Bib",
                            variant="secondary",
                            elem_id="the-list-proposal-bib-btn",
                            scale=0,
                            min_width=52,
                        )
                        proposal_view_mode = gr.Radio(
                            choices=[
                                ("Compiled", MARKDOWN_VIEW_PREVIEW),
                                ("Raw markdown", MARKDOWN_VIEW_RAW),
                            ],
                            value=DEFAULT_MARKDOWN_VIEW,
                            show_label=False,
                            container=False,
                            interactive=True,
                            elem_id="the-list-proposal-view-mode",
                            scale=0,
                            min_width=0,
                        )
                with gr.Row(elem_id="the-list-proposal-editor-grid"):
                    proposal_markdown = gr.Textbox(
                        show_label=False,
                        lines=14,
                        placeholder="Write the full article markdown here...",
                        elem_id="the-list-proposal-markdown-input",
                        visible=False,
                    )
                    proposal_preview = gr.Markdown(value="", visible=True, elem_id="the-list-proposal-preview")
            citation_source_options = gr.Textbox(
                value="[]",
                visible=False,
                interactive=False,
                elem_id="the-list-source-citation-options",
            )

            with gr.Column(visible=True, elem_id="the-list-create-summary-shell"):
                gr.Markdown(
                    "Submit once to create both **card** and **article** proposals for review.",
                    elem_id="the-list-create-summary-help",
                )
                proposal_note = gr.Textbox(
                    label="Proposal summary",
                    lines=2,
                    placeholder="Short summary for reviewers...",
                    elem_id="the-list-card-proposal-note",
                )
                with gr.Row(elem_id="the-list-create-proposal-actions"):
                    submit_new_profile_btn = gr.Button("Submit", variant="primary")
                    cancel_new_profile_btn = gr.Button("Cancel", variant="secondary")
                card_proposal_status = gr.Markdown(elem_id="the-list-card-proposal-status")

        app.load(timed_page_load("/theory-create", _header_theory_create), outputs=[hdr])
        app.load(
            timed_page_load("/theory-create", _load_theory_create_page),
            outputs=[
                title_md,
                create_intro,
                card_proposal_help,
                proposal_help,
                detail_html,
                current_slug,
                current_name,
                current_bucket,
                current_tags,
                proposal_note,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_image,
                card_proposal_image_data,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                card_proposal_status,
                citation_source_options,
            ],
        )

        proposal_view_mode.change(
            timed_page_load(
                "/theory-create",
                _toggle_proposal_markdown_view,
                label="toggle_proposal_markdown_view",
            ),
            inputs=[proposal_view_mode, proposal_markdown],
            outputs=[proposal_markdown, proposal_preview],
        )
        proposal_markdown.blur(
            timed_page_load(
                "/theory-create",
                _toggle_proposal_markdown_view,
                label="blur_toggle_proposal_markdown_view",
            ),
            inputs=[proposal_view_mode, proposal_markdown],
            outputs=[proposal_markdown, proposal_preview],
        )

        submit_new_profile_btn.click(
            timed_page_load(
                "/theory-create",
                _submit_new_theory_proposal_with_refresh,
                label="submit_new_theory_proposal_with_refresh",
            ),
            inputs=[
                proposal_note,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_image,
                card_proposal_image_data,
                proposal_markdown,
            ],
            outputs=[
                detail_html,
                current_slug,
                current_name,
                current_bucket,
                current_tags,
                proposal_note,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_image,
                card_proposal_image_data,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                card_proposal_status,
            ],
        )

        cancel_new_profile_btn.click(
            timed_page_load("/theory-create", _cancel_theory_create_form, label="cancel_theory_create_form"),
            outputs=[
                detail_html,
                current_slug,
                current_name,
                current_bucket,
                current_tags,
                proposal_note,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_image,
                card_proposal_image_data,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                card_proposal_status,
            ],
        )

    return app
