from __future__ import annotations

import html
import json
import logging
from pathlib import Path

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.theory_display.core_theory import (
    DEFAULT_MARKDOWN_VIEW,
    EDIT_TOGGLE_BUTTON_LABEL,
    MARKDOWN_VIEW_PREVIEW,
    MARKDOWN_VIEW_RAW,
    _append_markdown_image,
    _build_proposal_help_messages,
    _cancel_card_editor,
    _cancel_markdown_editor,
    _fetch_person as _fetch_theory,
    _fetch_source_citation_options,
    _fetch_tag_catalog,
    _query_param,
    _render_article_markdown,
    _render_person_hero as _render_theory_hero,
    _role_flags_from_request,
    _submit_card_proposal,
    _submit_markdown_proposal,
    _tags_to_text,
    _toggle_card_editor,
    _toggle_markdown_editor,
    _toggle_proposal_markdown_view,
    _user_has_editor_privilege,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "theory_display_page.css"
EDITOR_JS_PATH = ASSETS_DIR / "js" / "theory_editor.js"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing theory display asset at %s", path)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_editor_js() -> str:
    script = _read_asset(EDITOR_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _header_theory_display(request: gr.Request):
    return render_header(path="/theories", request=request)


def _render_theory_selection_prompt() -> str:
    return (
        "<section class='person-detail-card person-detail-card--missing'>"
        "<div class='person-detail-card__body'>"
        "<h2>Select a theory</h2>"
        "<p>Open a card from Theories to view and edit it here.</p>"
        "</div></section>"
    )


def _render_missing_theory(slug: str) -> str:
    safe_slug = html.escape(slug or "unknown")
    return (
        "<section class='person-detail-card person-detail-card--missing'>"
        "<div class='person-detail-card__body'>"
        "<h2>Theory not found</h2>"
        f"<p>No theory matched slug <code>{safe_slug}</code>.</p>"
        "</div></section>"
    )


def _empty_detail_state(
    title_html: str,
    detail_html: str,
    markdown_help: str = "",
    card_help: str = "",
    citation_source_options: str = "[]",
):
    return (
        title_html,
        gr.update(value=detail_html, visible=True),
        gr.update(value="", visible=False),
        "",
        "",
        "",
        "",
        "",
        False,
        gr.update(value=EDIT_TOGGLE_BUTTON_LABEL, visible=False),
        False,
        gr.update(value=EDIT_TOGGLE_BUTTON_LABEL, visible=False),
        gr.update(value="", visible=False),
        gr.update(visible=False),
        markdown_help,
        "",
        "",
        gr.update(visible=False),
        card_help,
        "",
        "",
        "",
        "",
        citation_source_options,
    )


def _load_theory_display_page(request: gr.Request):
    try:
        user, _can_review, can_submit = _role_flags_from_request(request)
        user_name = str(user.get("name") or user.get("email") or "User")
        user_email = str(user.get("email") or "").strip().lower()
        markdown_help, card_help = _build_proposal_help_messages(
            user_name,
            user_email,
            can_submit,
            is_editor=_user_has_editor_privilege(user),
        )
        citation_source_options = json.dumps(_fetch_source_citation_options(), ensure_ascii=True)

        slug = _query_param(request, "slug").lower()
        if not slug:
            return _empty_detail_state(
                "<h2>Theories</h2>",
                _render_theory_selection_prompt(),
                markdown_help,
                card_help,
                citation_source_options,
            )

        theory = _fetch_theory(slug)
        if theory is None:
            return _empty_detail_state(
                "<h2>Theory not found</h2>",
                _render_missing_theory(slug),
                markdown_help,
                card_help,
                citation_source_options,
            )
        theory["tag_catalog"] = _fetch_tag_catalog()

        markdown_value = str(theory.get("markdown") or "")
        name_value = str(theory.get("name") or "")
        bucket_value = str(theory.get("title") or theory.get("bucket") or "")
        tags_value = _tags_to_text(theory.get("tags", []))

        return (
            f"<h2>{html.escape(name_value or 'Theory')}</h2>",
            gr.update(value=_render_theory_hero(theory), visible=True),
            gr.update(value=_render_article_markdown(markdown_value), visible=True),
            slug,
            markdown_value,
            name_value,
            bucket_value,
            tags_value,
            False,
            gr.update(value=EDIT_TOGGLE_BUTTON_LABEL, visible=can_submit),
            False,
            gr.update(value=EDIT_TOGGLE_BUTTON_LABEL, visible=can_submit),
            gr.update(value="", visible=False),
            gr.update(visible=False),
            markdown_help,
            markdown_value,
            "",
            gr.update(visible=False),
            card_help,
            name_value,
            bucket_value,
            tags_value,
            "",
            citation_source_options,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to load theory display page: %s", exc)
        return _empty_detail_state(
            "<h2>Theories</h2>",
            _render_missing_theory("load-error"),
        )


def make_theory_display_app() -> gr.Blocks:
    stylesheet = _load_css()
    editor_js = _load_editor_js()
    with gr.Blocks(
        title="Theory Display",
        css=stylesheet or None,
        head=with_light_mode_head(editor_js),
    ) as app:
        hdr = gr.HTML()
        markdown_edit_mode_state = gr.State(False)
        card_edit_mode_state = gr.State(False)

        with gr.Column(elem_id="people-shell"):
            with gr.Row(elem_id="people-title-row"):
                title_md = gr.HTML("<h2>Theories</h2>", elem_id="people-title")

            markdown_edit_btn = gr.Button(
                EDIT_TOGGLE_BUTTON_LABEL,
                visible=False,
                variant="secondary",
                elem_id="the-list-markdown-edit-btn",
            )
            card_edit_btn = gr.Button(
                EDIT_TOGGLE_BUTTON_LABEL,
                visible=False,
                variant="secondary",
                elem_id="the-list-card-edit-btn",
            )
            review_btn_html = gr.HTML(visible=False, elem_id="the-list-review-link")
            detail_html = gr.HTML(visible=False, elem_id="person-detail-hero")

            with gr.Column(visible=False, elem_id="the-list-card-proposal-shell") as card_proposal_shell:
                card_proposal_help = gr.Markdown(elem_id="the-list-card-proposal-help")
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
                card_proposal_note = gr.Textbox(
                    label="Card change summary",
                    lines=2,
                    placeholder="Short summary for creators...",
                    elem_id="the-list-card-proposal-note",
                )
                with gr.Row(elem_id="the-list-card-proposal-actions"):
                    submit_card_proposal_btn = gr.Button("Submit", variant="primary")
                    cancel_card_edit_btn = gr.Button("Cancel", variant="secondary")
                card_proposal_status = gr.Markdown(elem_id="the-list-card-proposal-status")

            detail_markdown = gr.Markdown(visible=False, elem_id="person-detail-markdown")

            current_slug = gr.Textbox(value="", visible=False, interactive=False, elem_id="the-list-current-slug")
            current_markdown = gr.Textbox(
                value="",
                visible=False,
                interactive=False,
                elem_id="the-list-current-markdown",
            )
            current_name = gr.Textbox(
                value="",
                visible=False,
                interactive=False,
                elem_id="the-list-current-name",
            )
            current_bucket = gr.Textbox(
                value="",
                visible=False,
                interactive=False,
                elem_id="the-list-current-bucket",
            )
            current_tags = gr.Textbox(
                value="",
                visible=False,
                interactive=False,
                elem_id="the-list-current-tags",
            )
            citation_source_options = gr.Textbox(
                value="[]",
                visible=False,
                interactive=False,
                elem_id="the-list-source-citation-options",
            )

            with gr.Column(visible=False, elem_id="the-list-proposal-shell") as proposal_shell:
                proposal_help = gr.Markdown(elem_id="the-list-proposal-help")
                proposal_note = gr.Textbox(
                    label="Article change summary",
                    lines=2,
                    placeholder="Short summary for creators...",
                )
                with gr.Row(elem_id="the-list-markdown-toolbar"):
                    gr.Markdown("**Proposed article markdown**")
                    with gr.Row(elem_id="the-list-markdown-toolbar-controls"):
                        proposal_image_plus_btn = gr.UploadButton(
                            "+",
                            file_types=["image"],
                            file_count="single",
                            elem_id="the-list-proposal-image-plus-btn",
                            scale=0,
                            min_width=36,
                        )
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
                        placeholder="Edit the article markdown here...",
                        elem_id="the-list-proposal-markdown-input",
                    )
                    proposal_preview = gr.Markdown(value="", visible=True, elem_id="the-list-proposal-preview")
                with gr.Row(elem_id="the-list-proposal-actions"):
                    submit_markdown_proposal_btn = gr.Button("Submit Article Proposal", variant="primary")
                    cancel_markdown_edit_btn = gr.Button("Cancel", variant="secondary")
                proposal_status = gr.Markdown(elem_id="the-list-proposal-status")

        app.load(timed_page_load("/theory-display", _header_theory_display), outputs=[hdr])
        app.load(
            timed_page_load("/theory-display", _load_theory_display_page),
            outputs=[
                title_md,
                detail_html,
                detail_markdown,
                current_slug,
                current_markdown,
                current_name,
                current_bucket,
                current_tags,
                markdown_edit_mode_state,
                markdown_edit_btn,
                card_edit_mode_state,
                card_edit_btn,
                review_btn_html,
                proposal_shell,
                proposal_help,
                proposal_markdown,
                proposal_status,
                card_proposal_shell,
                card_proposal_help,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_status,
                citation_source_options,
            ],
        )

        markdown_edit_btn.click(
            timed_page_load("/theory-display", _toggle_markdown_editor, label="toggle_markdown_editor"),
            inputs=[markdown_edit_mode_state, current_slug, current_markdown],
            outputs=[
                markdown_edit_mode_state,
                detail_markdown,
                proposal_shell,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                markdown_edit_btn,
                proposal_status,
            ],
        )

        cancel_markdown_edit_btn.click(
            timed_page_load("/theory-display", _cancel_markdown_editor, label="cancel_markdown_editor"),
            inputs=[current_markdown],
            outputs=[
                markdown_edit_mode_state,
                detail_markdown,
                proposal_shell,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                markdown_edit_btn,
                proposal_note,
                proposal_status,
            ],
        )

        proposal_view_mode.change(
            timed_page_load(
                "/theory-display",
                _toggle_proposal_markdown_view,
                label="toggle_proposal_markdown_view",
            ),
            inputs=[proposal_view_mode, proposal_markdown],
            outputs=[proposal_markdown, proposal_preview],
        )
        proposal_markdown.blur(
            timed_page_load(
                "/theory-display",
                _toggle_proposal_markdown_view,
                label="blur_toggle_proposal_markdown_view",
            ),
            inputs=[proposal_view_mode, proposal_markdown],
            outputs=[proposal_markdown, proposal_preview],
        )

        proposal_image_plus_btn.upload(
            timed_page_load("/theory-display", _append_markdown_image, label="append_markdown_image"),
            inputs=[current_slug, proposal_markdown, proposal_image_plus_btn],
            outputs=[
                proposal_markdown,
                proposal_preview,
                proposal_status,
                proposal_image_plus_btn,
            ],
        )

        submit_markdown_proposal_btn.click(
            timed_page_load("/theory-display", _submit_markdown_proposal, label="submit_markdown_proposal"),
            inputs=[
                current_slug,
                proposal_note,
                proposal_markdown,
                current_markdown,
                markdown_edit_mode_state,
            ],
            outputs=[
                proposal_status,
                proposal_note,
                markdown_edit_mode_state,
                detail_markdown,
                proposal_shell,
                proposal_markdown,
                proposal_preview,
                proposal_view_mode,
                markdown_edit_btn,
            ],
        )

        card_edit_btn.click(
            timed_page_load("/theory-display", _toggle_card_editor, label="toggle_card_editor"),
            inputs=[card_edit_mode_state, current_slug, current_name, current_bucket, current_tags],
            outputs=[
                card_edit_mode_state,
                card_proposal_shell,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_edit_btn,
                card_proposal_status,
                card_proposal_image_data,
            ],
        )

        cancel_card_edit_btn.click(
            timed_page_load("/theory-display", _cancel_card_editor, label="cancel_card_editor"),
            inputs=[current_name, current_bucket, current_tags],
            outputs=[
                card_edit_mode_state,
                card_proposal_shell,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_edit_btn,
                card_proposal_note,
                card_proposal_status,
                card_proposal_image,
                card_proposal_image_data,
            ],
        )

        submit_card_proposal_btn.click(
            timed_page_load("/theory-display", _submit_card_proposal, label="submit_card_proposal"),
            inputs=[
                current_slug,
                card_proposal_note,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_proposal_image,
                card_proposal_image_data,
                current_name,
                current_bucket,
                current_tags,
                card_edit_mode_state,
            ],
            outputs=[
                card_proposal_status,
                card_proposal_note,
                card_proposal_image,
                card_proposal_image_data,
                card_edit_mode_state,
                card_proposal_shell,
                card_proposal_name,
                card_proposal_bucket,
                card_proposal_tags,
                card_edit_btn,
            ],
        )

    return app
