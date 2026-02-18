from __future__ import annotations

import logging
from pathlib import Path

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.sources_list.core_sources import (
    CATALOG_VIEW_ICONS,
    DEFAULT_MARKDOWN_VIEW,
    FILE_VIEW_ICONS,
    _create_source_card,
    _is_source_markdown_preview_mode,
    _render_cover_image_preview_markup,
    _render_source_description_markdown,
    _role_flags_from_request,
    _sync_create_file_origins_editor,
    _toggle_source_editor_markdown_view,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "sources_page.css"
CREATE_PAGE_CSS_PATH = ASSETS_DIR / "css" / "source_create_page.css"
CREATE_TAGS_JS_PATH = ASSETS_DIR / "js" / "sources_create_tags.js"
FILE_ORIGINS_JS_PATH = ASSETS_DIR / "js" / "sources_file_origins_inline.js"
COVER_IMAGE_CROP_JS_PATH = ASSETS_DIR / "js" / "sources_cover_image_crop.js"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing source create asset at %s", path)
        return ""


def _load_css() -> str:
    return "\n".join(part for part in (_read_asset(CSS_PATH), _read_asset(CREATE_PAGE_CSS_PATH)) if part)


def _load_script(path: Path) -> str:
    script = _read_asset(path)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _create_tags_editor_markup() -> str:
    return (
        "<section id='sources-create-tags-editor' class='sources-create-tags-editor'>"
        "<span class='sources-create-tags-editor__label'>Tags</span>"
        "<div class='sources-create-tags-editor__chips' aria-live='polite'></div>"
        "</section>"
    )


def _header_source_create(request: gr.Request):
    return render_header(path="/sources", request=request)


def _reset_source_create_form():
    default_preview_mode = _is_source_markdown_preview_mode(DEFAULT_MARKDOWN_VIEW)
    return (
        gr.update(value=""),
        gr.update(value="", visible=not default_preview_mode),
        gr.update(value=_render_source_description_markdown(""), visible=default_preview_mode),
        gr.update(value=DEFAULT_MARKDOWN_VIEW),
        gr.update(value=None),
        gr.update(value=""),
        gr.update(value=_render_cover_image_preview_markup("")),
        gr.update(value=""),
        gr.update(value=None),
        gr.update(value="", visible=False),
        gr.update(value=""),
    )


def _load_source_create_page(request: gr.Request):
    user, can_submit = _role_flags_from_request(request)
    intro = gr.update(value="", visible=False)
    if not user:
        intro = gr.update(value="You must sign in to create a source.", visible=True)
    elif not can_submit:
        intro = gr.update(
            value="Your `base_user` privilege is currently disabled, so source creation is blocked.",
            visible=True,
        )

    return (
        "<h2>New Source</h2>",
        intro,
        *_reset_source_create_form(),
        "",
    )


def _submit_source_create_form(
    source_name: str,
    source_description_markdown: str,
    source_cover_media: object,
    source_cover_media_data: str,
    source_tags: str,
    uploaded_files: object,
    source_file_origins: object,
    request: gr.Request,
):
    (
        status_message,
        clear_name,
        _clear_description_markdown,
        clear_cover_media,
        clear_cover_media_data,
        clear_cover_media_preview,
        clear_tags,
        clear_files,
        clear_origins,
        clear_origins_editor,
        _tag_filter_update,
        _tag_selection,
        _sources,
        _catalog_html,
        _source_selector_update,
        _resolved_selected_slug,
        _files_html,
    ) = _create_source_card(
        source_name,
        source_description_markdown,
        source_cover_media,
        source_cover_media_data,
        source_tags,
        uploaded_files,
        source_file_origins,
        "",
        None,
        CATALOG_VIEW_ICONS,
        FILE_VIEW_ICONS,
        request,
    )

    success = str(status_message or "").strip().startswith("✅")
    default_preview_mode = _is_source_markdown_preview_mode(DEFAULT_MARKDOWN_VIEW)
    if success:
        description_markdown_update = gr.update(value="", visible=not default_preview_mode)
        description_preview_update = gr.update(value=_render_source_description_markdown(""), visible=default_preview_mode)
        description_mode_update = gr.update(value=DEFAULT_MARKDOWN_VIEW)
    else:
        description_markdown_update = gr.update()
        description_preview_update = gr.update()
        description_mode_update = gr.update()

    return (
        clear_name,
        description_markdown_update,
        description_preview_update,
        description_mode_update,
        clear_cover_media,
        clear_cover_media_data,
        clear_cover_media_preview,
        clear_tags,
        clear_files,
        clear_origins_editor,
        clear_origins,
        status_message,
    )


def _cancel_source_create_form():
    return (*_reset_source_create_form(), "")


def make_sources_create_app() -> gr.Blocks:
    stylesheet = _load_css()
    scripts = "\n".join(
        script
        for script in (
            _load_script(CREATE_TAGS_JS_PATH),
            _load_script(FILE_ORIGINS_JS_PATH),
            _load_script(COVER_IMAGE_CROP_JS_PATH),
        )
        if script
    )

    with gr.Blocks(
        title="Create Source",
        css=stylesheet or None,
        head=with_light_mode_head(scripts or None),
    ) as app:
        hdr = gr.HTML()

        with gr.Column(elem_id="sources-create-shell"):
            with gr.Row(elem_id="sources-create-page-title-row"):
                title_md = gr.HTML("<h2>New Source</h2>", elem_id="sources-title")
                gr.HTML(
                    "<a class='the-list-create-back-link' href='/sources/'>Back to Sources</a>",
                    elem_id="sources-create-back-link",
                )

            create_intro = gr.Markdown(elem_id="sources-create-intro", visible=False)
            create_status = gr.Markdown(elem_id="sources-create-status")

            create_name = gr.Textbox(
                label="Source name",
                placeholder="WHO Reports, Clinical Trials, ...",
                elem_id="sources-create-name",
            )

            with gr.Row(elem_id="sources-create-markdown-toolbar"):
                gr.Markdown("**Description/Explanation**")
                create_description_view_mode = gr.Radio(
                    choices=[("Compiled", "preview"), ("Raw markdown", "raw")],
                    value=DEFAULT_MARKDOWN_VIEW,
                    show_label=False,
                    container=False,
                    interactive=True,
                    elem_id="sources-create-summary-view-mode",
                    scale=0,
                    min_width=0,
                )

            with gr.Row(elem_id="sources-create-markdown-grid"):
                create_description_markdown = gr.Textbox(
                    show_label=False,
                    lines=12,
                    placeholder="Why this source matters, what it contains, and references if needed.",
                    elem_id="sources-create-summary",
                    visible=False,
                )
                create_description_preview = gr.Markdown(
                    value="",
                    visible=True,
                    elem_id="sources-create-summary-preview",
                )

            with gr.Row(elem_id="sources-create-media-tags-row"):
                with gr.Column(elem_id="sources-create-media-col", scale=0, min_width=360):
                    with gr.Row(elem_id="sources-create-cover-image-row"):
                        gr.Markdown("**Source image**")
                        create_cover_media = gr.UploadButton(
                            "+",
                            file_types=["image"],
                            file_count="single",
                            elem_id="sources-create-cover-image-plus-btn",
                            scale=0,
                            min_width=40,
                        )
                    create_cover_media_data = gr.Textbox(
                        value="",
                        visible=False,
                        interactive=True,
                        elem_id="sources-create-cover-image-data",
                    )
                    create_cover_media_preview = gr.HTML(
                        _render_cover_image_preview_markup(""),
                        elem_id="sources-create-cover-image-preview",
                        container=False,
                    )

                with gr.Column(elem_id="sources-create-tags-col", scale=1, min_width=300):
                    create_tags = gr.Textbox(
                        label="Tags",
                        lines=1,
                        placeholder="research, policy, reference",
                        elem_id="sources-create-tags-input",
                        show_label=False,
                        container=False,
                        elem_classes=["sources-hidden-input"],
                    )
                    gr.HTML(
                        _create_tags_editor_markup(),
                        elem_id="sources-create-tags-editor-shell",
                        container=False,
                    )

            create_files = gr.File(
                label="Source files",
                file_count="multiple",
                file_types=["image", "video", ".pdf", ".txt", ".md", ".csv", ".json"],
                elem_id="sources-create-files",
            )
            create_file_origins_editor = gr.HTML(
                visible=False,
                elem_id="sources-create-file-origins-editor",
            )
            create_file_origins = gr.Textbox(
                value="",
                show_label=False,
                elem_id="sources-create-file-origins-state",
                elem_classes=["sources-hidden-input"],
            )

            with gr.Row(elem_id="sources-create-page-actions"):
                create_source_btn = gr.Button("Create source + upload files", variant="primary")
                cancel_create_btn = gr.Button("Cancel", variant="secondary")

        app.load(timed_page_load("/source-create", _header_source_create), outputs=[hdr])
        app.load(
            timed_page_load("/source-create", _load_source_create_page),
            outputs=[
                title_md,
                create_intro,
                create_name,
                create_description_markdown,
                create_description_preview,
                create_description_view_mode,
                create_cover_media,
                create_cover_media_data,
                create_cover_media_preview,
                create_tags,
                create_files,
                create_file_origins_editor,
                create_file_origins,
                create_status,
            ],
        )

        create_description_view_mode.change(
            timed_page_load(
                "/source-create",
                _toggle_source_editor_markdown_view,
                label="toggle_source_create_markdown_view",
            ),
            inputs=[create_description_view_mode, create_description_markdown],
            outputs=[create_description_markdown, create_description_preview],
            show_progress=False,
        )

        create_files.change(
            timed_page_load(
                "/source-create",
                _sync_create_file_origins_editor,
                label="sync_source_create_file_origins_editor",
            ),
            inputs=[create_files, create_file_origins],
            outputs=[create_file_origins_editor, create_file_origins],
            show_progress=False,
        )

        create_source_btn.click(
            timed_page_load(
                "/source-create",
                _submit_source_create_form,
                label="submit_source_create_form",
            ),
            inputs=[
                create_name,
                create_description_markdown,
                create_cover_media,
                create_cover_media_data,
                create_tags,
                create_files,
                create_file_origins,
            ],
            outputs=[
                create_name,
                create_description_markdown,
                create_description_preview,
                create_description_view_mode,
                create_cover_media,
                create_cover_media_data,
                create_cover_media_preview,
                create_tags,
                create_files,
                create_file_origins_editor,
                create_file_origins,
                create_status,
            ],
            show_progress=False,
        )

        cancel_create_btn.click(
            timed_page_load(
                "/source-create",
                _cancel_source_create_form,
                label="cancel_source_create_form",
            ),
            outputs=[
                create_name,
                create_description_markdown,
                create_description_preview,
                create_description_view_mode,
                create_cover_media,
                create_cover_media_data,
                create_cover_media_preview,
                create_tags,
                create_files,
                create_file_origins_editor,
                create_file_origins,
                create_status,
            ],
            show_progress=False,
        )

    return app
