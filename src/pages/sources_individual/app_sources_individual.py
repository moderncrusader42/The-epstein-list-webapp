from __future__ import annotations

import logging
from pathlib import Path

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.sources_list.core_sources import (
    DEFAULT_MARKDOWN_VIEW,
    FILE_VIEW_ICONS,
    FILE_VIEW_LIST,
    MARKDOWN_VIEW_PREVIEW,
    MARKDOWN_VIEW_RAW,
    _cancel_source_editor_for_individual,
    _load_sources_individual_editor_page,
    _open_source_editor_for_individual,
    _refresh_source_editor_markdown_preview,
    _rerender_source_files_for_individual_page,
    _save_source_editor_for_individual,
    _sync_create_file_origins_editor,
    _toggle_source_editor_markdown_view,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "sources_individual_page.css"
COVER_IMAGE_CROP_JS_PATH = ASSETS_DIR.parent / "sources_list" / "js" / "sources_cover_image_crop.js"
FILE_ORIGINS_JS_PATH = ASSETS_DIR.parent / "sources_list" / "js" / "sources_file_origins_inline.js"
EDITOR_JS_PATH = ASSETS_DIR / "js" / "sources_individual_editor.js"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing individual sources asset at %s", path)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_cover_image_crop_js() -> str:
    script = _read_asset(COVER_IMAGE_CROP_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _load_file_origins_js() -> str:
    script = _read_asset(FILE_ORIGINS_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _load_editor_js() -> str:
    script = _read_asset(EDITOR_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _header_sources_individual(request: gr.Request):
    return render_header(path="/sources", request=request)


def make_sources_individual_app() -> gr.Blocks:
    stylesheet = _load_css()
    cover_image_crop_js = _load_cover_image_crop_js()
    file_origins_js = _load_file_origins_js()
    editor_js = _load_editor_js()
    head_scripts = "\n".join(script for script in (cover_image_crop_js, file_origins_js, editor_js) if script)

    with gr.Blocks(
        title="Source",
        css=stylesheet or None,
        head=with_light_mode_head(head_scripts or None),
    ) as app:
        hdr = gr.HTML()
        selected_source_state = gr.State("")

        with gr.Column(elem_id="sources-shell"):
            with gr.Row(elem_id="sources-title-row"):
                title_md = gr.HTML("<h2>Source</h2>", elem_id="sources-title")
                file_view_mode = gr.Radio(
                    choices=[("Icons", FILE_VIEW_ICONS), ("List", FILE_VIEW_LIST)],
                    value=FILE_VIEW_ICONS,
                    show_label=False,
                    container=False,
                    interactive=True,
                    elem_id="sources-files-view",
                    scale=0,
                    min_width=150,
                )
                edit_btn = gr.Button(
                    "Edit source",
                    visible=False,
                    variant="secondary",
                    elem_id="sources-edit-btn",
                    scale=0,
                    min_width=34,
                )

            with gr.Column(elem_id="sources-head-stack"):
                source_head_meta = gr.HTML(
                    "",
                    visible=False,
                    elem_id="sources-browser-head-meta",
                    container=False,
                )
                with gr.Column(elem_id="sources-browser-head-shell", visible=False) as source_head_shell:
                    source_head_description = gr.Markdown(
                        value="",
                        visible=False,
                        container=False,
                        elem_id="sources-browser-head-description",
                    )

            with gr.Column(visible=False, elem_id="sources-edit-shell") as edit_shell:
                edit_status = gr.Markdown(elem_id="sources-edit-status")
                edit_name = gr.Textbox(
                    value="",
                    visible=False,
                    interactive=True,
                    elem_id="sources-edit-name",
                )
                edit_tags = gr.Textbox(
                    value="",
                    visible=False,
                    interactive=True,
                    elem_id="sources-edit-tags",
                )

                with gr.Column(elem_id="sources-edit-media-col"):
                    with gr.Row(elem_id="sources-edit-cover-image-row"):
                        gr.Markdown("**Source image**")
                        edit_cover_media = gr.UploadButton(
                            "+",
                            file_types=["image"],
                            file_count="single",
                            elem_id="sources-edit-cover-image-plus-btn",
                            scale=0,
                            min_width=40,
                        )
                    edit_cover_media_data = gr.Textbox(
                        value="",
                        visible=False,
                        interactive=True,
                        elem_id="sources-edit-cover-image-data",
                    )
                    edit_cover_media_preview = gr.HTML(
                        "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' "
                        "role='button' tabindex='0' aria-label='Select source image'>"
                        "<span class='sources-cover-image-preview-empty'>No image selected yet.</span>"
                        "</div>",
                        elem_id="sources-edit-cover-image-preview",
                        container=False,
                    )

                with gr.Row(elem_id="sources-edit-markdown-toolbar"):
                    gr.Markdown("**Description/Explanation**")
                    edit_description_view_mode = gr.Radio(
                        choices=[
                            ("Compiled", MARKDOWN_VIEW_PREVIEW),
                            ("Raw markdown", MARKDOWN_VIEW_RAW),
                        ],
                        value=DEFAULT_MARKDOWN_VIEW,
                        show_label=False,
                        container=False,
                        interactive=True,
                        elem_id="sources-edit-summary-view-mode",
                        scale=0,
                        min_width=0,
                    )
                with gr.Row(elem_id="sources-edit-markdown-grid"):
                    edit_description_markdown = gr.Textbox(
                        show_label=False,
                        lines=12,
                        placeholder="Write markdown here. Supports citations and references like the people display.",
                        elem_id="sources-edit-summary",
                        visible=False,
                    )
                    edit_description_preview = gr.Markdown(
                        value="",
                        visible=True,
                        elem_id="sources-edit-summary-preview",
                    )

                edit_files = gr.File(
                    label="Add files",
                    file_count="multiple",
                    file_types=["image", "video", ".pdf", ".txt", ".md", ".csv", ".json"],
                    elem_id="sources-edit-files",
                )
                edit_file_origins_editor = gr.HTML(
                    visible=False,
                    elem_id="sources-edit-file-origins-editor",
                )
                edit_file_origins = gr.Textbox(
                    value="",
                    show_label=False,
                    elem_id="sources-edit-file-origins-state",
                    elem_classes=["sources-hidden-input"],
                )
                edit_existing_files_editor = gr.HTML(
                    visible=False,
                    elem_id="sources-edit-existing-files-editor",
                )
                edit_delete_files = gr.Textbox(
                    value="",
                    show_label=False,
                    elem_id="sources-edit-delete-files-state",
                    elem_classes=["sources-hidden-input"],
                )
                with gr.Row(elem_id="sources-edit-actions"):
                    save_edit_btn = gr.Button("Save", variant="primary", elem_id="sources-edit-save-btn")
                    cancel_edit_btn = gr.Button("Cancel", variant="secondary", elem_id="sources-edit-cancel-btn")

            files_html = gr.HTML(elem_id="sources-files-html")

        app.load(timed_page_load("/sources-individual", _header_sources_individual), outputs=[hdr])
        app.load(
            timed_page_load("/sources-individual", _load_sources_individual_editor_page),
            outputs=[
                title_md,
                selected_source_state,
                source_head_shell,
                source_head_meta,
                source_head_description,
                files_html,
                edit_btn,
                edit_status,
            ],
        )

        edit_btn.click(
            timed_page_load("/sources-individual", _open_source_editor_for_individual, label="open_source_editor_for_individual"),
            inputs=[selected_source_state],
            outputs=[
                edit_shell,
                edit_name,
                edit_description_markdown,
                edit_description_preview,
                edit_description_view_mode,
                edit_cover_media,
                edit_cover_media_data,
                edit_cover_media_preview,
                edit_tags,
                edit_files,
                edit_file_origins_editor,
                edit_file_origins,
                edit_existing_files_editor,
                edit_delete_files,
                edit_status,
            ],
            show_progress=False,
        )

        cancel_edit_btn.click(
            _cancel_source_editor_for_individual,
            inputs=[selected_source_state],
            outputs=[
                edit_shell,
                edit_name,
                edit_description_markdown,
                edit_description_preview,
                edit_description_view_mode,
                edit_cover_media,
                edit_cover_media_data,
                edit_cover_media_preview,
                edit_tags,
                edit_files,
                edit_file_origins_editor,
                edit_file_origins,
                edit_existing_files_editor,
                edit_delete_files,
                edit_status,
            ],
            show_progress=False,
        )

        edit_description_view_mode.change(
            timed_page_load(
                "/sources-individual",
                _toggle_source_editor_markdown_view,
                label="toggle_source_editor_markdown_view",
            ),
            inputs=[edit_description_view_mode, edit_description_markdown],
            outputs=[edit_description_markdown, edit_description_preview],
            show_progress=False,
        )
        edit_description_markdown.blur(
            timed_page_load(
                "/sources-individual",
                _refresh_source_editor_markdown_preview,
                label="blur_refresh_source_editor_markdown_preview",
            ),
            inputs=[edit_description_markdown],
            outputs=[edit_description_preview],
            show_progress=False,
        )

        edit_files.change(
            timed_page_load("/sources-individual", _sync_create_file_origins_editor, label="sync_source_edit_file_origins_editor"),
            inputs=[edit_files, edit_file_origins],
            outputs=[edit_file_origins_editor, edit_file_origins],
            show_progress=False,
        )

        save_edit_btn.click(
            timed_page_load(
                "/sources-individual",
                _save_source_editor_for_individual,
                label="save_source_editor_for_individual",
            ),
            inputs=[
                selected_source_state,
                edit_name,
                edit_description_markdown,
                edit_cover_media,
                edit_cover_media_data,
                edit_tags,
                edit_files,
                edit_file_origins,
                edit_delete_files,
                file_view_mode,
            ],
            outputs=[
                edit_status,
                title_md,
                source_head_shell,
                source_head_meta,
                source_head_description,
                files_html,
                edit_btn,
                edit_name,
                edit_description_markdown,
                edit_description_preview,
                edit_description_view_mode,
                edit_cover_media,
                edit_cover_media_data,
                edit_cover_media_preview,
                edit_tags,
                edit_files,
                edit_file_origins_editor,
                edit_file_origins,
                edit_existing_files_editor,
                edit_delete_files,
            ],
            show_progress=False,
        )

        file_view_mode.change(
            timed_page_load(
                "/sources-individual",
                _rerender_source_files_for_individual_page,
                label="rerender_source_files_for_individual_page",
            ),
            inputs=[selected_source_state, file_view_mode],
            outputs=[files_html],
            show_progress=False,
        )

    return app
