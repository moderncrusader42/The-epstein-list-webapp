from __future__ import annotations

import logging
from pathlib import Path

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.sources_list.core_sources import (
    CATALOG_VIEW_ICONS,
    CATALOG_VIEW_LIST,
    TAG_FILTER_ALL_OPTION,
    _create_source_card_for_list,
    _load_sources_list_page,
    _rerender_sources_catalog,
    _sync_create_file_origins_editor,
    _update_sources_catalog_by_tag_filter,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "sources_page.css"
TAG_FILTER_JS_PATH = ASSETS_DIR / "js" / "sources_tag_filter.js"
CREATE_TAGS_JS_PATH = ASSETS_DIR / "js" / "sources_create_tags.js"
FILE_ORIGINS_JS_PATH = ASSETS_DIR / "js" / "sources_file_origins_inline.js"
COVER_IMAGE_CROP_JS_PATH = ASSETS_DIR / "js" / "sources_cover_image_crop.js"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing sources asset at %s", path)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_tag_filter_js() -> str:
    script = _read_asset(TAG_FILTER_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _load_create_tags_js() -> str:
    script = _read_asset(CREATE_TAGS_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _load_file_origins_js() -> str:
    script = _read_asset(FILE_ORIGINS_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _load_cover_image_crop_js() -> str:
    script = _read_asset(COVER_IMAGE_CROP_JS_PATH)
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


def _header_sources(request: gr.Request):
    return render_header(path="/sources", request=request)


def _open_create_modal():
    return gr.update(visible=True), gr.update(value="")


def _close_create_modal():
    return gr.update(visible=False), gr.update(value="")


def _sync_create_modal_visibility(status_message: str):
    if str(status_message or "").strip().startswith("✅"):
        return gr.update(visible=False)
    return gr.update(visible=True)


def make_sources_app() -> gr.Blocks:
    stylesheet = _load_css()
    tag_filter_js = _load_tag_filter_js()
    create_tags_js = _load_create_tags_js()
    file_origins_js = _load_file_origins_js()
    cover_image_crop_js = _load_cover_image_crop_js()
    head_scripts = "\n".join(
        script for script in (tag_filter_js, create_tags_js, file_origins_js, cover_image_crop_js) if script
    )

    with gr.Blocks(
        title="Sources",
        css=stylesheet or None,
        head=with_light_mode_head(head_scripts or None),
    ) as app:
        hdr = gr.HTML()

        all_sources_state = gr.State([])
        tag_filter_selection_state = gr.State([])

        with gr.Column(elem_id="sources-shell"):
            with gr.Row(elem_id="sources-title-row"):
                title_md = gr.HTML("<h2>Sources</h2>", elem_id="sources-title")
                with gr.Column(elem_id="sources-filter-row", visible=True, scale=0, min_width=220) as tag_filter_row:
                    tag_filter = gr.Dropdown(
                        label="Filter by tags",
                        choices=[(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)],
                        value=[],
                        multiselect=True,
                        allow_custom_value=False,
                        interactive=True,
                        show_label=False,
                        container=False,
                        elem_id="sources-tag-filter",
                    )

                catalog_view_mode = gr.Radio(
                    choices=[("Icons", CATALOG_VIEW_ICONS), ("List", CATALOG_VIEW_LIST)],
                    value=CATALOG_VIEW_ICONS,
                    show_label=False,
                    container=False,
                    interactive=True,
                    elem_id="sources-catalog-view",
                    scale=0,
                    min_width=150,
                )
                open_create_modal_btn = gr.Button(
                    "+",
                    variant="secondary",
                    elem_id="sources-create-trigger",
                    scale=0,
                    min_width=36,
                )

            sources_html = gr.HTML(elem_id="sources-catalog")

        with gr.Column(visible=False, elem_id="sources-create-modal") as create_modal:
            with gr.Column(elem_id="sources-create-modal-card"):
                with gr.Row(elem_id="sources-create-modal-header"):
                    gr.Markdown("### New source")
                    close_create_modal_btn = gr.Button(
                        "×",
                        variant="secondary",
                        elem_id="sources-create-close-btn",
                        scale=0,
                        min_width=40,
                    )

                create_status = gr.Markdown(elem_id="sources-create-status")
                create_name = gr.Textbox(
                    label="Source name",
                    placeholder="WHO Reports, Clinical Trials, ...",
                    elem_id="sources-create-name",
                )
                create_description_markdown = gr.Textbox(
                    label="Description/Explanation (Markdown)",
                    lines=6,
                    placeholder="Why this source matters, what it contains, and references if needed.",
                    elem_id="sources-create-summary",
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
                            "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' "
                            "role='button' tabindex='0' aria-label='Select source image'>"
                            "<span class='sources-cover-image-preview-empty'>No image selected yet.</span>"
                            "</div>",
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

                with gr.Row(elem_id="sources-create-modal-actions"):
                    create_source_btn = gr.Button("Create source + upload files", variant="primary")
                    cancel_create_modal_btn = gr.Button("Cancel", variant="secondary")

        app.load(timed_page_load("/sources", _header_sources), outputs=[hdr])
        app.load(
            timed_page_load("/sources", _load_sources_list_page),
            outputs=[
                title_md,
                tag_filter_row,
                tag_filter,
                tag_filter_selection_state,
                catalog_view_mode,
                all_sources_state,
                sources_html,
                create_status,
            ],
        )

        tag_filter.input(
            timed_page_load(
                "/sources",
                _update_sources_catalog_by_tag_filter,
                label="update_sources_catalog_by_tag_filter",
            ),
            inputs=[tag_filter, tag_filter_selection_state, all_sources_state, catalog_view_mode],
            outputs=[tag_filter, tag_filter_selection_state, sources_html],
            show_progress=False,
        )

        catalog_view_mode.change(
            timed_page_load("/sources", _rerender_sources_catalog, label="rerender_sources_catalog"),
            inputs=[catalog_view_mode, tag_filter_selection_state, all_sources_state],
            outputs=[sources_html],
            show_progress=False,
        )

        open_create_modal_btn.click(
            _open_create_modal,
            outputs=[create_modal, create_status],
            show_progress=False,
        )

        close_create_modal_btn.click(
            _close_create_modal,
            outputs=[create_modal, create_status],
            show_progress=False,
        )

        cancel_create_modal_btn.click(
            _close_create_modal,
            outputs=[create_modal, create_status],
            show_progress=False,
        )

        create_files.change(
            timed_page_load("/sources", _sync_create_file_origins_editor, label="sync_create_file_origins_editor"),
            inputs=[create_files, create_file_origins],
            outputs=[create_file_origins_editor, create_file_origins],
            show_progress=False,
        )

        create_source_btn.click(
            timed_page_load("/sources", _create_source_card_for_list, label="create_source_card_for_list"),
            inputs=[
                create_name,
                create_description_markdown,
                create_cover_media,
                create_cover_media_data,
                create_tags,
                create_files,
                create_file_origins,
                tag_filter_selection_state,
                catalog_view_mode,
            ],
            outputs=[
                create_status,
                create_name,
                create_description_markdown,
                create_cover_media,
                create_cover_media_data,
                create_cover_media_preview,
                create_tags,
                create_files,
                create_file_origins,
                create_file_origins_editor,
                tag_filter,
                tag_filter_selection_state,
                all_sources_state,
                sources_html,
            ],
            show_progress=False,
        ).then(
            _sync_create_modal_visibility,
            inputs=[create_status],
            outputs=[create_modal],
            show_progress=False,
        )

    return app
