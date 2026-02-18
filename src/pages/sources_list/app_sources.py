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
    _load_sources_list_page,
    _rerender_sources_catalog,
    _update_sources_catalog_by_tag_filter,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "sources_page.css"
TAG_FILTER_JS_PATH = ASSETS_DIR / "js" / "sources_tag_filter.js"


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


def _header_sources(request: gr.Request):
    return render_header(path="/sources", request=request)


def make_sources_app() -> gr.Blocks:
    stylesheet = _load_css()
    head_scripts = _load_tag_filter_js()

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
                open_create_page_btn = gr.Button(
                    "+",
                    variant="secondary",
                    elem_id="sources-create-trigger",
                    scale=0,
                    min_width=36,
                )

            create_status = gr.Markdown(value="", visible=False, elem_id="sources-create-status")
            sources_html = gr.HTML(elem_id="sources-catalog")

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

        open_create_page_btn.click(
            fn=None,
            inputs=None,
            outputs=None,
            js="() => { window.location.assign('/source-create/'); }",
            show_progress=False,
        )

    return app
