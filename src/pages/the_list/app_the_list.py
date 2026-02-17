from __future__ import annotations

import html
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.the_list.core_the_list import (
    TAG_FILTER_ALL_OPTION,
    _build_tag_filter_choices,
    _build_tag_filter_update,
    _choice_values,
    _fetch_all_people,
    _filter_people_for_tag_selection,
    _normalize_selection,
    _normalize_tag,
    _parse_tag_query_values,
    _query_param,
    _render_tag_chips,
)

logger = logging.getLogger(__name__)
timing_logger = logging.getLogger("uvicorn.error")

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "the_list_page.css"
TAG_FILTER_JS_PATH = ASSETS_DIR / "js" / "the_list_tag_filter.js"


def _log_timing(event_name: str, start: float, **fields: object) -> None:
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    if fields:
        field_text = " ".join(f"{key}={value}" for key, value in fields.items())
        timing_logger.info("the_list.page.timing event=%s ms=%.2f %s", event_name, elapsed_ms, field_text)
        return
    timing_logger.info("the_list.page.timing event=%s ms=%.2f", event_name, elapsed_ms)


def _read_asset(path: Path) -> str:
    start = time.perf_counter()
    try:
        text_value = path.read_text(encoding="utf-8")
        _log_timing("read_asset", start, path=path.name, bytes=len(text_value))
        return text_value
    except FileNotFoundError:
        logger.warning("Missing The List asset at %s", path)
        _log_timing("read_asset_missing", start, path=path.name)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_tag_filter_js() -> str:
    script = _read_asset(TAG_FILTER_JS_PATH)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _versioned_media_url(image_url: object, image_version: object) -> str:
    """
    Add a stable cache-busting token only for `/media/...` URLs.
    Local `/images/...` assets keep their original URL.
    """
    raw = str(image_url or "").strip() or "/images/Logo.png"
    if not raw.startswith("/media/"):
        return raw

    token = str(image_version or "").strip()
    if not token:
        return raw

    parts = urlsplit(raw)
    params = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "v"]
    params.append(("v", token))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))


def _render_cards(people: Sequence[Dict[str, object]]) -> str:
    start = time.perf_counter()
    if not people:
        _log_timing("render_cards.empty", start)
        return '<div class="people-empty">No profiles are available.</div>'

    cards: List[str] = []
    for row in people:
        name = html.escape(str(row.get("name") or "Unknown"))
        slug = str(row.get("slug") or "")
        title = html.escape(str(row.get("title") or row.get("bucket") or "Unassigned"))
        image_url = html.escape(_versioned_media_url(row.get("image_url"), row.get("image_version")), quote=True)
        href = f"/people-display/?slug={quote(slug, safe='-')}"
        tag_values = [_normalize_tag(str(tag)) for tag in row.get("tags", []) if str(tag).strip()]
        tags_markup = _render_tag_chips(row.get("tags", []))
        tags_json_attr = html.escape(json.dumps(tag_values, ensure_ascii=True), quote=True)
        cards.append(
            f"""
            <a class="person-card" href="{href}" data-tags-json="{tags_json_attr}">
              <div class="person-card__image-wrap">
                <img class="person-card__image" src="{image_url}" alt="{name}" loading="lazy" />
              </div>
              <div class="person-card__content">
                <h3 class="person-card__title">{name}</h3>
                <div class="person-card__bucket">{title}</div>
                <div class="person-card__tags">{tags_markup}</div>
              </div>
            </a>
            """.strip()
        )

    html_value = f'<div class="people-grid">{"".join(cards)}</div>'
    _log_timing("render_cards", start, people=len(people), html_bytes=len(html_value))
    return html_value


def _update_people_cards_by_tag_filter(
    current_selection: Sequence[object] | None,
    previous_selection: Sequence[object] | None,
):
    total_start = time.perf_counter()
    step_start = time.perf_counter()
    people = _fetch_all_people()
    _log_timing("update_tag_filter.fetch_people", step_start, people=len(people))
    step_start = time.perf_counter()
    choices = _build_tag_filter_choices(people)
    choice_values = _choice_values(choices)
    all_key = _normalize_tag(TAG_FILTER_ALL_OPTION)
    allowed_values = [value for value in choice_values if _normalize_tag(value) != all_key]
    _log_timing(
        "update_tag_filter.build_choices",
        step_start,
        choices=len(choices),
        allowed_values=len(allowed_values),
    )

    step_start = time.perf_counter()
    current_norm = {_normalize_tag(value) for value in _normalize_selection(current_selection)}
    previous_norm = {_normalize_tag(value) for value in _normalize_selection(previous_selection)}
    current_filtered = [value for value in allowed_values if _normalize_tag(value) in current_norm]
    previous_filtered = [value for value in allowed_values if _normalize_tag(value) in previous_norm]
    _log_timing(
        "update_tag_filter.normalize_selection",
        step_start,
        current=len(current_filtered),
        previous=len(previous_filtered),
    )

    current_has_all = all_key in current_norm
    previous_has_all = all_key in previous_norm

    next_selection = current_filtered
    dropdown_update = gr.update()

    if current_has_all and not previous_has_all:
        if (not current_filtered) or (current_filtered == previous_filtered):
            next_selection = [TAG_FILTER_ALL_OPTION, *allowed_values]
            dropdown_update = gr.update(value=next_selection)
        elif len(current_filtered) < len(allowed_values):
            next_selection = current_filtered
        else:
            next_selection = [TAG_FILTER_ALL_OPTION, *allowed_values]
    elif (not current_has_all) and previous_has_all and len(current_filtered) == len(allowed_values):
        next_selection = []
        dropdown_update = gr.update(value=next_selection)
    elif current_has_all and previous_has_all and len(current_filtered) < len(previous_filtered):
        next_selection = current_filtered
    elif current_has_all and len(current_filtered) == len(allowed_values):
        next_selection = [TAG_FILTER_ALL_OPTION, *allowed_values]
    elif len(current_filtered) == len(allowed_values) and allowed_values:
        next_selection = [TAG_FILTER_ALL_OPTION, *allowed_values]
        dropdown_update = gr.update(value=next_selection)

    next_has_all = any(_normalize_tag(value) == all_key for value in next_selection)
    if current_has_all and not next_has_all:
        dropdown_update = gr.update(value=next_selection)

    step_start = time.perf_counter()
    filtered_people = _filter_people_for_tag_selection(people, next_selection)
    _log_timing("update_tag_filter.filter_people", step_start, filtered_people=len(filtered_people))
    step_start = time.perf_counter()
    cards_update = gr.update(value=_render_cards(filtered_people), visible=True)
    _log_timing("update_tag_filter.render_update", step_start)
    _log_timing(
        "update_tag_filter.total",
        total_start,
        selected=len(next_selection),
        filtered_people=len(filtered_people),
    )
    return dropdown_update, next_selection, cards_update


def _header_the_list(request: gr.Request):
    start = time.perf_counter()
    header_html = render_header(path="/the-list", request=request)
    _log_timing("header_the_list.render_header", start, html_bytes=len(header_html))
    return header_html


def _load_the_list_page(request: gr.Request):
    total_start = time.perf_counter()
    try:
        step_start = time.perf_counter()
        people = _fetch_all_people()
        _log_timing("load_the_list_page.fetch_people", step_start, people=len(people))
        step_start = time.perf_counter()
        selected_tags = _parse_tag_query_values(_query_param(request, "tag"))
        _log_timing("load_the_list_page.parse_query_tags", step_start, selected_tags=len(selected_tags))
        step_start = time.perf_counter()
        tag_filter_update, _tag_filter_choices, tag_filter_selection = _build_tag_filter_update(
            people,
            selected_tags,
            default_to_all=True,
        )
        _log_timing(
            "load_the_list_page.build_tag_filter_update",
            step_start,
            selected=len(tag_filter_selection),
        )
        step_start = time.perf_counter()
        filtered_people = _filter_people_for_tag_selection(people, tag_filter_selection)
        _log_timing("load_the_list_page.filter_people", step_start, filtered_people=len(filtered_people))
        step_start = time.perf_counter()
        cards_html = _render_cards(filtered_people)
        _log_timing("load_the_list_page.render_cards", step_start, html_bytes=len(cards_html))
        _log_timing(
            "load_the_list_page.total",
            total_start,
            people=len(people),
            selected=len(tag_filter_selection),
            filtered=len(filtered_people),
        )

        return (
            "<h2>The List</h2>",
            gr.update(visible=True),
            tag_filter_update,
            tag_filter_selection,
            gr.update(value=cards_html, visible=True),
        )
    except Exception as exc:  # noqa: BLE001
        _log_timing("load_the_list_page.error", total_start)
        logger.exception("Failed to load The List page: %s", exc)
        return (
            "<h2>The List</h2>",
            gr.update(visible=False),
            gr.update(choices=[(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)], value=[], interactive=True),
            [],
            gr.update(value='<div class="people-empty">Could not load profiles.</div>', visible=True),
        )


def make_the_list_app() -> gr.Blocks:
    stylesheet = _load_css()
    tag_filter_js = _load_tag_filter_js()
    with gr.Blocks(
        title="The List",
        css=stylesheet or None,
        head=with_light_mode_head(tag_filter_js),
    ) as app:
        hdr = gr.HTML()

        with gr.Column(elem_id="people-shell"):
            with gr.Row(elem_id="people-title-row"):
                title_md = gr.HTML("<h2>The List</h2>", elem_id="people-title")
                with gr.Column(elem_id="people-filter-row", visible=True, scale=0, min_width=210) as tag_filter_row:
                    tag_filter = gr.Dropdown(
                        label="Filter by tags",
                        choices=[(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)],
                        value=[],
                        multiselect=True,
                        allow_custom_value=False,
                        interactive=True,
                        show_label=False,
                        container=False,
                        elem_id="people-tag-filter",
                    )
                gr.HTML(
                    "<a class='the-list-create-profile-btn' href='/people-create/' aria-label='Create new card and article proposal'>+</a>",
                    elem_id="the-list-create-profile-link",
                )

            tag_filter_selection_state = gr.State([])
            cards_html = gr.HTML(elem_id="people-cards")

        app.load(timed_page_load("/the-list", _header_the_list), outputs=[hdr])
        app.load(
            timed_page_load("/the-list", _load_the_list_page),
            outputs=[
                title_md,
                tag_filter_row,
                tag_filter,
                tag_filter_selection_state,
                cards_html,
            ],
        )

        tag_filter.input(
            timed_page_load(
                "/the-list",
                _update_people_cards_by_tag_filter,
                label="update_people_cards_by_tag_filter",
            ),
            inputs=[tag_filter, tag_filter_selection_state],
            outputs=[tag_filter, tag_filter_selection_state, cards_html],
            show_progress=False,
        )

    return app
