from __future__ import annotations

import html
import json
import logging
import time
from typing import Dict, List, Sequence, Tuple

import gradio as gr
from sqlalchemy import text

from src.db import readonly_session_scope


timing_logger = logging.getLogger("uvicorn.error")

TAG_FILTER_ALL_OPTION = "All"


def _log_timing(event_name: str, start: float, **fields: object) -> None:
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    if fields:
        field_text = " ".join(f"{key}={value}" for key, value in fields.items())
        timing_logger.info("theories.timing event=%s ms=%.2f %s", event_name, elapsed_ms, field_text)
        return
    timing_logger.info("theories.timing event=%s ms=%.2f", event_name, elapsed_ms)


def _decode_tags(raw_value: object) -> List[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _fetch_all_people() -> List[Dict[str, object]]:
    """Historical function name kept for compatibility with existing callers."""
    total_start = time.perf_counter()
    query_start = time.perf_counter()
    with readonly_session_scope() as session:
        rows = session.execute(
            text(
                """
                WITH theory_tags AS (
                    SELECT
                        dedup.person_id,
                        json_agg(dedup.label ORDER BY dedup.label)::text AS tags_json
                    FROM (
                        SELECT DISTINCT
                            tpt.person_id,
                            tt.label
                        FROM app.theory_person_tags tpt
                        JOIN app.theory_tags tt
                            ON tt.id = tpt.tag_id
                    ) AS dedup
                    GROUP BY dedup.person_id
                )
                SELECT
                    c.slug,
                    th.name,
                    COALESCE(title.label, 'Unassigned') AS title,
                    c.bucket,
                    c.image_url,
                    EXTRACT(EPOCH FROM COALESCE(c.updated_at, CURRENT_TIMESTAMP))::bigint AS image_version,
                    COALESCE(tg.tags_json, '[]') AS tags_json
                FROM app.theory_cards c
                JOIN app.theories th
                    ON th.id = c.person_id
                LEFT JOIN app.theory_titles title
                    ON title.id = c.title_id
                LEFT JOIN theory_tags tg
                    ON tg.person_id = c.person_id
                ORDER BY th.name
                """
            )
        ).mappings().all()
    _log_timing("fetch_all_theories.query_rows", query_start, rows=len(rows))

    theories: List[Dict[str, object]] = []
    decode_start = time.perf_counter()
    for row in rows:
        theories.append(
            {
                "slug": row["slug"],
                "name": row["name"],
                "title": row["title"],
                "bucket": row["bucket"],
                "image_url": row["image_url"],
                "image_version": int(row["image_version"] or 0),
                "tags": _decode_tags(row["tags_json"]),
            }
        )
    _log_timing("fetch_all_theories.decode_rows", decode_start, theories=len(theories))
    _log_timing("fetch_all_theories.total", total_start, theories=len(theories))
    return theories


def _normalize_tag(value: str) -> str:
    return str(value or "").strip().lower()


def _choice_values(choices: Sequence[object]) -> List[str]:
    values: List[str] = []
    for choice in choices or []:
        if isinstance(choice, (tuple, list)) and len(choice) >= 2:
            raw_value = choice[1]
        else:
            raw_value = choice
        text_value = str(raw_value or "").strip()
        if text_value and text_value not in values:
            values.append(text_value)
    return values


def _normalize_selection(values: Sequence[object] | None) -> List[str]:
    normalized_values: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        text_value = str(value or "").strip()
        if not text_value:
            continue
        normalized = _normalize_tag(text_value)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(text_value)
    return normalized_values


def _build_tag_filter_choices(people: Sequence[Dict[str, object]]) -> List[Tuple[str, str]]:
    unique_tags: set[str] = set()
    for person in people:
        for raw_tag in person.get("tags", []):
            normalized_tag = _normalize_tag(str(raw_tag))
            if normalized_tag:
                unique_tags.add(normalized_tag)

    sorted_tags = sorted(unique_tags)
    choices: List[Tuple[str, str]] = [(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)]
    choices.extend((tag, tag) for tag in sorted_tags)
    return choices


def _parse_tag_query_values(raw_query: str) -> List[str]:
    query = str(raw_query or "").strip()
    if not query:
        return []

    if query.startswith("["):
        try:
            parsed = json.loads(query)
        except json.JSONDecodeError:
            parsed = []
        if isinstance(parsed, list):
            return _normalize_selection(parsed)

    values = [part.strip() for part in query.split(",") if part.strip()]
    return _normalize_selection(values)


def _resolve_tag_filter_selection(
    choices: Sequence[object],
    selected_values: Sequence[object] | None,
    *,
    default_to_all: bool,
) -> List[str]:
    all_values = _choice_values(choices)
    if not all_values:
        return []

    all_option_normalized = _normalize_tag(TAG_FILTER_ALL_OPTION)
    allowed_values = [value for value in all_values if _normalize_tag(value) != all_option_normalized]
    if not allowed_values:
        return []

    selected = _normalize_selection(selected_values)
    if not selected:
        return [TAG_FILTER_ALL_OPTION, *allowed_values] if default_to_all else []

    selected_normalized = {_normalize_tag(value) for value in selected}
    filtered_values = [value for value in allowed_values if _normalize_tag(value) in selected_normalized]
    has_all = all_option_normalized in selected_normalized

    if has_all:
        if not filtered_values:
            return [TAG_FILTER_ALL_OPTION, *allowed_values]
        if len(filtered_values) == len(allowed_values):
            return [TAG_FILTER_ALL_OPTION, *allowed_values]
        return filtered_values

    if not filtered_values:
        return [TAG_FILTER_ALL_OPTION, *allowed_values] if default_to_all else []

    if len(filtered_values) == len(allowed_values):
        return [TAG_FILTER_ALL_OPTION, *allowed_values]
    return filtered_values


def _build_tag_filter_update(
    people: Sequence[Dict[str, object]],
    selected_values: Sequence[object] | None = None,
    *,
    default_to_all: bool = True,
) -> tuple[gr.update, List[Tuple[str, str]], List[str]]:
    choices = _build_tag_filter_choices(people)
    resolved_selection = _resolve_tag_filter_selection(choices, selected_values, default_to_all=default_to_all)
    return (
        gr.update(choices=choices, value=resolved_selection, interactive=True),
        choices,
        resolved_selection,
    )


def _filter_people_for_tag_selection(
    people: Sequence[Dict[str, object]],
    selected_values: Sequence[object] | None,
) -> List[Dict[str, object]]:
    selected_normalized = {
        _normalize_tag(value)
        for value in _normalize_selection(selected_values)
        if _normalize_tag(value)
    }
    all_key = _normalize_tag(TAG_FILTER_ALL_OPTION)
    if all_key in selected_normalized:
        return list(people)

    selected_normalized.discard(all_key)
    if not selected_normalized:
        return []

    filtered_people: List[Dict[str, object]] = []
    for person in people:
        person_tags = {
            _normalize_tag(str(tag))
            for tag in person.get("tags", [])
            if _normalize_tag(str(tag))
        }
        if person_tags.intersection(selected_normalized):
            filtered_people.append(person)
    return filtered_people


def _query_param(request: gr.Request | None, key: str) -> str:
    if request is None:
        return ""
    request_obj = getattr(request, "request", request)
    query_params = getattr(request_obj, "query_params", None)
    if not query_params:
        return ""
    return str(query_params.get(key, "")).strip()


def _render_tag_chips(tags: Sequence[str]) -> str:
    if not tags:
        return '<span class="person-tag person-tag--muted">no-tags</span>'
    parts = []
    for tag in tags:
        safe_tag = html.escape(tag)
        parts.append(f'<span class="person-tag">{safe_tag}</span>')
    return "".join(parts)
