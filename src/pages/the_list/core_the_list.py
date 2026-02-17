from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from typing import Dict, List, Sequence, Tuple

import gradio as gr
from sqlalchemy import text

from src.db import readonly_session_scope, session_scope
from src.people_taxonomy import (
    ensure_people_cards_refs,
    ensure_people_person,
    ensure_people_taxonomy_schema,
    ensure_people_title,
    sync_people_card_taxonomy,
)

timing_logger = logging.getLogger("uvicorn.error")

TAG_FILTER_ALL_OPTION = "All"
DEFAULT_MEDIA_BUCKET = (os.getenv("BUCKET_NAME") or "media-db-dev").strip() or "media-db-dev"
TRUE_VALUES = {"1", "true", "yes", "on"}
SEED_DEMO_DATA = str(os.getenv("THE_LIST_SEED_DEMO_DATA") or "").strip().lower() in TRUE_VALUES
_runtime_bootstrap_default = "0" if os.getenv("INSTANCE_CONNECTION_NAME") else "1"
RUNTIME_SCHEMA_BOOTSTRAP = (
    str(os.getenv("THE_LIST_RUNTIME_SCHEMA_BOOTSTRAP", _runtime_bootstrap_default)).strip().lower() in TRUE_VALUES
)
_DB_INIT_LOCK = threading.Lock()
_DB_INIT_DONE = False

_IMAGE_POOL: Sequence[str] = (
    "/images/Logo.png",
    "/images/Logo_raw.png",
    "/images/Logo_with_text.png",
    "/images/Logo_text.png",
    "/images/The-list-logo.png",
    "/images/The-list-logo2.png",
    "/images/The-list-logo2_old.png",
    "/images/eye.svg",
    "/images/eye-off.svg",
)

_DUMMY_PEOPLE: Sequence[Tuple[str, str, Tuple[str, ...]]] = (
    ("Adrian Holt", "Goalkeeper", ("captain", "shot-stopper", "distribution")),
    ("Bruno Silva", "Goalkeeper", ("reflexes", "1v1", "sweeper-keeper")),
    ("Caleb Nunez", "Defender", ("aggressive", "aerial", "leader")),
    ("Dario Quinn", "Defender", ("left-footed", "crossing", "stamina")),
    ("Eli Archer", "Defender", ("positioning", "calm", "long-passes")),
    ("Fabio Young", "Defender", ("press-resistant", "interceptions", "duels")),
    ("Gavin Porter", "Midfielder", ("box-to-box", "engine", "pressing")),
    ("Hector Doyle", "Midfielder", ("playmaker", "vision", "tempo-control")),
    ("Isaac Bennett", "Midfielder", ("set-pieces", "range", "discipline")),
    ("Jamal Rivers", "Midfielder", ("ball-winner", "coverage", "teamwork")),
    ("Kai Morales", "Midfielder", ("creativity", "through-balls", "control")),
    ("Liam Walker", "Forward", ("finisher", "movement", "off-ball")),
    ("Marco Stone", "Forward", ("dribbler", "pace", "1v1")),
    ("Niko Foster", "Forward", ("target-man", "hold-up", "aerial")),
    ("Owen Price", "Forward", ("poacher", "positioning", "instinct")),
    ("Pablo Reed", "Forward", ("inside-forward", "right-foot", "cut-inside")),
    ("Quentin Hale", "Defender", ("recovery-speed", "timing", "focus")),
    ("Rafael Mendez", "Defender", ("tackles", "marking", "consistency")),
    ("Soren Blake", "Midfielder", ("deep-lying", "distribution", "composure")),
    ("Thiago King", "Midfielder", ("line-breaker", "progressive-passes", "agility")),
    ("Uriel Grant", "Forward", ("counter-attack", "pace", "composure")),
    ("Victor Lane", "Forward", ("left-wing", "crossing", "work-rate")),
    ("Wyatt Green", "Defender", ("overlaps", "stamina", "pressing")),
    ("Xander Shaw", "Midfielder", ("half-spaces", "link-play", "awareness")),
    ("Yasin Clarke", "Forward", ("right-wing", "acceleration", "decision-making")),
    ("Zane Brooks", "Goalkeeper", ("command", "communication", "distribution")),
)


def _log_timing(event_name: str, start: float, **fields: object) -> None:
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    if fields:
        field_text = " ".join(f"{key}={value}" for key, value in fields.items())
        timing_logger.info("the_list.timing event=%s ms=%.2f %s", event_name, elapsed_ms, field_text)
        return
    timing_logger.info("the_list.timing event=%s ms=%.2f", event_name, elapsed_ms)


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower())
    return normalized.strip("-") or "profile"


def _markdown_for_dummy_person(name: str, title: str, tags: Sequence[str], index: int) -> str:
    tags_md = ", ".join(f"`{tag}`" for tag in tags)
    strengths = "\n".join(f"- {tag.replace('-', ' ').title()}" for tag in tags)
    return (
        f"# {name}\n\n"
        "## Snapshot\n"
        f"- **Title:** {title}\n"
        f"- **Tags:** {tags_md}\n"
        f"- **Dummy ID:** P-{index:03d}\n\n"
        "## Background\n"
        f"{name} is a placeholder profile generated for testing card density, click-through navigation, and markdown rendering.\n\n"
        "## Strengths\n"
        f"{strengths}\n\n"
        "## Recent Notes\n"
        "| Match | Result | Notes |\n"
        "|---|---|---|\n"
        f"| Friendly {index} | 2-1 | Created multiple high-value actions |\n"
        f"| Friendly {index + 1} | 1-1 | Strong in transition and shape |\n"
    )


def _ensure_local_db() -> None:
    total_start = time.perf_counter()
    global _DB_INIT_DONE
    if _DB_INIT_DONE:
        _log_timing("ensure_local_db.skip_already_done", total_start)
        return
    lock_wait_start = time.perf_counter()
    with _DB_INIT_LOCK:
        _log_timing("ensure_local_db.lock_wait", lock_wait_start)
        if _DB_INIT_DONE:
            _log_timing("ensure_local_db.skip_already_done_after_lock", total_start)
            return
        if not RUNTIME_SCHEMA_BOOTSTRAP:
            _DB_INIT_DONE = True
            _log_timing("ensure_local_db.bootstrap_disabled", total_start)
            return
        bootstrap_start = time.perf_counter()
        _ensure_local_db_once()
        _log_timing("ensure_local_db.bootstrap_once", bootstrap_start)
        _DB_INIT_DONE = True
    _log_timing("ensure_local_db.total", total_start)


def _ensure_local_db_once() -> None:
    total_start = time.perf_counter()
    timing_logger.info(
        "the_list.timing event=ensure_local_db_once.start runtime_bootstrap=%s seed_demo_data=%s",
        RUNTIME_SCHEMA_BOOTSTRAP,
        SEED_DEMO_DATA,
    )
    session_start = time.perf_counter()
    with session_scope() as session:
        _log_timing("ensure_local_db_once.open_session_scope", session_start)
        step_start = time.perf_counter()
        ensure_people_taxonomy_schema(session)
        _log_timing("ensure_local_db_once.ensure_people_taxonomy_schema", step_start)
        step_start = time.perf_counter()
        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.people_cards (
                    id BIGSERIAL PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    person_id BIGINT NOT NULL REFERENCES app.people(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    title_id BIGINT NOT NULL REFERENCES app.people_titles(id) ON UPDATE CASCADE ON DELETE RESTRICT,
                    bucket TEXT NOT NULL,
                    image_url TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        )
        _log_timing("ensure_local_db_once.create_people_cards", step_start)
        step_start = time.perf_counter()
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_people_cards_bucket ON app.people_cards(bucket)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_people_cards_person_id ON app.people_cards(person_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_people_cards_title_id ON app.people_cards(title_id)"))
        _log_timing("ensure_local_db_once.create_people_cards_indexes", step_start)
        step_start = time.perf_counter()
        ensure_people_cards_refs(session)
        _log_timing("ensure_local_db_once.ensure_people_cards_refs", step_start)
        step_start = time.perf_counter()
        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.people_articles (
                    id BIGSERIAL PRIMARY KEY,
                    person_slug TEXT NOT NULL UNIQUE REFERENCES app.people_cards(slug) ON UPDATE CASCADE ON DELETE CASCADE,
                    markdown TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        )
        _log_timing("ensure_local_db_once.create_people_articles", step_start)
        step_start = time.perf_counter()
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_people_articles_person_slug ON app.people_articles(person_slug)"))
        _log_timing("ensure_local_db_once.create_people_articles_indexes", step_start)

        step_start = time.perf_counter()
        has_legacy_markdown = bool(
            session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'app'
                          AND table_name = 'people_cards'
                          AND column_name = 'markdown'
                    )
                    """
                )
            ).scalar_one()
        )
        _log_timing(
            "ensure_local_db_once.check_legacy_markdown_column",
            step_start,
            has_legacy_markdown=has_legacy_markdown,
        )
        if has_legacy_markdown:
            step_start = time.perf_counter()
            session.execute(
                text(
                    """
                    INSERT INTO app.people_articles (person_slug, markdown)
                    SELECT slug, markdown
                    FROM app.people_cards
                    WHERE markdown IS NOT NULL
                    ON CONFLICT (person_slug) DO NOTHING
                    """
                )
            )
            _log_timing("ensure_local_db_once.copy_legacy_markdown", step_start)

        step_start = time.perf_counter()
        count = int(session.execute(text("SELECT COUNT(1) FROM app.people_cards")).scalar_one())
        _log_timing("ensure_local_db_once.count_people_cards", step_start, people_cards=count)
        if SEED_DEMO_DATA and count == 0:
            step_start = time.perf_counter()
            seed_card_rows: List[Dict[str, object]] = []
            seed_article_rows: List[Dict[str, object]] = []
            seed_taxonomy_rows: List[Dict[str, object]] = []
            for index, (name, title, tags) in enumerate(_DUMMY_PEOPLE, start=1):
                slug = _slugify(name)
                image_url = _IMAGE_POOL[(index - 1) % len(_IMAGE_POOL)]
                markdown = _markdown_for_dummy_person(name, title, tags, index)
                person_id = ensure_people_person(session, name)
                seed_card_rows.append(
                    {
                        "slug": slug,
                        "person_id": person_id,
                        "title_id": ensure_people_title(session, title),
                        "bucket": DEFAULT_MEDIA_BUCKET,
                        "image_url": image_url,
                    }
                )
                seed_taxonomy_rows.append({"person_id": person_id, "title": title, "tags": list(tags)})
                seed_article_rows.append({"person_slug": slug, "markdown": markdown})
            _log_timing(
                "ensure_local_db_once.prepare_seed_rows",
                step_start,
                people=len(seed_card_rows),
            )
            step_start = time.perf_counter()
            session.execute(
                text(
                    """
                    INSERT INTO app.people_cards (slug, person_id, title_id, bucket, image_url)
                    VALUES (:slug, :person_id, :title_id, :bucket, :image_url)
                    ON CONFLICT (slug) DO NOTHING
                    """
                ),
                seed_card_rows,
            )
            _log_timing("ensure_local_db_once.insert_seed_cards", step_start, rows=len(seed_card_rows))
            step_start = time.perf_counter()
            session.execute(
                text(
                    """
                    INSERT INTO app.people_articles (person_slug, markdown)
                    VALUES (:person_slug, :markdown)
                    ON CONFLICT (person_slug) DO NOTHING
                    """
                ),
                seed_article_rows,
            )
            _log_timing("ensure_local_db_once.insert_seed_articles", step_start, rows=len(seed_article_rows))
            step_start = time.perf_counter()
            for row in seed_taxonomy_rows:
                sync_people_card_taxonomy(
                    session,
                    person_id=int(row["person_id"]),
                    title=str(row["title"]),
                    tags=row["tags"],
                )
            _log_timing("ensure_local_db_once.sync_seed_taxonomy", step_start, rows=len(seed_taxonomy_rows))

        step_start = time.perf_counter()
        session.execute(
            text(
                """
                INSERT INTO app.people_articles (person_slug, markdown)
                SELECT
                    c.slug,
                    '# ' || COALESCE(NULLIF(p.name, ''), c.slug) || E'\n\nProfile pending article content.'
                FROM app.people_cards c
                JOIN app.people p
                    ON p.id = c.person_id
                LEFT JOIN app.people_articles a
                    ON a.person_slug = c.slug
                WHERE a.person_slug IS NULL
                ON CONFLICT (person_slug) DO NOTHING
                """
            )
        )
        _log_timing("ensure_local_db_once.ensure_people_articles_defaults", step_start)

        if SEED_DEMO_DATA:
            step_start = time.perf_counter()
            dario_markdown = session.execute(
                text(
                    """
                    SELECT markdown
                    FROM app.people_articles
                    WHERE person_slug = 'dario-quinn'
                    """
                )
            ).scalar_one_or_none()
            _log_timing(
                "ensure_local_db_once.fetch_dario_markdown",
                step_start,
                found=dario_markdown is not None,
            )
            if dario_markdown is not None:
                current_markdown = str(dario_markdown or "")
                marker = "![Profile image preview]"
                if marker not in current_markdown:
                    updated_markdown = (
                        current_markdown
                        + "\n\n## Profile Image\n"
                        + "![Profile image preview](/images/Logo_with_text.png)\n"
                    )
                    step_start = time.perf_counter()
                    session.execute(
                        text(
                            """
                            UPDATE app.people_articles
                            SET markdown = :markdown
                            WHERE person_slug = 'dario-quinn'
                            """
                        ),
                        {"markdown": updated_markdown},
                    )
                    _log_timing("ensure_local_db_once.patch_dario_markdown", step_start)
    _log_timing("ensure_local_db_once.total", total_start)

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
    total_start = time.perf_counter()
    ensure_start = time.perf_counter()
    _ensure_local_db()
    _log_timing("fetch_all_people.ensure_local_db", ensure_start)
    query_start = time.perf_counter()
    with readonly_session_scope() as session:
        rows = session.execute(
            text(
                """
                WITH person_tags AS (
                    SELECT
                        dedup.person_id,
                        json_agg(dedup.label ORDER BY dedup.label)::text AS tags_json
                    FROM (
                        SELECT DISTINCT
                            ppt.person_id,
                            tg.label
                        FROM app.people_person_tags ppt
                        JOIN app.people_tags tg
                            ON tg.id = ppt.tag_id
                    ) AS dedup
                    GROUP BY dedup.person_id
                )
                SELECT
                    c.slug,
                    p.name,
                    COALESCE(t.label, 'Unassigned') AS title,
                    c.bucket,
                    c.image_url,
                    EXTRACT(EPOCH FROM COALESCE(c.updated_at, CURRENT_TIMESTAMP))::bigint AS image_version,
                    COALESCE(pt.tags_json, '[]') AS tags_json
                FROM app.people_cards c
                JOIN app.people p
                    ON p.id = c.person_id
                LEFT JOIN app.people_titles t
                    ON t.id = c.title_id
                LEFT JOIN person_tags pt
                    ON pt.person_id = c.person_id
                ORDER BY p.name
                """
            )
        ).mappings().all()
    _log_timing("fetch_all_people.query_rows", query_start, rows=len(rows))

    people: List[Dict[str, object]] = []
    decode_start = time.perf_counter()
    for row in rows:
        people.append(
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
    _log_timing("fetch_all_people.decode_rows", decode_start, people=len(people))
    _log_timing("fetch_all_people.total", total_start, people=len(people))
    return people


def _normalize_tag(value: str) -> str:
    return str(value or "").strip().lower()


def _choice_values(choices: Sequence[object]) -> List[str]:
    values: List[str] = []
    for choice in choices or []:
        if isinstance(choice, (tuple, list)) and len(choice) >= 2:
            raw_value = choice[1]
        else:
            raw_value = choice
        text = str(raw_value or "").strip()
        if text and text not in values:
            values.append(text)
    return values


def _normalize_selection(values: Sequence[object] | None) -> List[str]:
    normalized_values: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        text = str(value or "").strip()
        if not text:
            continue
        normalized = _normalize_tag(text)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(text)
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
