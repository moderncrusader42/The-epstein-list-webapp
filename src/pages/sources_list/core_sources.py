from __future__ import annotations

import html
import base64
import binascii
import json
import logging
import mimetypes
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple
from urllib.parse import quote, urlparse
from uuid import uuid4

import gradio as gr
from sqlalchemy import text

from src.db import readonly_session_scope, session_scope
from src.employees import ensure_user
from src.gcs_storage import bucket_name as configured_bucket_name
from src.gcs_storage import get_bucket, media_path
from src.login_logic import get_user
from src.pages.people_display.core_people import _render_article_markdown as _render_citation_compiled_markdown

logger = logging.getLogger(__name__)

TAG_FILTER_ALL_OPTION = "All"
CATALOG_VIEW_ICONS = "icons"
CATALOG_VIEW_LIST = "list"
FILE_VIEW_ICONS = "icons"
FILE_VIEW_LIST = "list"
MARKDOWN_VIEW_RAW = "raw"
MARKDOWN_VIEW_PREVIEW = "preview"
DEFAULT_MARKDOWN_VIEW = MARKDOWN_VIEW_PREVIEW
PROPOSAL_SCOPE_SOURCE = "source"

DEFAULT_SOURCE_MAX_BYTES = 1 * 1024 * 1024 * 1024
DEFAULT_SOURCE_MAX_MB = DEFAULT_SOURCE_MAX_BYTES // (1024 * 1024)
DEFAULT_SOURCE_BUCKET = (os.getenv("BUCKET_NAME") or configured_bucket_name() or "media-db-dev").strip() or "media-db-dev"
SOURCES_MEDIA_PREFIX = (os.getenv("SOURCES_MEDIA_PREFIX") or "sources").strip("/ ")

TRUE_VALUES = {"1", "true", "yes", "on"}
_runtime_bootstrap_default = "0" if os.getenv("INSTANCE_CONNECTION_NAME") else "1"
RUNTIME_SCHEMA_BOOTSTRAP = (
    str(os.getenv("THE_LIST_RUNTIME_SCHEMA_BOOTSTRAP", _runtime_bootstrap_default)).strip().lower() in TRUE_VALUES
)

_DB_INIT_LOCK = threading.Lock()
_DB_INIT_DONE = False

SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
MAX_COVER_IMAGE_BYTES = 8 * 1024 * 1024
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
IMAGE_CONTENT_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}
ALLOWED_IMAGE_MIME_TYPES = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
DATA_URL_IMAGE_RE = re.compile(r"^data:(image/[A-Za-z0-9.+-]+);base64,([A-Za-z0-9+/=\s]+)$", re.IGNORECASE)
ACTION_CREATE_NEW_SOURCE = "create_new_source"
UPLOAD_ORIGIN_KEY_PREFIX = "upload::"
UNSORTED_ORIGIN_KEY_PREFIX = "unsorted::"


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in TRUE_VALUES


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower())
    return normalized.strip("-") or "source"


def _normalize_tag(value: str) -> str:
    return str(value or "").strip().lower()


def _sanitize_filename(name: str) -> str:
    cleaned = SAFE_FILENAME_RE.sub("-", str(name or "").strip())
    cleaned = cleaned.strip(" .-")
    if not cleaned:
        return ""
    return cleaned[:180]


def _parse_tags_input(raw_value: object) -> List[str]:
    tags: List[str] = []
    seen: set[str] = set()
    for chunk in re.split(r"[,\n]+", str(raw_value or "")):
        normalized = _normalize_tag(chunk)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        tags.append(normalized)
    return tags


def _parse_source_file_ids(raw_value: object) -> List[int]:
    values: List[object]
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            values = []
        elif stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                return _parse_source_file_ids(parsed)
            values = []
        else:
            values = [part for part in re.split(r"[,\s]+", stripped) if part]
    elif isinstance(raw_value, (list, tuple, set)):
        values = list(raw_value)
    elif raw_value in (None, ""):
        values = []
    else:
        values = [raw_value]

    parsed_ids: List[int] = []
    seen_ids: set[int] = set()
    for value in values:
        try:
            parsed = int(str(value or "").strip())
        except (TypeError, ValueError):
            continue
        if parsed <= 0 or parsed in seen_ids:
            continue
        seen_ids.add(parsed)
        parsed_ids.append(parsed)
    return parsed_ids


def _parse_unsorted_file_ids(raw_value: object) -> List[int]:
    return _parse_source_file_ids(raw_value)


def _origin_key_for_uploaded_path(path_obj: Path) -> str:
    try:
        resolved = str(path_obj.resolve())
    except Exception:
        resolved = str(path_obj)
    return f"{UPLOAD_ORIGIN_KEY_PREFIX}{resolved}"


def _origin_key_for_unsorted_file(file_id: int) -> str:
    return f"{UNSORTED_ORIGIN_KEY_PREFIX}{int(file_id)}"


def _parse_unsorted_id_from_origin_key(origin_key: str) -> int:
    raw_key = str(origin_key or "").strip()
    if not raw_key.startswith(UNSORTED_ORIGIN_KEY_PREFIX):
        return 0
    try:
        parsed = int(raw_key[len(UNSORTED_ORIGIN_KEY_PREFIX) :])
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _parse_origin_key_set(raw_value: object) -> set[str]:
    if raw_value in (None, ""):
        return set()

    candidates: List[object]
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return set()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = []
            candidates = parsed if isinstance(parsed, list) else []
        else:
            candidates = [part for part in re.split(r"[,\s]+", stripped) if str(part or "").strip()]
    elif isinstance(raw_value, (list, tuple, set)):
        candidates = list(raw_value)
    else:
        candidates = [raw_value]

    keys: set[str] = set()
    for candidate in candidates:
        key = str(candidate or "").strip()
        if key:
            keys.add(key)
    return keys


def _parse_file_origins_input(raw_value: object) -> List[str]:
    values_attr = getattr(raw_value, "values", None)
    if values_attr is not None and hasattr(values_attr, "tolist"):
        try:
            converted_values = values_attr.tolist()
        except Exception:
            converted_values = None
        if isinstance(converted_values, list):
            return _parse_file_origins_input(converted_values)

    tolist_fn = getattr(raw_value, "tolist", None)
    if callable(tolist_fn):
        try:
            converted_rows = tolist_fn()
        except Exception:
            converted_rows = None
        if isinstance(converted_rows, list):
            return _parse_file_origins_input(converted_rows)

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return _parse_file_origins_input(parsed)
        return [
            str(candidate or "").strip()
            for candidate in str(raw_value or "").splitlines()
            if str(candidate or "").strip()
        ]

    if isinstance(raw_value, (list, tuple)):
        candidates = list(raw_value)
        if candidates and all(isinstance(row, (list, tuple)) for row in candidates):
            origins: List[str] = []
            for row in candidates:
                if len(row) >= 2:
                    origins.append(str(row[1] or "").strip())
                elif len(row) == 1:
                    origins.append(str(row[0] or "").strip())
                else:
                    origins.append("")
            return origins
        return [str(candidate or "").strip() for candidate in candidates]

    return []


def _resolve_file_origins_for_upload(file_paths: Sequence[Path], raw_origins: object) -> List[str]:
    if not file_paths:
        return []

    origin_values = _parse_file_origins_input(raw_origins)
    expected_count = len(file_paths)
    if len(origin_values) > expected_count:
        overflow_values = origin_values[expected_count:]
        if any(str(value or "").strip() for value in overflow_values):
            raise ValueError(
                "Too many Origin/Url rows were provided. "
                f"Expected {expected_count}, received {len(origin_values)}."
            )
        origin_values = origin_values[:expected_count]
    if len(origin_values) != expected_count:
        raise ValueError(
            "Provide one Origin/Url per uploaded file "
            f"({expected_count} required, {len(origin_values)} provided)."
        )
    return origin_values


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


def _effective_description_markdown(raw_description: object, raw_legacy_summary: object) -> str:
    description_value = str(raw_description or "").strip()
    if description_value:
        return description_value
    return str(raw_legacy_summary or "").strip()


def _description_preview_text(markdown: object, max_chars: int = 180) -> str:
    text_value = str(markdown or "").replace("\r\n", "\n").strip()
    if not text_value:
        return ""

    text_value = re.sub(r"\\(?:cite|bib)\{[^{}\n]*\}", " ", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"!\[[^\]]*]\([^)]+\)", " ", text_value)
    text_value = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text_value)
    text_value = re.sub(r"^\s{0,3}#{1,6}\s*", "", text_value, flags=re.MULTILINE)
    text_value = re.sub(r"^\s{0,3}[-*+]\s+", "", text_value, flags=re.MULTILINE)
    text_value = re.sub(r"^\s{0,3}\d+[.)]\s+", "", text_value, flags=re.MULTILINE)
    text_value = re.sub(r"^\s{0,3}>\s?", "", text_value, flags=re.MULTILINE)
    text_value = re.sub(r"\s*`{1,3}([^`]+)`{1,3}\s*", r" \1 ", text_value)
    text_value = re.sub(r"[*_~]", "", text_value)
    text_value = re.sub(r"\[[0-9]{1,4}\]\s*:\s*.+$", " ", text_value, flags=re.MULTILINE)
    text_value = re.sub(r"\s+", " ", text_value).strip()
    if len(text_value) <= max_chars:
        return text_value
    return f"{text_value[:max_chars].rstrip()}..."


def _format_bytes(size_bytes: int) -> str:
    value = float(max(0, int(size_bytes or 0)))
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for next_unit in units:
        unit = next_unit
        if value < 1024.0 or next_unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.1f} {unit}"


def _normalize_catalog_view_mode(raw_value: object) -> str:
    value = str(raw_value or "").strip().lower()
    if value == CATALOG_VIEW_LIST:
        return CATALOG_VIEW_LIST
    return CATALOG_VIEW_ICONS


def _normalize_file_view_mode(raw_value: object) -> str:
    value = str(raw_value or "").strip().lower()
    if value == FILE_VIEW_LIST:
        return FILE_VIEW_LIST
    return FILE_VIEW_ICONS


def _query_param(request: gr.Request | None, key: str) -> str:
    if request is None:
        return ""
    request_obj = getattr(request, "request", request)
    query_params = getattr(request_obj, "query_params", None)
    if not query_params:
        return ""
    return str(query_params.get(key, "")).strip()


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


def _build_unsorted_origin_display_label(file_id: int, file_name: str) -> str:
    safe_name = str(file_name or "").strip() or f"file-{file_id}"
    return f"[Unsorted #{int(file_id)}] {safe_name}"


def _fetch_unsorted_files_for_create() -> List[Dict[str, object]]:
    _ensure_sources_db()
    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "unsorted_files"):
            return []

        rows = session.execute(
            text(
                """
                SELECT
                    uf.id,
                    uf.bucket,
                    uf.blob_path,
                    uf.file_name,
                    uf.origin_text,
                    uf.mime_type,
                    uf.size_bytes,
                    uf.created_at
                FROM app.unsorted_files uf
                ORDER BY uf.created_at DESC, uf.id DESC
                """
            )
        ).mappings().all()

    files: List[Dict[str, object]] = []
    for row in rows:
        file_id = int(row["id"] or 0)
        if file_id <= 0:
            continue
        files.append(
            {
                "id": file_id,
                "bucket": str(row["bucket"] or "").strip() or DEFAULT_SOURCE_BUCKET,
                "blob_path": str(row["blob_path"] or "").strip().lstrip("/"),
                "file_name": str(row["file_name"] or "").strip(),
                "origin_text": str(row["origin_text"] or "").strip(),
                "mime_type": str(row["mime_type"] or "").strip().lower(),
                "size_bytes": max(0, int(row["size_bytes"] or 0)),
                "created_at": row.get("created_at"),
            }
        )
    return files


def _build_source_create_unsorted_choices(unsorted_files: Sequence[Dict[str, object]]) -> List[Tuple[str, str]]:
    choices: List[Tuple[str, str]] = []
    for row in unsorted_files:
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            continue
        file_name = str(row.get("file_name") or "").strip() or f"file-{file_id}"
        size_label = _format_bytes(int(row.get("size_bytes") or 0))
        created_at = row.get("created_at")
        if isinstance(created_at, datetime):
            created_label = created_at.strftime("%Y-%m-%d")
        else:
            created_label = ""
        date_suffix = f" - {created_label}" if created_label else ""
        label = f"[#{file_id}] {file_name} ({size_label}{date_suffix})"
        choices.append((label, str(file_id)))
    return choices


def _serialize_source_create_unsorted_catalog(unsorted_files: Sequence[Dict[str, object]]) -> str:
    payload: List[Dict[str, object]] = []
    for row in unsorted_files:
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            continue
        payload.append(
            {
                "id": file_id,
                "bucket": str(row.get("bucket") or "").strip() or DEFAULT_SOURCE_BUCKET,
                "blob_path": str(row.get("blob_path") or "").strip().lstrip("/"),
                "file_name": str(row.get("file_name") or "").strip(),
                "origin_text": str(row.get("origin_text") or "").strip(),
                "mime_type": str(row.get("mime_type") or "").strip().lower(),
                "size_bytes": max(0, int(row.get("size_bytes") or 0)),
            }
        )
    return json.dumps(payload, ensure_ascii=True)


def _parse_source_create_unsorted_catalog(raw_value: object) -> List[Dict[str, object]]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        candidates = raw_value
    else:
        text_value = str(raw_value or "").strip()
        if not text_value:
            return []
        try:
            parsed = json.loads(text_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        candidates = parsed

    rows: List[Dict[str, object]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        file_id = int(candidate.get("id") or 0)
        if file_id <= 0:
            continue
        rows.append(
            {
                "id": file_id,
                "bucket": str(candidate.get("bucket") or "").strip() or DEFAULT_SOURCE_BUCKET,
                "blob_path": str(candidate.get("blob_path") or "").strip().lstrip("/"),
                "file_name": str(candidate.get("file_name") or "").strip(),
                "origin_text": str(candidate.get("origin_text") or "").strip(),
                "mime_type": str(candidate.get("mime_type") or "").strip().lower(),
                "size_bytes": max(0, int(candidate.get("size_bytes") or 0)),
            }
        )
    return rows


def _source_create_unsorted_picker_state(request: gr.Request | None) -> tuple[List[Tuple[str, str]], List[str], str]:
    all_unsorted_files = _fetch_unsorted_files_for_create()
    choice_rows = _build_source_create_unsorted_choices(all_unsorted_files)
    catalog_json = _serialize_source_create_unsorted_catalog(all_unsorted_files)

    requested_ids = _parse_unsorted_file_ids(_query_param(request, "from_unsorted"))
    available_ids = {int(row.get("id") or 0) for row in all_unsorted_files}
    selected_values = [str(file_id) for file_id in requested_ids if file_id in available_ids]
    return choice_rows, selected_values, catalog_json


def _ensure_sources_db() -> None:
    global _DB_INIT_DONE
    if _DB_INIT_DONE:
        return
    with _DB_INIT_LOCK:
        if _DB_INIT_DONE:
            return
        if not RUNTIME_SCHEMA_BOOTSTRAP:
            _DB_INIT_DONE = True
            return
        _ensure_sources_db_once()
        _DB_INIT_DONE = True


def _ensure_sources_db_once() -> None:
    with session_scope() as session:
        session.execute(text("CREATE SCHEMA IF NOT EXISTS app"))

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_cards (
                    id BIGSERIAL PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    description_markdown TEXT NOT NULL DEFAULT '',
                    bucket TEXT NOT NULL,
                    folder_prefix TEXT NOT NULL UNIQUE,
                    cover_media_url TEXT NOT NULL DEFAULT '',
                    max_bytes BIGINT NOT NULL DEFAULT 1073741824,
                    created_by_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT chk_sources_cards_max_bytes CHECK (max_bytes > 0)
                )
                """
            )
        )
        session.execute(
            text("ALTER TABLE app.sources_cards ADD COLUMN IF NOT EXISTS description_markdown TEXT NOT NULL DEFAULT ''")
        )
        has_legacy_summary_column = bool(
            session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'app'
                          AND table_name = 'sources_cards'
                          AND column_name = 'summary'
                    )
                    """
                )
            ).scalar_one()
        )
        if has_legacy_summary_column:
            # Legacy support: carry forward old summary text into the markdown description field.
            session.execute(
                text(
                    """
                    UPDATE app.sources_cards
                    SET description_markdown = summary
                    WHERE COALESCE(BTRIM(description_markdown), '') = ''
                      AND COALESCE(BTRIM(summary), '') <> ''
                    """
                )
            )
        session.execute(
            text("ALTER TABLE app.sources_cards ADD COLUMN IF NOT EXISTS cover_media_url TEXT NOT NULL DEFAULT ''")
        )
        session.execute(
            text(
                "ALTER TABLE app.sources_cards "
                "ADD COLUMN IF NOT EXISTS max_bytes BIGINT NOT NULL DEFAULT 1073741824"
            )
        )
        session.execute(
            text(
                "ALTER TABLE app.sources_cards "
                "ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT REFERENCES app.\"user\"(id) "
                "ON UPDATE CASCADE ON DELETE SET NULL"
            )
        )
        session.execute(text("ALTER TABLE app.sources_cards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"))

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_tags (
                    id BIGSERIAL PRIMARY KEY,
                    code TEXT NOT NULL,
                    label TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        )

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_card_tags (
                    source_id BIGINT NOT NULL REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    tag_id BIGINT NOT NULL REFERENCES app.sources_tags(id) ON UPDATE CASCADE ON DELETE RESTRICT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (source_id, tag_id)
                )
                """
            )
        )

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_files (
                    id BIGSERIAL PRIMARY KEY,
                    source_id BIGINT NOT NULL REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    blob_path TEXT NOT NULL UNIQUE,
                    file_name TEXT NOT NULL,
                    origin_url TEXT NOT NULL DEFAULT '',
                    mime_type TEXT,
                    size_bytes BIGINT NOT NULL DEFAULT 0,
                    uploaded_by_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT chk_sources_files_size_bytes CHECK (size_bytes >= 0)
                )
                """
            )
        )
        session.execute(text("ALTER TABLE app.sources_files ADD COLUMN IF NOT EXISTS origin_url TEXT NOT NULL DEFAULT ''"))

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_change_proposals (
                    id BIGSERIAL PRIMARY KEY,
                    source_slug TEXT NOT NULL,
                    source_id BIGINT REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    proposer_user_id BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
                    proposal_scope TEXT NOT NULL DEFAULT 'source',
                    base_payload TEXT NOT NULL DEFAULT '',
                    proposed_payload TEXT NOT NULL DEFAULT '',
                    note TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    reviewed_at TIMESTAMPTZ,
                    reviewer_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
                    review_note TEXT,
                    report_triggered INTEGER NOT NULL DEFAULT 0,
                    CONSTRAINT chk_sources_change_scope CHECK (lower(proposal_scope) IN ('source')),
                    CONSTRAINT chk_sources_change_status CHECK (lower(status) IN ('pending', 'accepted', 'declined', 'reported')),
                    CONSTRAINT chk_sources_change_report_triggered CHECK (report_triggered IN (0, 1))
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.sources_change_events (
                    id BIGSERIAL PRIMARY KEY,
                    proposal_id BIGINT NOT NULL REFERENCES app.sources_change_proposals(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    actor_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    notes TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        )

        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_cards_name ON app.sources_cards(name)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_cards_bucket ON app.sources_cards(bucket)"))
        session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_sources_cards_folder_prefix ON app.sources_cards(folder_prefix)")
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_cards_created_by_user_id "
                "ON app.sources_cards(created_by_user_id)"
            )
        )
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_tags_code ON app.sources_tags(code)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_card_tags_tag_id ON app.sources_card_tags(tag_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_files_source_id ON app.sources_files(source_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_sources_files_created_at ON app.sources_files(created_at)"))
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_files_uploaded_by_user_id "
                "ON app.sources_files(uploaded_by_user_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_source_slug "
                "ON app.sources_change_proposals(source_slug)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_source_id "
                "ON app.sources_change_proposals(source_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_status "
                "ON app.sources_change_proposals(status)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_created_at "
                "ON app.sources_change_proposals(created_at)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_proposer_user_id "
                "ON app.sources_change_proposals(proposer_user_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_proposals_reviewer_user_id "
                "ON app.sources_change_proposals(reviewer_user_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_sources_change_events_proposal "
                "ON app.sources_change_events(proposal_id)"
            )
        )


def _table_exists_in_app_schema(session, table_name: str) -> bool:
    rel_name = f"app.{str(table_name or '').strip()}"
    if rel_name == "app.":
        return False
    return bool(
        session.execute(
            text("SELECT to_regclass(:rel_name) IS NOT NULL"),
            {"rel_name": rel_name},
        ).scalar_one()
    )


def _column_exists_in_app_schema(session, table_name: str, column_name: str) -> bool:
    return bool(
        session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'app'
                      AND table_name = :table_name
                      AND column_name = :column_name
                )
                """
            ),
            {
                "table_name": str(table_name or "").strip(),
                "column_name": str(column_name or "").strip(),
            },
        ).scalar_one()
    )


def _fetch_all_sources() -> List[Dict[str, object]]:
    _ensure_sources_db()
    with readonly_session_scope() as session:
        has_cards_table = _table_exists_in_app_schema(session, "sources_cards")
        if not has_cards_table:
            return []

        has_legacy_summary_column = _column_exists_in_app_schema(session, "sources_cards", "summary")
        has_tags_join_table = _table_exists_in_app_schema(session, "sources_card_tags")
        has_tags_table = _table_exists_in_app_schema(session, "sources_tags")
        has_files_table = _table_exists_in_app_schema(session, "sources_files")

        source_tags_cte = (
            """
            source_tags AS (
                SELECT
                    dedup.source_id,
                    json_agg(dedup.label ORDER BY dedup.label)::text AS tags_json
                FROM (
                    SELECT DISTINCT
                        sct.source_id,
                        st.label
                    FROM app.sources_card_tags sct
                    JOIN app.sources_tags st
                        ON st.id = sct.tag_id
                ) AS dedup
                GROUP BY dedup.source_id
            )
            """
            if has_tags_join_table and has_tags_table
            else """
            source_tags AS (
                SELECT
                    NULL::bigint AS source_id,
                    '[]'::text AS tags_json
                WHERE FALSE
            )
            """
        )

        file_stats_cte = (
            """
            file_stats AS (
                SELECT
                    sf.source_id,
                    COUNT(*)::bigint AS file_count,
                    COALESCE(SUM(sf.size_bytes), 0)::bigint AS total_bytes
                FROM app.sources_files sf
                GROUP BY sf.source_id
            )
            """
            if has_files_table
            else """
            file_stats AS (
                SELECT
                    NULL::bigint AS source_id,
                    0::bigint AS file_count,
                    0::bigint AS total_bytes
                WHERE FALSE
            )
            """
        )

        latest_file_cte = (
            """
            latest_file AS (
                SELECT DISTINCT ON (sf.source_id)
                    sf.source_id,
                    sf.blob_path,
                    COALESCE(sf.mime_type, '') AS mime_type
                FROM app.sources_files sf
                ORDER BY sf.source_id, sf.created_at DESC, sf.id DESC
            )
            """
            if has_files_table
            else """
            latest_file AS (
                SELECT
                    NULL::bigint AS source_id,
                    ''::text AS blob_path,
                    ''::text AS mime_type
                WHERE FALSE
            )
            """
        )

        legacy_summary_select = "COALESCE(c.summary, '') AS legacy_summary," if has_legacy_summary_column else "''::text AS legacy_summary,"

        rows = session.execute(
            text(
                f"""
                WITH
                {source_tags_cte},
                {file_stats_cte},
                {latest_file_cte}
                SELECT
                    c.id,
                    c.slug,
                    c.name,
                    {legacy_summary_select}
                    COALESCE(c.description_markdown, '') AS description_markdown,
                    c.bucket,
                    c.folder_prefix,
                    c.cover_media_url,
                    c.max_bytes,
                    COALESCE(st.tags_json, '[]') AS tags_json,
                    COALESCE(fs.file_count, 0)::bigint AS file_count,
                    COALESCE(fs.total_bytes, 0)::bigint AS total_bytes,
                    COALESCE(lf.blob_path, '') AS preview_blob_path,
                    COALESCE(lf.mime_type, '') AS preview_mime
                FROM app.sources_cards c
                LEFT JOIN source_tags st
                    ON st.source_id = c.id
                LEFT JOIN file_stats fs
                    ON fs.source_id = c.id
                LEFT JOIN latest_file lf
                    ON lf.source_id = c.id
                ORDER BY lower(c.name), c.id
                """
            )
        ).mappings().all()

    sources: List[Dict[str, object]] = []
    for row in rows:
        max_bytes = max(1, int(row["max_bytes"] or DEFAULT_SOURCE_MAX_BYTES))
        total_bytes = max(0, int(row["total_bytes"] or 0))
        description_markdown = _effective_description_markdown(
            row.get("description_markdown"),
            row.get("legacy_summary"),
        )
        cover_media_url = str(row["cover_media_url"] or "").strip()
        preview_blob_path = str(row["preview_blob_path"] or "").strip()
        preview_url = _resolve_media_reference(cover_media_url)
        preview_mime = ""
        if not preview_url and preview_blob_path:
            preview_url = media_path(preview_blob_path)
            preview_mime = str(row["preview_mime"] or "").strip()

        sources.append(
            {
                "id": int(row["id"]),
                "slug": str(row["slug"] or "").strip().lower(),
                "name": str(row["name"] or "").strip(),
                "summary": _description_preview_text(description_markdown),
                "description_markdown": description_markdown,
                "bucket": str(row["bucket"] or "").strip(),
                "folder_prefix": str(row["folder_prefix"] or "").strip(),
                "cover_media_url": cover_media_url,
                "preview_url": preview_url,
                "preview_mime": preview_mime,
                "max_bytes": max_bytes,
                "file_count": int(row["file_count"] or 0),
                "total_bytes": total_bytes,
                "usage_pct": min(100.0, (float(total_bytes) / float(max_bytes)) * 100.0),
                "tags": _decode_tags(row["tags_json"]),
            }
        )
    return sources


def _fetch_source_and_files(source_slug: str, limit: int = 400) -> tuple[Dict[str, object] | None, List[Dict[str, object]]]:
    normalized_slug = str(source_slug or "").strip().lower()
    if not normalized_slug:
        return None, []

    _ensure_sources_db()
    with readonly_session_scope() as session:
        has_cards_table = _table_exists_in_app_schema(session, "sources_cards")
        if not has_cards_table:
            return None, []

        has_legacy_summary_column = _column_exists_in_app_schema(session, "sources_cards", "summary")
        has_tags_join_table = _table_exists_in_app_schema(session, "sources_card_tags")
        has_tags_table = _table_exists_in_app_schema(session, "sources_tags")
        has_files_table = _table_exists_in_app_schema(session, "sources_files")

        tag_rows_cte = (
            """
            tag_rows AS (
                SELECT
                    sct.source_id,
                    json_agg(st.label ORDER BY st.label)::text AS tags_json
                FROM app.sources_card_tags sct
                JOIN app.sources_tags st
                    ON st.id = sct.tag_id
                GROUP BY sct.source_id
            )
            """
            if has_tags_join_table and has_tags_table
            else """
            tag_rows AS (
                SELECT
                    NULL::bigint AS source_id,
                    '[]'::text AS tags_json
                WHERE FALSE
            )
            """
        )

        file_stats_cte = (
            """
            file_stats AS (
                SELECT
                    source_id,
                    COUNT(*)::bigint AS file_count,
                    COALESCE(SUM(size_bytes), 0)::bigint AS total_bytes
                FROM app.sources_files
                GROUP BY source_id
            )
            """
            if has_files_table
            else """
            file_stats AS (
                SELECT
                    NULL::bigint AS source_id,
                    0::bigint AS file_count,
                    0::bigint AS total_bytes
                WHERE FALSE
            )
            """
        )

        legacy_summary_select = "COALESCE(c.summary, '') AS legacy_summary," if has_legacy_summary_column else "''::text AS legacy_summary,"

        source_row = session.execute(
            text(
                f"""
                WITH
                {tag_rows_cte},
                {file_stats_cte}
                SELECT
                    c.id,
                    c.slug,
                    c.name,
                    {legacy_summary_select}
                    COALESCE(c.description_markdown, '') AS description_markdown,
                    c.cover_media_url,
                    c.bucket,
                    c.folder_prefix,
                    c.max_bytes,
                    COALESCE(tr.tags_json, '[]') AS tags_json,
                    COALESCE(fs.file_count, 0)::bigint AS file_count,
                    COALESCE(fs.total_bytes, 0)::bigint AS total_bytes
                FROM app.sources_cards c
                LEFT JOIN tag_rows tr
                    ON tr.source_id = c.id
                LEFT JOIN file_stats fs
                    ON fs.source_id = c.id
                WHERE c.slug = :slug
                LIMIT 1
                """
            ),
            {"slug": normalized_slug},
        ).mappings().one_or_none()

        if source_row is None:
            return None, []

        if has_files_table:
            file_rows = session.execute(
                text(
                    """
                    SELECT
                        sf.id,
                        sf.blob_path,
                        sf.file_name,
                        COALESCE(sf.origin_url, '') AS origin_url,
                        COALESCE(sf.mime_type, '') AS mime_type,
                        COALESCE(sf.size_bytes, 0)::bigint AS size_bytes,
                        sf.created_at
                    FROM app.sources_files sf
                    WHERE sf.source_id = :source_id
                    ORDER BY sf.created_at DESC, sf.id DESC
                    LIMIT :row_limit
                    """
                ),
                {
                    "source_id": int(source_row["id"]),
                    "row_limit": int(max(1, limit)),
                },
            ).mappings().all()
        else:
            file_rows = []

    description_markdown = _effective_description_markdown(
        source_row.get("description_markdown"),
        source_row.get("legacy_summary"),
    )

    source: Dict[str, object] = {
        "id": int(source_row["id"]),
        "slug": str(source_row["slug"] or "").strip().lower(),
        "name": str(source_row["name"] or "").strip(),
        "summary": _description_preview_text(description_markdown),
        "description_markdown": description_markdown,
        "cover_media_url": str(source_row["cover_media_url"] or "").strip(),
        "bucket": str(source_row["bucket"] or "").strip(),
        "folder_prefix": str(source_row["folder_prefix"] or "").strip(),
        "max_bytes": int(source_row["max_bytes"] or DEFAULT_SOURCE_MAX_BYTES),
        "file_count": int(source_row["file_count"] or 0),
        "total_bytes": int(source_row["total_bytes"] or 0),
        "tags": _decode_tags(source_row["tags_json"]),
    }

    files: List[Dict[str, object]] = []
    for row in file_rows:
        blob_path = str(row["blob_path"] or "").strip()
        files.append(
            {
                "id": int(row["id"]),
                "blob_path": blob_path,
                "file_name": str(row["file_name"] or "").strip() or blob_path,
                "origin_url": str(row["origin_url"] or "").strip(),
                "mime_type": str(row["mime_type"] or "").strip(),
                "size_bytes": int(row["size_bytes"] or 0),
                "created_at": row["created_at"],
                "media_url": media_path(blob_path),
            }
        )

    return source, files


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


def _build_tag_filter_choices(sources: Sequence[Dict[str, object]]) -> List[Tuple[str, str]]:
    unique_tags: set[str] = set()
    for source in sources:
        for raw_tag in source.get("tags", []):
            normalized_tag = _normalize_tag(str(raw_tag))
            if normalized_tag:
                unique_tags.add(normalized_tag)

    choices: List[Tuple[str, str]] = [(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)]
    choices.extend((tag, tag) for tag in sorted(unique_tags))
    return choices


def _resolve_tag_filter_selection(
    choices: Sequence[object],
    selected_values: Sequence[object] | None,
    *,
    default_to_all: bool,
) -> List[str]:
    all_values = _choice_values(choices)
    if not all_values:
        return []

    all_key = _normalize_tag(TAG_FILTER_ALL_OPTION)
    allowed_values = [value for value in all_values if _normalize_tag(value) != all_key]
    if not allowed_values:
        return []

    selected = _normalize_selection(selected_values)
    if not selected:
        return [TAG_FILTER_ALL_OPTION, *allowed_values] if default_to_all else []

    selected_normalized = {_normalize_tag(value) for value in selected}
    filtered_values = [value for value in allowed_values if _normalize_tag(value) in selected_normalized]
    has_all = all_key in selected_normalized

    if has_all:
        if not filtered_values or len(filtered_values) == len(allowed_values):
            return [TAG_FILTER_ALL_OPTION, *allowed_values]
        return filtered_values

    if not filtered_values:
        return [TAG_FILTER_ALL_OPTION, *allowed_values] if default_to_all else []

    if len(filtered_values) == len(allowed_values):
        return [TAG_FILTER_ALL_OPTION, *allowed_values]
    return filtered_values


def _build_tag_filter_update(
    sources: Sequence[Dict[str, object]],
    selected_values: Sequence[object] | None = None,
    *,
    default_to_all: bool = True,
) -> tuple[gr.update, List[Tuple[str, str]], List[str]]:
    choices = _build_tag_filter_choices(sources)
    resolved_selection = _resolve_tag_filter_selection(choices, selected_values, default_to_all=default_to_all)
    return (
        gr.update(choices=choices, value=resolved_selection, interactive=True),
        choices,
        resolved_selection,
    )


def _filter_sources_for_tag_selection(
    sources: Sequence[Dict[str, object]],
    selected_values: Sequence[object] | None,
) -> List[Dict[str, object]]:
    selected_normalized = {
        _normalize_tag(value)
        for value in _normalize_selection(selected_values)
        if _normalize_tag(value)
    }
    all_key = _normalize_tag(TAG_FILTER_ALL_OPTION)
    if all_key in selected_normalized:
        return list(sources)

    selected_normalized.discard(all_key)
    if not selected_normalized:
        return []

    filtered_sources: List[Dict[str, object]] = []
    for source in sources:
        source_tags = {
            _normalize_tag(str(tag))
            for tag in source.get("tags", [])
            if _normalize_tag(str(tag))
        }
        if source_tags.intersection(selected_normalized):
            filtered_sources.append(source)
    return filtered_sources


def _resolve_mime_type(raw_mime: object, file_name: object, media_url: object) -> str:
    mime_value = str(raw_mime or "").strip().lower()
    if mime_value:
        return mime_value

    file_name_text = str(file_name or "").strip()
    guessed = mimetypes.guess_type(file_name_text)[0] if file_name_text else None
    if guessed:
        return guessed.lower()

    media_text = str(media_url or "").strip()
    guessed = mimetypes.guess_type(media_text)[0] if media_text else None
    return str(guessed or "").strip().lower()


def _resolve_media_reference(raw_value: object) -> str:
    text_value = str(raw_value or "").strip()
    if not text_value:
        return ""

    parsed = urlparse(text_value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return text_value
    if text_value.startswith("/"):
        return text_value
    return media_path(text_value.strip("/"))


def _is_pdf_mime(mime_value: str) -> bool:
    return mime_value == "application/pdf" or mime_value.endswith("/pdf")


def _render_origin_value(origin_value: object) -> str:
    text_value = str(origin_value or "").strip()
    if not text_value:
        return "-"

    parsed = urlparse(text_value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        safe_url = html.escape(text_value, quote=True)
        safe_label = html.escape(text_value)
        return f"<a class='source-table__link' href='{safe_url}' target='_blank' rel='noopener'>{safe_label}</a>"

    return html.escape(text_value)


def _render_media_preview(media_url: str, mime_type: str, file_name: str, *, compact: bool = False) -> str:
    safe_url = html.escape(str(media_url or ""), quote=True)
    safe_name = html.escape(str(file_name or "file"))
    resolved_mime = _resolve_mime_type(mime_type, file_name, media_url)
    compact_class = " source-preview--compact" if compact else ""

    if safe_url and resolved_mime.startswith("image/"):
        return f"<img class='source-preview{compact_class}' src='{safe_url}' alt='{safe_name}' loading='lazy' />"

    if safe_url and resolved_mime.startswith("video/"):
        return (
            f"<video class='source-preview{compact_class}' src='{safe_url}' controls preload='metadata' "
            "playsinline></video>"
        )

    if safe_url and _is_pdf_mime(resolved_mime):
        return (
            f"<iframe class='source-preview source-preview--pdf{compact_class}' "
            f"src='{safe_url}#toolbar=0&navpanes=0&scrollbar=0' title='{safe_name}' loading='lazy'></iframe>"
        )

    extension = Path(str(file_name or "")).suffix.lower().lstrip(".")
    extension_label = html.escape(extension.upper() if extension else "FILE")
    return (
        f"<div class='source-preview source-preview--file{compact_class}'>"
        f"<span>{extension_label}</span>"
        "</div>"
    )


def _render_cover_image_preview_markup(media_url: str) -> str:
    resolved_url = _resolve_media_reference(media_url)
    if not resolved_url:
        return (
            "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' "
            "role='button' tabindex='0' aria-label='Select source image'>"
            "<span class='sources-cover-image-preview-empty'>No image selected yet.</span>"
            "</div>"
        )

    safe_url = html.escape(resolved_url, quote=True)
    return (
        "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' "
        "role='button' tabindex='0' aria-label='Change source image'>"
        f"<img class='sources-cover-image-preview-img' src='{safe_url}' alt='Source cover image' loading='lazy' />"
        "</div>"
    )


def _render_tag_chips(tags: Sequence[str]) -> str:
    if not tags:
        return '<span class="source-tag source-tag--muted">no-tags</span>'
    chips = []
    for tag in tags:
        chips.append(f"<span class='source-tag'>{html.escape(str(tag))}</span>")
    return "".join(chips)


def _render_sources_catalog_cards(sources: Sequence[Dict[str, object]]) -> str:
    if not sources:
        return "<div class='source-empty'>No sources match the current filter.</div>"

    cards: List[str] = []
    for source in sources:
        slug = str(source.get("slug") or "")
        href = f"/sources-individual/?slug={quote(slug, safe='-')}"
        name = html.escape(str(source.get("name") or "Untitled source"))
        summary = html.escape(str(source.get("summary") or ""))
        preview_url = str(source.get("preview_url") or "")
        preview_mime = str(source.get("preview_mime") or "")
        file_count = int(source.get("file_count") or 0)
        used_bytes = int(source.get("total_bytes") or 0)
        max_bytes = int(source.get("max_bytes") or DEFAULT_SOURCE_MAX_BYTES)
        usage_pct = float(source.get("usage_pct") or 0.0)
        usage_label = f"{_format_bytes(used_bytes)} / {_format_bytes(max_bytes)}"
        tags_markup = _render_tag_chips(source.get("tags", []))
        preview_markup = _render_media_preview(preview_url, preview_mime, name, compact=True)

        cards.append(
            (
                f"<a class='source-card' href='{href}'>"
                f"<div class='source-card__preview'>{preview_markup}</div>"
                "<div class='source-card__content'>"
                f"<h3 class='source-card__title'>{name}</h3>"
                f"<p class='source-card__summary'>{summary or 'No description yet.'}</p>"
                f"<div class='source-card__tags'>{tags_markup}</div>"
                "<div class='source-card__meta'>"
                f"<span>{file_count} files</span>"
                f"<span>{usage_label}</span>"
                "</div>"
                "<div class='source-card__usage'>"
                f"<div class='source-card__usage-fill' style='width:{min(100.0, max(0.0, usage_pct)):.1f}%'></div>"
                "</div>"
                "</div>"
                "</a>"
            )
        )

    return f"<div class='source-grid'>{''.join(cards)}</div>"


def _render_sources_catalog_table(sources: Sequence[Dict[str, object]]) -> str:
    if not sources:
        return "<div class='source-empty'>No sources match the current filter.</div>"

    rows: List[str] = []
    for source in sources:
        slug = str(source.get("slug") or "")
        href = f"/sources-individual/?slug={quote(slug, safe='-')}"
        name = html.escape(str(source.get("name") or "Untitled source"))
        file_count = int(source.get("file_count") or 0)
        used_bytes = int(source.get("total_bytes") or 0)
        tags = ", ".join(str(tag) for tag in source.get("tags", []))
        tags_display = html.escape(tags) if tags else "-"

        rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td><a class='source-table__link' href='{href}'>{name}</a></td>",
                    f"<td>{tags_display}</td>",
                    f"<td>{file_count}</td>",
                    f"<td>{_format_bytes(used_bytes)}</td>",
                    "</tr>",
                ]
            )
        )

    return (
        "<div class='source-table-wrap'><table class='source-table'>"
        "<thead><tr>"
        "<th>Source</th><th>Tags</th><th>Files</th><th>Used</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def _render_sources_catalog(sources: Sequence[Dict[str, object]], view_mode: str) -> str:
    if _normalize_catalog_view_mode(view_mode) == CATALOG_VIEW_LIST:
        return _render_sources_catalog_table(sources)
    return _render_sources_catalog_cards(sources)


def _render_source_files_icons(files: Sequence[Dict[str, object]]) -> str:
    cards: List[str] = []
    for file_row in files:
        file_name = str(file_row.get("file_name") or "file")
        media_url = str(file_row.get("media_url") or "")
        mime_type = str(file_row.get("mime_type") or "")
        origin_url = str(file_row.get("origin_url") or "")
        size_bytes = int(file_row.get("size_bytes") or 0)
        created_at = file_row.get("created_at")
        created_label = created_at.strftime("%Y-%m-%d %H:%M") if isinstance(created_at, datetime) else ""

        cards.append(
            "".join(
                [
                    "<article class='source-file-card'>",
                    f"<div class='source-file-card__preview'>{_render_media_preview(media_url, mime_type, file_name)}</div>",
                    "<div class='source-file-card__meta'>",
                    f"<a class='source-file-card__name' href='{html.escape(media_url, quote=True)}' target='_blank' rel='noopener'>{html.escape(file_name)}</a>",
                    f"<span><strong>Origin/Url:</strong> {_render_origin_value(origin_url)}</span>",
                    f"<span>{_format_bytes(size_bytes)}</span>",
                    f"<span>{html.escape(created_label)}</span>",
                    "</div>",
                    "</article>",
                ]
            )
        )

    return f"<div class='source-file-grid'>{''.join(cards)}</div>"


def _render_source_files_table(files: Sequence[Dict[str, object]]) -> str:
    rows: List[str] = []
    for file_row in files:
        file_name = str(file_row.get("file_name") or "file")
        media_url = str(file_row.get("media_url") or "")
        mime_type = str(file_row.get("mime_type") or "")
        origin_url = str(file_row.get("origin_url") or "")
        size_bytes = int(file_row.get("size_bytes") or 0)
        created_at = file_row.get("created_at")
        created_label = created_at.strftime("%Y-%m-%d %H:%M") if isinstance(created_at, datetime) else ""

        rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td><a class='source-table__link' href='{html.escape(media_url, quote=True)}' target='_blank' rel='noopener'>{html.escape(file_name)}</a></td>",
                    f"<td>{_render_origin_value(origin_url)}</td>",
                    f"<td>{html.escape(mime_type or '-')}</td>",
                    f"<td>{_format_bytes(size_bytes)}</td>",
                    f"<td>{html.escape(created_label)}</td>",
                    "</tr>",
                ]
            )
        )

    return (
        "<div class='source-table-wrap source-table-wrap--files'><table class='source-table'>"
        "<thead><tr><th>Name</th><th>Origin/Url</th><th>Type</th><th>Size</th><th>Uploaded</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def _build_source_file_delete_choices(files: Sequence[Dict[str, object]]) -> List[Tuple[str, str]]:
    choices: List[Tuple[str, str]] = []
    for file_row in files:
        try:
            file_id = int(file_row.get("id") or 0)
        except (TypeError, ValueError):
            file_id = 0
        if file_id <= 0:
            continue
        file_name = str(file_row.get("file_name") or "file").strip() or "file"
        size_label = _format_bytes(int(file_row.get("size_bytes") or 0))
        created_at = file_row.get("created_at")
        created_label = created_at.strftime("%Y-%m-%d %H:%M") if isinstance(created_at, datetime) else ""
        if created_label:
            label = f"{file_name} ({size_label}, {created_label})"
        else:
            label = f"{file_name} ({size_label})"
        choices.append((label, str(file_id)))
    return choices


def _render_source_edit_delete_files_editor(
    files: Sequence[Dict[str, object]],
    selected_file_ids: Sequence[int] | None = None,
) -> str:
    rows = list(files or [])
    if not rows:
        return "<div class='source-empty source-empty--compact'>No existing files to delete.</div>"

    selected_ids: set[int] = set()
    for candidate in selected_file_ids or []:
        try:
            parsed = int(candidate)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            selected_ids.add(parsed)

    cards: List[str] = []
    for file_row in rows:
        try:
            file_id = int(file_row.get("id") or 0)
        except (TypeError, ValueError):
            file_id = 0
        if file_id <= 0:
            continue

        file_name = str(file_row.get("file_name") or "file").strip() or "file"
        media_url = str(file_row.get("media_url") or "")
        mime_type = str(file_row.get("mime_type") or "")
        size_label = _format_bytes(int(file_row.get("size_bytes") or 0))
        created_at = file_row.get("created_at")
        created_label = created_at.strftime("%Y-%m-%d %H:%M") if isinstance(created_at, datetime) else ""
        selected = file_id in selected_ids
        selected_class = " is-selected" if selected else ""
        button_title = "Undo delete" if selected else f"Delete {file_name}"

        cards.append(
            "".join(
                [
                    (
                        f"<article class='sources-edit-delete-card{selected_class}' "
                        f"data-file-id='{file_id}' data-file-name='{html.escape(file_name, quote=True)}'>"
                    ),
                    (
                        "<button type='button' class='sources-edit-delete-card__remove-btn' "
                        f"aria-label='{html.escape(button_title, quote=True)}' "
                        f"title='{html.escape(button_title, quote=True)}' "
                        f"aria-pressed='{'true' if selected else 'false'}'>"
                        "×"
                        "</button>"
                    ),
                    "<div class='sources-edit-delete-card__preview'>",
                    _render_media_preview(media_url, mime_type, file_name, compact=True),
                    "</div>",
                    "<div class='sources-edit-delete-card__meta'>",
                    f"<div class='sources-edit-delete-card__name'>{html.escape(file_name)}</div>",
                    f"<div class='sources-edit-delete-card__sub'>{html.escape(size_label)}</div>",
                    (
                        f"<div class='sources-edit-delete-card__sub'>{html.escape(created_label)}</div>"
                        if created_label
                        else ""
                    ),
                    "</div>",
                    "</article>",
                ]
            )
        )

    if not cards:
        return "<div class='source-empty source-empty--compact'>No existing files to delete.</div>"

    return (
        "<section class='sources-edit-delete-shell'>"
        "<div class='sources-edit-delete-head'>Existing files (click × to mark for deletion)</div>"
        f"<div class='sources-edit-delete-grid'>{''.join(cards)}</div>"
        "</section>"
    )


def _fetch_source_tag_catalog() -> List[str]:
    _ensure_sources_db()
    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "sources_tags"):
            return []
        rows = session.execute(
            text(
                """
                SELECT label
                FROM app.sources_tags
                ORDER BY lower(label), id
                """
            )
        ).scalars().all()

    catalog: List[str] = []
    seen: set[str] = set()
    for row in rows:
        normalized = _normalize_tag(str(row or ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        catalog.append(normalized)
    return catalog


def _render_source_header_meta(source: Dict[str, object]) -> str:
    tags_source = source.get("tags", [])
    if not isinstance(tags_source, (list, tuple, set)):
        tags_source = []
    tags = [_normalize_tag(str(tag)) for tag in tags_source if _normalize_tag(str(tag))]
    catalog_tags = list(tags)
    try:
        catalog_tags = sorted(
            {
                _normalize_tag(tag)
                for tag in [*tags, *_fetch_source_tag_catalog()]
                if _normalize_tag(tag)
            }
        )
    except Exception:
        logger.warning("Could not load source tag catalog for inline editor.", exc_info=True)
        catalog_tags = list(tags)
    tags_markup = _render_tag_chips(tags)
    tag_catalog_json = html.escape(json.dumps(catalog_tags, ensure_ascii=True), quote=True)
    return (
        "<div class='source-browser-head__tags' "
        f"data-tag-catalog='{tag_catalog_json}'>"
        f"{tags_markup}"
        "</div>"
    )


def _render_source_description_markdown(markdown: str) -> str:
    normalized = str(markdown or "").strip()
    if not normalized:
        return ""
    return _render_citation_compiled_markdown(normalized)


def _is_source_markdown_preview_mode(view_mode: object) -> bool:
    normalized_mode = str(view_mode or DEFAULT_MARKDOWN_VIEW).strip().lower()
    return normalized_mode in {
        str(MARKDOWN_VIEW_PREVIEW).strip().lower(),
        "compiled",
        "preview",
    }


def _toggle_source_editor_markdown_view(view_mode: str, source_markdown: str):
    is_preview = _is_source_markdown_preview_mode(view_mode)
    markdown_value = source_markdown or ""
    preview_value = _render_source_description_markdown(markdown_value)
    return (
        gr.update(value=markdown_value, visible=not is_preview),
        gr.update(value=preview_value, visible=is_preview),
    )


def _refresh_source_editor_markdown_preview(source_markdown: str):
    return gr.update(value=_render_source_description_markdown(source_markdown or ""))


def _render_source_files_content(files: Sequence[Dict[str, object]], view_mode: str) -> str:
    if not files:
        return "<div class='source-empty'>No files uploaded for this source yet.</div>"
    content_markup = (
        _render_source_files_table(files)
        if _normalize_file_view_mode(view_mode) == FILE_VIEW_LIST
        else _render_source_files_icons(files)
    )
    return f"<div class='source-browser-content'>{content_markup}</div>"


def _render_source_files_panel(source_slug: str, view_mode: str) -> str:
    normalized_slug = str(source_slug or "").strip().lower()
    if not normalized_slug:
        return "<div class='source-empty'>Select a source to browse its uploaded files.</div>"

    source, files = _fetch_source_and_files(normalized_slug)
    if source is None:
        return "<div class='source-empty'>The selected source no longer exists.</div>"

    header_markup = (
        "<section class='source-browser-head'>"
        f"{_render_source_header_meta(source)}"
        "</section>"
    )
    return header_markup + _render_source_files_content(files, view_mode)


def _role_flags_from_request(request: gr.Request | None) -> tuple[Dict[str, object], bool]:
    user = get_user(request) or {}
    privileges = user.get("privileges") or {}
    can_submit = _is_truthy(privileges.get("base_user"))
    return user, can_submit


def _user_has_editor_privilege(user: Dict[str, object]) -> bool:
    privileges = user.get("privileges")
    if not isinstance(privileges, dict):
        return False
    return _is_truthy(privileges.get("editor"))


def _resolve_request_user_id(user: Dict[str, object]) -> int:
    for key in ("user_id", "employee_id", "id"):
        raw_value = user.get(key)
        if raw_value in (None, ""):
            continue
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return 0


def _resolve_or_create_actor_user_id(session, user: Dict[str, object]) -> int:
    actor_user_id = _resolve_request_user_id(user)
    if actor_user_id > 0:
        return actor_user_id

    email = str(user.get("email") or "").strip().lower()
    if not email:
        return 0

    display_name = str(user.get("name") or email.split("@", 1)[0]).strip()
    user_id, _email, _name = ensure_user(
        session,
        user_identifier=email,
        display_name=display_name,
    )
    return int(user_id)


def _source_slug_exists(session, slug: str) -> bool:
    return bool(
        session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM app.sources_cards
                    WHERE slug = :slug
                )
                """
            ),
            {"slug": str(slug or "").strip().lower()},
        ).scalar_one()
    )


def _next_available_source_slug(session, seed: str) -> str:
    base_slug = _slugify(seed)
    candidate = base_slug
    suffix = 2
    while _source_slug_exists(session, candidate):
        candidate = f"{base_slug}-{suffix}"
        suffix += 1
    return candidate


def _build_source_folder_prefix(slug: str) -> str:
    base = str(SOURCES_MEDIA_PREFIX or "").strip("/ ")
    if base:
        return f"{base}/{_slugify(slug)}"
    return _slugify(slug)


def _ensure_source_tag(session, label: str) -> int:
    normalized_label = _normalize_tag(label)
    if not normalized_label:
        raise ValueError("Tag label cannot be empty.")
    code = _slugify(normalized_label).replace("-", "_")

    row = session.execute(
        text(
            """
            INSERT INTO app.sources_tags (code, label)
            VALUES (:code, :label)
            ON CONFLICT (label) DO UPDATE
            SET code = EXCLUDED.code,
                updated_at = now()
            RETURNING id
            """
        ),
        {"code": code, "label": normalized_label},
    ).mappings().one()
    return int(row["id"])


def _set_source_tags(session, source_id: int, tags: Sequence[str]) -> None:
    if not (_table_exists_in_app_schema(session, "sources_card_tags") and _table_exists_in_app_schema(session, "sources_tags")):
        if tags:
            logger.warning("Skipping source tags update because sources tag tables are missing.")
        return

    session.execute(
        text(
            """
            DELETE FROM app.sources_card_tags
            WHERE source_id = :source_id
            """
        ),
        {"source_id": int(source_id)},
    )

    rows: List[Dict[str, int]] = []
    for tag in tags:
        tag_id = _ensure_source_tag(session, tag)
        rows.append({"source_id": int(source_id), "tag_id": tag_id})

    if rows:
        session.execute(
            text(
                """
                INSERT INTO app.sources_card_tags (source_id, tag_id)
                VALUES (:source_id, :tag_id)
                ON CONFLICT (source_id, tag_id) DO NOTHING
                """
            ),
            rows,
        )


def _extract_upload_path(uploaded_file: object) -> str:
    if not uploaded_file:
        return ""
    if isinstance(uploaded_file, Path):
        return str(uploaded_file)
    if isinstance(uploaded_file, str):
        return uploaded_file
    if isinstance(uploaded_file, dict):
        return str(uploaded_file.get("path") or uploaded_file.get("name") or "")
    if isinstance(uploaded_file, (list, tuple)):
        for item in uploaded_file:
            candidate = _extract_upload_path(item)
            if candidate:
                return candidate
    for attr in ("path", "name", "orig_name"):
        value = getattr(uploaded_file, attr, None)
        if isinstance(value, str) and value.strip():
            return value
    file_obj = getattr(uploaded_file, "file", None)
    inner_name = getattr(file_obj, "name", None) if file_obj is not None else None
    if isinstance(inner_name, str):
        return inner_name
    return ""


def _resolve_upload_paths(uploaded_files: object) -> List[Path]:
    if uploaded_files is None:
        return []

    candidates: List[object]
    if isinstance(uploaded_files, (list, tuple)):
        candidates = list(uploaded_files)
    else:
        candidates = [uploaded_files]

    paths: List[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        upload_path = _extract_upload_path(candidate)
        if not upload_path:
            continue
        path_obj = Path(upload_path)
        if not path_obj.is_file():
            continue
        key = str(path_obj.resolve())
        if key in seen:
            continue
        seen.add(key)
        paths.append(path_obj)
    return paths


def _actor_storage_identity(user: Dict[str, object], actor_user_id: int) -> str:
    actor_email = str(user.get("email") or "").strip().lower()
    return actor_email or f"user-{max(1, int(actor_user_id or 0))}"


def _upload_cover_image_bytes(
    image_bytes: bytes,
    *,
    extension: str,
    bucket_value: str,
    folder_prefix: str,
    actor_identity: str,
    uploaded_blob_refs: List[Tuple[str, str]],
) -> str:
    if not image_bytes:
        raise ValueError("Source image is empty.")
    if len(image_bytes) > MAX_COVER_IMAGE_BYTES:
        raise ValueError(f"Source image exceeds {MAX_COVER_IMAGE_BYTES // (1024 * 1024)} MB limit.")

    normalized_extension = str(extension or "").strip().lower()
    if normalized_extension not in ALLOWED_IMAGE_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_IMAGE_EXTENSIONS))
        raise ValueError(f"Unsupported source image format. Allowed: {allowed}")

    actor_slug = _slugify((actor_identity or "anon").split("@", 1)[0])
    filename = f"{actor_slug}-cover-{uuid4().hex[:10]}{normalized_extension}"
    blob_name = f"{folder_prefix}/{filename}" if folder_prefix else filename

    blob = get_bucket(bucket_value).blob(blob_name)
    blob.cache_control = "public, max-age=3600"
    blob.upload_from_string(
        image_bytes,
        content_type=IMAGE_CONTENT_TYPES.get(normalized_extension, "application/octet-stream"),
    )
    uploaded_blob_refs.append((bucket_value, blob_name))
    return media_path(blob_name)


def _persist_uploaded_source_cover_image(
    upload_path: str,
    *,
    bucket_value: str,
    folder_prefix: str,
    actor_identity: str,
    uploaded_blob_refs: List[Tuple[str, str]],
) -> str:
    source = Path((upload_path or "").strip())
    if not source.is_file():
        raise ValueError("Uploaded source image could not be read.")

    extension = source.suffix.lower()
    image_bytes = source.read_bytes()
    return _upload_cover_image_bytes(
        image_bytes,
        extension=extension,
        bucket_value=bucket_value,
        folder_prefix=folder_prefix,
        actor_identity=actor_identity,
        uploaded_blob_refs=uploaded_blob_refs,
    )


def _persist_uploaded_source_cover_image_data_url(
    image_data_url: str,
    *,
    bucket_value: str,
    folder_prefix: str,
    actor_identity: str,
    uploaded_blob_refs: List[Tuple[str, str]],
) -> str:
    raw_payload = str(image_data_url or "").strip()
    if not raw_payload:
        raise ValueError("Cropped source image payload is empty.")

    match = DATA_URL_IMAGE_RE.match(raw_payload)
    if not match:
        raise ValueError("Cropped source image payload is invalid.")

    mime_type = str(match.group(1) or "").strip().lower()
    extension = ALLOWED_IMAGE_MIME_TYPES.get(mime_type)
    if not extension:
        allowed = ", ".join(sorted(ALLOWED_IMAGE_MIME_TYPES))
        raise ValueError(f"Unsupported cropped source image type `{mime_type}`. Allowed: {allowed}")

    base64_payload = re.sub(r"\s+", "", str(match.group(2) or ""))
    try:
        image_bytes = base64.b64decode(base64_payload, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Cropped source image payload could not be decoded.") from exc

    return _upload_cover_image_bytes(
        image_bytes,
        extension=extension,
        bucket_value=bucket_value,
        folder_prefix=folder_prefix,
        actor_identity=actor_identity,
        uploaded_blob_refs=uploaded_blob_refs,
    )


def _coerce_file_origin_rows(raw_rows: object) -> List[Tuple[str, str, str]]:
    values_attr = getattr(raw_rows, "values", None)
    if values_attr is not None and hasattr(values_attr, "tolist"):
        try:
            converted_values = values_attr.tolist()
        except Exception:
            converted_values = None
        if isinstance(converted_values, list):
            raw_rows = converted_values

    tolist_fn = getattr(raw_rows, "tolist", None)
    if callable(tolist_fn) and not isinstance(raw_rows, list):
        try:
            converted_rows = tolist_fn()
        except Exception:
            converted_rows = None
        if isinstance(converted_rows, list):
            raw_rows = converted_rows

    if isinstance(raw_rows, str):
        stripped = raw_rows.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = []
            raw_rows = parsed if isinstance(parsed, list) else []
        else:
            raw_rows = []

    rows: List[Tuple[str, str, str]] = []
    if not isinstance(raw_rows, list):
        return rows
    for row in raw_rows:
        if not isinstance(row, (list, tuple)):
            continue
        file_label = str(row[0] if len(row) >= 1 else "").strip()
        origin_value = str(row[1] if len(row) >= 2 else "").strip()
        display_label = str(row[2] if len(row) >= 3 else file_label).strip() or file_label
        rows.append((file_label, origin_value, display_label))
    return rows


def _render_create_file_origins_editor(rows: Sequence[Tuple[str, str, str]]) -> str:
    body = (
        "".join(
            (
                f"<div class='sources-file-origin-row' "
                f"data-origin-key='{html.escape(origin_key, quote=True)}' "
                f"data-file-name='{html.escape(display_name, quote=True)}'>"
                f"<div class='sources-file-origin-row__name'>{html.escape(display_name)}</div>"
                "<div class='sources-file-origin-row__input-wrap'>"
                f"<input class='sources-file-origin-row__input' type='text' value='{html.escape(origin_value, quote=True)}' "
                "placeholder='Origin/Url' />"
                "</div>"
                "<div class='sources-file-origin-row__actions'>"
                "<button class='sources-file-origin-row__remove' type='button' "
                "title='Remove file' aria-label='Remove file'>X</button>"
                "</div>"
                "</div>"
            )
            for origin_key, display_name, origin_value in rows
        )
        if rows
        else (
            "<div class='sources-file-origin-empty'>"
            "No files yet. Use + to add files."
            "</div>"
        )
    )
    return (
        "<section class='sources-file-origin-editor-shell'>"
        "<div class='sources-file-origin-editor-toolbar'>"
        "<span class='sources-file-origin-editor-title'>Source files</span>"
        "<button class='sources-file-origin-add-btn' type='button' "
        "title='Add files' aria-label='Add files'>+</button>"
        "</div>"
        "<div class='sources-file-origin-editor-head'>"
        "<span>File</span><span>Origin/Url</span><span aria-hidden='true'></span>"
        "</div>"
        f"<div class='sources-file-origin-editor-body'>{body}</div>"
        "</section>"
    )


def _unsorted_catalog_by_id(raw_catalog: object) -> Dict[int, Dict[str, object]]:
    rows = _parse_source_create_unsorted_catalog(raw_catalog)
    return {int(row.get("id") or 0): row for row in rows if int(row.get("id") or 0) > 0}


def _sync_create_file_origins_editor(
    uploaded_files: object,
    current_rows: object,
    selected_unsorted_file_ids: object = None,
    unsorted_catalog_state: object = None,
    removed_origin_keys: object = None,
):
    file_paths = _resolve_upload_paths(uploaded_files)
    selected_unsorted_ids = _parse_unsorted_file_ids(selected_unsorted_file_ids)
    unsorted_by_id = _unsorted_catalog_by_id(unsorted_catalog_state)
    removed_keys = _parse_origin_key_set(removed_origin_keys)
    selected_unsorted_rows = [
        unsorted_by_id[file_id]
        for file_id in selected_unsorted_ids
        if file_id in unsorted_by_id
    ]

    if not file_paths and not selected_unsorted_rows:
        return gr.update(value=_render_create_file_origins_editor([]), visible=True), gr.update(value="")

    existing_rows = _coerce_file_origin_rows(current_rows)
    existing_by_key: Dict[str, List[str]] = {}
    existing_by_display: Dict[str, List[str]] = {}
    for file_label, origin_value, display_label in existing_rows:
        key_label = str(file_label or "").strip()
        if key_label:
            existing_by_key.setdefault(key_label, []).append(origin_value)
        display_text = str(display_label or key_label).strip()
        if display_text:
            existing_by_display.setdefault(display_text, []).append(origin_value)

    def _take_existing_origin(origin_key: str, display_name: str, fallback: str = "") -> str:
        key_candidates = existing_by_key.get(origin_key, [])
        if key_candidates:
            return key_candidates.pop(0)
        display_candidates = existing_by_display.get(display_name, [])
        if display_candidates:
            return display_candidates.pop(0)
        return fallback

    next_rows: List[Tuple[str, str, str]] = []
    for path_obj in file_paths:
        display_name = path_obj.name
        origin_key = _origin_key_for_uploaded_path(path_obj)
        if origin_key in removed_keys:
            continue
        next_origin = _take_existing_origin(origin_key, display_name, "")
        next_rows.append((origin_key, display_name, next_origin))

    for row in selected_unsorted_rows:
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            continue
        display_name = _build_unsorted_origin_display_label(file_id, str(row.get("file_name") or ""))
        origin_key = _origin_key_for_unsorted_file(file_id)
        if origin_key in removed_keys:
            continue
        default_origin = str(row.get("origin_text") or "").strip()
        next_origin = _take_existing_origin(origin_key, display_name, default_origin)
        next_rows.append((origin_key, display_name, next_origin))

    next_serialized_rows = [
        [origin_key, origin_value, display_name]
        for origin_key, display_name, origin_value in next_rows
    ]
    next_serialized = json.dumps(next_serialized_rows, ensure_ascii=True) if next_serialized_rows else ""
    next_markup = _render_create_file_origins_editor(next_rows)
    return gr.update(value=next_markup, visible=True), gr.update(value=next_serialized)


def _filter_source_create_selected_files(
    file_paths: Sequence[Path],
    unsorted_rows: Sequence[Dict[str, object]],
    raw_origins: object,
) -> tuple[List[Path], List[Dict[str, object]]]:
    origin_rows = _coerce_file_origin_rows(raw_origins)
    if not origin_rows:
        return list(file_paths), list(unsorted_rows)

    include_counts: Dict[str, int] = {}
    for origin_key, _origin_value, _display_name in origin_rows:
        normalized_key = str(origin_key or "").strip()
        if not normalized_key:
            continue
        include_counts[normalized_key] = include_counts.get(normalized_key, 0) + 1

    selected_paths: List[Path] = []
    for path_obj in file_paths:
        origin_key = _origin_key_for_uploaded_path(path_obj)
        remaining = include_counts.get(origin_key, 0)
        if remaining <= 0:
            continue
        include_counts[origin_key] = remaining - 1
        selected_paths.append(path_obj)

    selected_unsorted_rows: List[Dict[str, object]] = []
    for row in unsorted_rows:
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            continue
        origin_key = _origin_key_for_unsorted_file(file_id)
        remaining = include_counts.get(origin_key, 0)
        if remaining <= 0:
            continue
        include_counts[origin_key] = remaining - 1
        selected_unsorted_rows.append(row)

    return selected_paths, selected_unsorted_rows


def _resolve_source_create_origin_values(
    file_paths: Sequence[Path],
    unsorted_rows: Sequence[Dict[str, object]],
    raw_origins: object,
) -> tuple[List[str], List[str]]:
    expected_keys: List[Tuple[str, str, str]] = []
    for path_obj in file_paths:
        display_name = path_obj.name
        expected_keys.append((_origin_key_for_uploaded_path(path_obj), display_name, "upload"))
    for row in unsorted_rows:
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            continue
        display_name = _build_unsorted_origin_display_label(file_id, str(row.get("file_name") or ""))
        expected_keys.append((_origin_key_for_unsorted_file(file_id), display_name, "unsorted"))

    if not expected_keys:
        return [], []

    existing_rows = _coerce_file_origin_rows(raw_origins)
    origin_by_key: Dict[str, List[str]] = {}
    origin_by_display: Dict[str, List[str]] = {}
    for file_label, origin_value, display_label in existing_rows:
        normalized_key = str(file_label or "").strip()
        if normalized_key:
            origin_by_key.setdefault(normalized_key, []).append(origin_value)
        normalized_display = str(display_label or normalized_key).strip()
        if normalized_display:
            origin_by_display.setdefault(normalized_display, []).append(origin_value)

    uploaded_origins: List[str] = []
    unsorted_origins: List[str] = []
    for origin_key, display_name, scope in expected_keys:
        candidates = origin_by_key.get(origin_key, [])
        origin_value = candidates.pop(0) if candidates else ""
        if not origin_value:
            display_candidates = origin_by_display.get(display_name, [])
            if display_candidates:
                origin_value = display_candidates.pop(0)
        if not origin_value and scope == "unsorted":
            unsorted_id = _parse_unsorted_id_from_origin_key(origin_key)
            row_match = next((row for row in unsorted_rows if int(row.get("id") or 0) == unsorted_id), None)
            origin_value = str(row_match.get("origin_text") or "").strip() if row_match else ""
        if not origin_value:
            raise ValueError("Each file selected for this source must include an Origin/Url value.")
        if scope == "upload":
            uploaded_origins.append(origin_value)
        else:
            unsorted_origins.append(origin_value)

    return uploaded_origins, unsorted_origins


def _store_uploaded_files_for_source(
    session,
    *,
    source_id: int,
    bucket_value: str,
    folder_prefix: str,
    max_bytes: int,
    file_paths: Sequence[Path],
    origin_urls: Sequence[str],
    actor_user_id: int,
    uploaded_blob_refs: List[Tuple[str, str]],
) -> int:
    if not file_paths:
        raise ValueError("No uploaded files were detected.")
    if len(origin_urls) != len(file_paths):
        raise ValueError("Each uploaded file must include an Origin/Url value.")

    normalized_max_bytes = max(1, int(max_bytes or DEFAULT_SOURCE_MAX_BYTES))
    incoming_bytes = sum(int(path_obj.stat().st_size) for path_obj in file_paths)
    if incoming_bytes <= 0:
        raise ValueError("Uploaded files are empty.")

    used_bytes = int(
        session.execute(
            text(
                """
                SELECT COALESCE(SUM(size_bytes), 0)::bigint
                FROM app.sources_files
                WHERE source_id = :source_id
                """
            ),
            {"source_id": int(source_id)},
        ).scalar_one()
        or 0
    )

    if used_bytes + incoming_bytes > normalized_max_bytes:
        available = normalized_max_bytes - used_bytes
        raise ValueError(
            "Source storage limit reached. "
            f"Current: {_format_bytes(used_bytes)}, "
            f"incoming: {_format_bytes(incoming_bytes)}, "
            f"available: {_format_bytes(max(0, available))}, "
            f"limit: {_format_bytes(normalized_max_bytes)}."
        )

    inserted_rows: List[Dict[str, object]] = []
    for index, path_obj in enumerate(file_paths):
        origin_url = str(origin_urls[index] or "").strip()
        if not origin_url:
            raise ValueError("Each uploaded file must include an Origin/Url value.")
        safe_name = _sanitize_filename(path_obj.name)
        if not safe_name:
            safe_name = f"file-{uuid4().hex[:8]}"
        stored_name = f"{uuid4().hex[:10]}-{safe_name}"
        blob_name = f"{folder_prefix}/{stored_name}" if folder_prefix else stored_name
        content_type = mimetypes.guess_type(safe_name)[0] or "application/octet-stream"

        blob = get_bucket(bucket_value).blob(blob_name)
        blob.cache_control = "public, max-age=3600"
        blob.upload_from_filename(str(path_obj), content_type=content_type)
        uploaded_blob_refs.append((bucket_value, blob_name))

        inserted_rows.append(
            {
                "source_id": int(source_id),
                "blob_path": blob_name,
                "file_name": safe_name,
                "origin_url": origin_url,
                "mime_type": content_type,
                "size_bytes": int(path_obj.stat().st_size),
                "uploaded_by_user_id": int(actor_user_id),
            }
        )

    if inserted_rows:
        session.execute(
            text(
                """
                INSERT INTO app.sources_files (
                    source_id,
                    blob_path,
                    file_name,
                    origin_url,
                    mime_type,
                    size_bytes,
                    uploaded_by_user_id
                )
                VALUES (
                    :source_id,
                    :blob_path,
                    :file_name,
                    :origin_url,
                    :mime_type,
                    :size_bytes,
                    :uploaded_by_user_id
                )
                """
            ),
            inserted_rows,
        )

    return incoming_bytes


def _fetch_unsorted_files_by_ids(session, file_ids: Sequence[int]) -> List[Dict[str, object]]:
    normalized_ids: List[int] = []
    seen_ids: set[int] = set()
    for file_id in file_ids:
        try:
            parsed = int(file_id)
        except (TypeError, ValueError):
            continue
        if parsed <= 0 or parsed in seen_ids:
            continue
        seen_ids.add(parsed)
        normalized_ids.append(parsed)
    if not normalized_ids:
        return []

    if not _table_exists_in_app_schema(session, "unsorted_files"):
        return []

    bind_values: Dict[str, object] = {}
    placeholders: List[str] = []
    for index, file_id in enumerate(normalized_ids):
        key = f"unsorted_file_id_{index}"
        bind_values[key] = int(file_id)
        placeholders.append(f":{key}")

    rows = session.execute(
        text(
            f"""
            SELECT
                uf.id,
                uf.bucket,
                uf.blob_path,
                uf.file_name,
                uf.origin_text,
                uf.mime_type,
                uf.size_bytes
            FROM app.unsorted_files uf
            WHERE uf.id IN ({", ".join(placeholders)})
            """
        ),
        bind_values,
    ).mappings().all()

    rows_by_id: Dict[int, Dict[str, object]] = {
        int(row.get("id") or 0): {
            "id": int(row.get("id") or 0),
            "bucket": str(row.get("bucket") or "").strip() or DEFAULT_SOURCE_BUCKET,
            "blob_path": str(row.get("blob_path") or "").strip().lstrip("/"),
            "file_name": str(row.get("file_name") or "").strip(),
            "origin_text": str(row.get("origin_text") or "").strip(),
            "mime_type": str(row.get("mime_type") or "").strip().lower(),
            "size_bytes": max(0, int(row.get("size_bytes") or 0)),
        }
        for row in rows
        if int(row.get("id") or 0) > 0
    }
    return [rows_by_id[file_id] for file_id in normalized_ids if file_id in rows_by_id]


def _mark_unsorted_files_create_source_action(
    session,
    *,
    unsorted_file_ids: Sequence[int],
    actor_user_id: int,
    source_id: int,
    source_slug: str,
) -> None:
    if actor_user_id <= 0 or not unsorted_file_ids:
        return
    if not _table_exists_in_app_schema(session, "unsorted_file_actions"):
        return

    rows: List[Dict[str, object]] = []
    for file_id in unsorted_file_ids:
        try:
            parsed = int(file_id)
        except (TypeError, ValueError):
            continue
        if parsed <= 0:
            continue
        rows.append(
            {
                "unsorted_file_id": parsed,
                "actor_user_id": int(actor_user_id),
                "action_type": ACTION_CREATE_NEW_SOURCE,
                "source_id": int(source_id),
                "source_slug": str(source_slug or "").strip().lower(),
            }
        )
    if not rows:
        return

    session.execute(
        text(
            """
            INSERT INTO app.unsorted_file_actions (
                unsorted_file_id,
                actor_user_id,
                action_type,
                source_id,
                source_slug
            )
            VALUES (
                :unsorted_file_id,
                :actor_user_id,
                :action_type,
                :source_id,
                :source_slug
            )
            ON CONFLICT (unsorted_file_id, actor_user_id, action_type) DO UPDATE
            SET source_id = EXCLUDED.source_id,
                source_slug = EXCLUDED.source_slug,
                updated_at = now()
            """
        ),
        rows,
    )


def _store_unsorted_files_for_source(
    session,
    *,
    source_id: int,
    source_slug: str,
    bucket_value: str,
    folder_prefix: str,
    max_bytes: int,
    unsorted_files: Sequence[Dict[str, object]],
    origin_urls: Sequence[str],
    actor_user_id: int,
    uploaded_blob_refs: List[Tuple[str, str]],
) -> int:
    if not unsorted_files:
        return 0
    if len(origin_urls) != len(unsorted_files):
        raise ValueError("Each selected unsorted file must include an Origin/Url value.")

    normalized_max_bytes = max(1, int(max_bytes or DEFAULT_SOURCE_MAX_BYTES))
    incoming_bytes = sum(max(0, int(row.get("size_bytes") or 0)) for row in unsorted_files)
    if incoming_bytes <= 0:
        raise ValueError("Selected unsorted files are empty.")

    used_bytes = int(
        session.execute(
            text(
                """
                SELECT COALESCE(SUM(size_bytes), 0)::bigint
                FROM app.sources_files
                WHERE source_id = :source_id
                """
            ),
            {"source_id": int(source_id)},
        ).scalar_one()
        or 0
    )
    if used_bytes + incoming_bytes > normalized_max_bytes:
        available = normalized_max_bytes - used_bytes
        raise ValueError(
            "Source storage limit reached. "
            f"Current: {_format_bytes(used_bytes)}, "
            f"incoming: {_format_bytes(incoming_bytes)}, "
            f"available: {_format_bytes(max(0, available))}, "
            f"limit: {_format_bytes(normalized_max_bytes)}."
        )

    inserted_rows: List[Dict[str, object]] = []
    imported_file_ids: List[int] = []
    destination_bucket = get_bucket(bucket_value)

    for index, row in enumerate(unsorted_files):
        file_id = int(row.get("id") or 0)
        if file_id <= 0:
            raise ValueError("An unsorted file selection is invalid.")

        origin_url = str(origin_urls[index] or "").strip()
        if not origin_url:
            raise ValueError("Each selected unsorted file must include an Origin/Url value.")

        source_blob_path = str(row.get("blob_path") or "").strip().lstrip("/")
        if not source_blob_path:
            raise ValueError("A selected unsorted file is missing its storage path.")

        source_bucket_name = str(row.get("bucket") or "").strip() or DEFAULT_SOURCE_BUCKET
        source_bucket = get_bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_path)

        safe_name = _sanitize_filename(str(row.get("file_name") or "").strip())
        if not safe_name:
            safe_name = f"file-{uuid4().hex[:8]}"
        stored_name = f"{uuid4().hex[:10]}-{safe_name}"
        destination_blob_name = f"{folder_prefix}/{stored_name}" if folder_prefix else stored_name

        copied_blob = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
        copied_blob.cache_control = "public, max-age=3600"
        try:
            copied_blob.patch()
        except Exception:
            pass

        uploaded_blob_refs.append((bucket_value, destination_blob_name))

        mime_type = str(row.get("mime_type") or "").strip().lower()
        if not mime_type:
            mime_type = mimetypes.guess_type(safe_name)[0] or "application/octet-stream"

        inserted_rows.append(
            {
                "source_id": int(source_id),
                "blob_path": destination_blob_name,
                "file_name": safe_name,
                "origin_url": origin_url,
                "mime_type": mime_type,
                "size_bytes": max(0, int(row.get("size_bytes") or 0)),
                "uploaded_by_user_id": int(actor_user_id),
            }
        )
        imported_file_ids.append(file_id)

    if inserted_rows:
        session.execute(
            text(
                """
                INSERT INTO app.sources_files (
                    source_id,
                    blob_path,
                    file_name,
                    origin_url,
                    mime_type,
                    size_bytes,
                    uploaded_by_user_id
                )
                VALUES (
                    :source_id,
                    :blob_path,
                    :file_name,
                    :origin_url,
                    :mime_type,
                    :size_bytes,
                    :uploaded_by_user_id
                )
                """
            ),
            inserted_rows,
        )

    _mark_unsorted_files_create_source_action(
        session,
        unsorted_file_ids=imported_file_ids,
        actor_user_id=actor_user_id,
        source_id=source_id,
        source_slug=source_slug,
    )
    return incoming_bytes


def _delete_source_files_for_source(
    session,
    *,
    source_id: int,
    bucket_value: str,
    file_ids: Sequence[int],
) -> int:
    normalized_ids: set[int] = set()
    for file_id in file_ids:
        try:
            parsed = int(file_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            normalized_ids.add(parsed)
    if not normalized_ids:
        return 0

    rows = session.execute(
        text(
            """
            SELECT
                id,
                blob_path
            FROM app.sources_files
            WHERE source_id = :source_id
            """
        ),
        {"source_id": int(source_id)},
    ).mappings().all()

    target_rows = [row for row in rows if int(row.get("id") or 0) in normalized_ids]
    if not target_rows:
        return 0

    for row in target_rows:
        blob_name = str(row.get("blob_path") or "").strip()
        if not blob_name:
            continue
        try:
            get_bucket(bucket_value).blob(blob_name).delete()
        except Exception:
            logger.warning(
                "Could not delete source blob %s from bucket %s while removing file id %s",
                blob_name,
                bucket_value,
                row.get("id"),
                exc_info=True,
            )

    session.execute(
        text(
            """
            DELETE FROM app.sources_files
            WHERE source_id = :source_id
              AND id = :file_id
            """
        ),
        [
            {
                "source_id": int(source_id),
                "file_id": int(row.get("id") or 0),
            }
            for row in target_rows
        ],
    )
    return len(target_rows)


def _serialize_source_snapshot(source: Dict[str, object] | None, files: Sequence[Dict[str, object]] | None) -> str:
    source_payload: Dict[str, object]
    if not isinstance(source, dict):
        source_payload = {}
    else:
        source_payload = {
            "slug": str(source.get("slug") or "").strip().lower(),
            "name": str(source.get("name") or "").strip(),
            "description_markdown": str(source.get("description_markdown") or "").strip(),
            "cover_media_url": str(source.get("cover_media_url") or "").strip(),
            "tags": sorted(
                {
                    _normalize_tag(str(tag))
                    for tag in (source.get("tags") or [])
                    if _normalize_tag(str(tag))
                }
            ),
        }

    file_payload: List[Dict[str, object]] = []
    for row in files or []:
        try:
            file_id = int(row.get("id") or 0)
        except (TypeError, ValueError):
            file_id = 0
        file_payload.append(
            {
                "id": file_id,
                "blob_path": str(row.get("blob_path") or "").strip(),
                "file_name": str(row.get("file_name") or "").strip(),
                "origin_url": str(row.get("origin_url") or "").strip(),
                "mime_type": str(row.get("mime_type") or "").strip(),
                "size_bytes": int(row.get("size_bytes") or 0),
            }
        )

    snapshot_payload = {
        "source": source_payload,
        "files": sorted(
            file_payload,
            key=lambda entry: (
                str(entry.get("file_name") or "").lower(),
                str(entry.get("blob_path") or "").lower(),
                int(entry.get("id") or 0),
            ),
        ),
    }
    return json.dumps(snapshot_payload, ensure_ascii=True, sort_keys=True)


def _record_source_proposal_event(
    session,
    proposal_id: int,
    *,
    event_type: str,
    actor_user_id: int,
    notes: str = "",
    payload: Dict[str, object] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO app.sources_change_events (
                proposal_id,
                event_type,
                actor_user_id,
                notes,
                payload_json
            )
            VALUES (
                :proposal_id,
                :event_type,
                :actor_user_id,
                :notes,
                :payload_json
            )
            """
        ),
        {
            "proposal_id": int(proposal_id),
            "event_type": str(event_type or "").strip() or "source_proposal_event",
            "actor_user_id": int(actor_user_id),
            "notes": str(notes or "").strip(),
            "payload_json": json.dumps(payload or {}, ensure_ascii=True, sort_keys=True),
        },
    )


def _build_source_selector_update(
    sources: Sequence[Dict[str, object]],
    selected_slug: str,
) -> tuple[gr.update, str]:
    choices = [(str(row.get("name") or "Source"), str(row.get("slug") or "")) for row in sources]
    slugs = {value for _label, value in choices if value}

    normalized = str(selected_slug or "").strip().lower()
    if normalized not in slugs:
        normalized = choices[0][1] if choices else ""

    update = gr.update(
        choices=choices,
        value=normalized,
        interactive=bool(choices),
    )
    return update, normalized


def _coerce_sources_state(raw_state: object) -> List[Dict[str, object]]:
    if not isinstance(raw_state, list):
        return []

    rows: List[Dict[str, object]] = []
    for entry in raw_state:
        if not isinstance(entry, dict):
            continue
        row = dict(entry)
        raw_tags = row.get("tags")
        if not isinstance(raw_tags, list):
            row["tags"] = []
        rows.append(row)
    return rows


def _dashboard_updates(
    selected_source_slug: str,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    file_view_mode: str,
) -> tuple[gr.update, List[str], List[Dict[str, object]], str, gr.update, str, str]:
    catalog_mode = _normalize_catalog_view_mode(catalog_view_mode)
    files_mode = _normalize_file_view_mode(file_view_mode)

    sources = _fetch_all_sources()
    tag_filter_update, _tag_choices, tag_selection = _build_tag_filter_update(
        sources,
        selected_tag_values,
        default_to_all=True,
    )
    filtered_sources = _filter_sources_for_tag_selection(sources, tag_selection)
    catalog_html = _render_sources_catalog(filtered_sources, catalog_mode)
    source_selector_update, resolved_selected_slug = _build_source_selector_update(sources, selected_source_slug)
    files_html = _render_source_files_panel(resolved_selected_slug, files_mode)

    return (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    )


def _safe_dashboard_updates(
    selected_source_slug: str,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    file_view_mode: str,
) -> tuple[gr.update, List[str], List[Dict[str, object]], str, gr.update, str, str]:
    try:
        return _dashboard_updates(selected_source_slug, selected_tag_values, catalog_view_mode, file_view_mode)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to refresh sources dashboard: %s", exc)
        return (
            gr.update(choices=[(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)], value=[]),
            [],
            [],
            "<div class='source-empty'>Could not load source cards.</div>",
            gr.update(choices=[], value="", interactive=False),
            "",
            "<div class='source-empty'>Could not load source files.</div>",
        )


def _catalog_updates(
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
) -> tuple[gr.update, List[str], List[Dict[str, object]], str]:
    catalog_mode = _normalize_catalog_view_mode(catalog_view_mode)
    sources = _fetch_all_sources()
    tag_filter_update, _tag_choices, tag_selection = _build_tag_filter_update(
        sources,
        selected_tag_values,
        default_to_all=True,
    )
    filtered_sources = _filter_sources_for_tag_selection(sources, tag_selection)
    catalog_html = _render_sources_catalog(filtered_sources, catalog_mode)
    return (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
    )


def _safe_catalog_updates(
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
) -> tuple[gr.update, List[str], List[Dict[str, object]], str]:
    try:
        return _catalog_updates(selected_tag_values, catalog_view_mode)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to refresh sources catalog: %s", exc)
        return (
            gr.update(choices=[(TAG_FILTER_ALL_OPTION, TAG_FILTER_ALL_OPTION)], value=[]),
            [],
            [],
            "<div class='source-empty'>Could not load source cards.</div>",
        )


def _source_title_html_for_slug(
    sources: Sequence[Dict[str, object]],
    selected_source_slug: str,
) -> str:
    normalized_slug = str(selected_source_slug or "").strip().lower()
    if not normalized_slug:
        return "<h2>Source</h2>"

    label = "Source"
    for row in sources:
        slug = str(row.get("slug") or "").strip().lower()
        if slug != normalized_slug:
            continue
        candidate = str(row.get("name") or "").strip()
        if candidate:
            label = candidate
        break

    return f"<h2>{html.escape(label)}</h2>"


def _individual_source_updates(
    selected_source_slug: str,
    file_view_mode: str,
) -> tuple[List[Dict[str, object]], str, gr.update, str, str, str, str]:
    sources = _fetch_all_sources()
    source_selector_update, resolved_selected_slug = _build_source_selector_update(sources, selected_source_slug)
    title_html = _source_title_html_for_slug(sources, resolved_selected_slug)
    normalized_view_mode = _normalize_file_view_mode(file_view_mode)

    if not resolved_selected_slug:
        return (
            sources,
            title_html,
            source_selector_update,
            resolved_selected_slug,
            "",
            "",
            "<div class='source-empty'>Select a source to browse its uploaded files.</div>",
        )

    source, files = _fetch_source_and_files(resolved_selected_slug)
    if source is None:
        return (
            sources,
            title_html,
            source_selector_update,
            resolved_selected_slug,
            "",
            "",
            "<div class='source-empty'>The selected source no longer exists.</div>",
        )

    head_meta_html = _render_source_header_meta(source)
    head_description_markdown = _render_source_description_markdown(str(source.get("description_markdown") or ""))
    files_html = _render_source_files_content(files, normalized_view_mode)
    return (
        sources,
        title_html,
        source_selector_update,
        resolved_selected_slug,
        head_meta_html,
        head_description_markdown,
        files_html,
    )


def _safe_individual_source_updates(
    selected_source_slug: str,
    file_view_mode: str,
) -> tuple[List[Dict[str, object]], str, gr.update, str, str, str, str]:
    try:
        return _individual_source_updates(selected_source_slug, file_view_mode)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to refresh source details page: %s", exc)
        return (
            [],
            "<h2>Source</h2>",
            gr.update(choices=[], value="", interactive=False),
            "",
            "",
            "",
            "<div class='source-empty'>Could not load source files.</div>",
        )


def _load_sources_list_page(request: gr.Request):
    selected_tags = _parse_tag_query_values(_query_param(request, "tag"))
    catalog_view_mode = _normalize_catalog_view_mode(_query_param(request, "view"))

    (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
    ) = _safe_catalog_updates(selected_tags, catalog_view_mode)

    return (
        "<h2>Sources</h2>",
        gr.update(visible=True),
        tag_filter_update,
        tag_selection,
        gr.update(value=catalog_view_mode),
        sources,
        gr.update(value=catalog_html, visible=True),
        "",
    )


def _load_sources_individual_page(request: gr.Request):
    selected_source_slug = (_query_param(request, "slug") or _query_param(request, "source")).lower()
    file_view_mode = _normalize_file_view_mode(_query_param(request, "files_view"))

    (
        sources,
        title_html,
        source_selector_update,
        resolved_selected_slug,
        _head_meta_html,
        _head_description_markdown,
        files_html,
    ) = _safe_individual_source_updates(selected_source_slug, file_view_mode)

    return (
        title_html,
        gr.update(value=file_view_mode),
        sources,
        source_selector_update,
        resolved_selected_slug,
        gr.update(value=files_html, visible=True),
        "",
    )


def _load_sources_individual_editor_page(request: gr.Request):
    selected_source_slug = (_query_param(request, "slug") or _query_param(request, "source")).lower()
    file_view_mode = _normalize_file_view_mode(_query_param(request, "files_view"))

    (
        _sources,
        title_html,
        _source_selector_update,
        resolved_selected_slug,
        head_meta_html,
        head_description_markdown,
        files_html,
    ) = _safe_individual_source_updates(selected_source_slug, file_view_mode)

    has_source = bool(resolved_selected_slug)
    has_description = bool(head_description_markdown.strip())
    has_tags = bool(head_meta_html.strip())
    has_head_content = has_description or has_tags
    return (
        title_html,
        resolved_selected_slug,
        gr.update(visible=has_source and has_head_content),
        gr.update(value=head_meta_html, visible=has_source and has_tags),
        gr.update(value=head_description_markdown, visible=has_source and has_description),
        gr.update(value=files_html, visible=True),
        gr.update(visible=has_source),
        "",
    )


def _open_source_editor_for_individual(selected_source_slug: str):
    normalized_slug = str(selected_source_slug or "").strip().lower()
    source, files = _fetch_source_and_files(normalized_slug)
    default_preview_mode = _is_source_markdown_preview_mode(DEFAULT_MARKDOWN_VIEW)
    if source is None:
        return (
            gr.update(visible=False),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            "❌ Select a valid source first.",
        )

    source_markdown = str(source.get("description_markdown") or "").strip()
    tags_value = ", ".join(
        str(tag).strip()
        for tag in source.get("tags", [])
        if str(tag).strip()
    )
    delete_editor_markup = _render_source_edit_delete_files_editor(files)
    return (
        gr.update(visible=True),
        gr.update(value=str(source.get("name") or "").strip()),
        gr.update(value=source_markdown, visible=not default_preview_mode),
        gr.update(value=_render_source_description_markdown(source_markdown), visible=default_preview_mode),
        gr.update(value=DEFAULT_MARKDOWN_VIEW),
        gr.update(value=None),
        gr.update(value=""),
        gr.update(value=_render_cover_image_preview_markup(str(source.get("cover_media_url") or "").strip())),
        gr.update(value=tags_value),
        gr.update(value=None),
        gr.update(value="", visible=False),
        gr.update(value=""),
        gr.update(value=delete_editor_markup, visible=bool(files)),
        gr.update(value=""),
        "",
    )


def _cancel_source_editor_for_individual(selected_source_slug: str):
    normalized_slug = str(selected_source_slug or "").strip().lower()
    source, files = _fetch_source_and_files(normalized_slug)
    default_preview_mode = _is_source_markdown_preview_mode(DEFAULT_MARKDOWN_VIEW)
    if source is None:
        return (
            gr.update(visible=False),
            gr.update(value=""),
            gr.update(value="", visible=not default_preview_mode),
            gr.update(value="", visible=default_preview_mode),
            gr.update(value=DEFAULT_MARKDOWN_VIEW),
            gr.update(value=None),
            gr.update(value=""),
            gr.update(value=_render_cover_image_preview_markup("")),
            gr.update(value=""),
            gr.update(value=None),
            gr.update(value="", visible=False),
            gr.update(value=""),
            gr.update(value=_render_source_edit_delete_files_editor([]), visible=False),
            gr.update(value=""),
            "",
        )

    source_markdown = str(source.get("description_markdown") or "").strip()
    tags_value = ", ".join(
        str(tag).strip()
        for tag in source.get("tags", [])
        if str(tag).strip()
    )
    return (
        gr.update(visible=False),
        gr.update(value=str(source.get("name") or "").strip()),
        gr.update(value=source_markdown, visible=not default_preview_mode),
        gr.update(value=_render_source_description_markdown(source_markdown), visible=default_preview_mode),
        gr.update(value=DEFAULT_MARKDOWN_VIEW),
        gr.update(value=None),
        gr.update(value=""),
        gr.update(value=_render_cover_image_preview_markup(str(source.get("cover_media_url") or "").strip())),
        gr.update(value=tags_value),
        gr.update(value=None),
        gr.update(value="", visible=False),
        gr.update(value=""),
        gr.update(value=_render_source_edit_delete_files_editor(files), visible=bool(files)),
        gr.update(value=""),
        "",
    )


def _save_source_editor_for_individual(
    selected_source_slug: str,
    source_name: str,
    source_description_markdown: str,
    source_cover_media: object,
    source_cover_media_data: str,
    source_tags: str,
    source_new_files: object,
    source_new_file_origins: object,
    source_delete_file_ids: object,
    file_view_mode: str,
    request: gr.Request,
):
    normalized_slug = str(selected_source_slug or "").strip().lower()
    uploaded_blob_refs: List[Tuple[str, str]] = []
    source_id = 0
    actor_user_id = 0
    auto_accept = False
    proposal_id = 0
    uploaded_file_count = 0
    uploaded_bytes = 0
    deleted_file_count = 0
    proposal_warning = ""

    base_source, base_files = _fetch_source_and_files(normalized_slug)
    base_payload = _serialize_source_snapshot(base_source, base_files)
    default_preview_mode = _is_source_markdown_preview_mode(DEFAULT_MARKDOWN_VIEW)

    name_value = str(source_name or "").strip()
    description_markdown_value = str(source_description_markdown or "").strip()
    tags_value = _parse_tags_input(source_tags)
    cover_upload_path = _extract_upload_path(source_cover_media)
    cover_data_url = str(source_cover_media_data or "").strip()
    new_file_paths = _resolve_upload_paths(source_new_files)
    delete_file_ids = _parse_source_file_ids(source_delete_file_ids)
    file_origin_values = _resolve_file_origins_for_upload(new_file_paths, source_new_file_origins) if new_file_paths else []

    try:
        user, can_submit = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to edit a source.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")
        if not normalized_slug:
            raise ValueError("Missing source slug.")
        if base_source is None:
            raise ValueError("Selected source was not found.")
        if not name_value:
            raise ValueError("Source name is required.")

        auto_accept = _user_has_editor_privilege(user)
        current_tags = {
            _normalize_tag(str(tag))
            for tag in (base_source.get("tags") or [])
            if _normalize_tag(str(tag))
        }
        requested_tags = {
            _normalize_tag(str(tag))
            for tag in tags_value
            if _normalize_tag(str(tag))
        }
        has_metadata_changes = (
            name_value != str(base_source.get("name") or "").strip()
            or description_markdown_value != str(base_source.get("description_markdown") or "").strip()
            or requested_tags != current_tags
        )
        has_cover_change = bool(cover_data_url or cover_upload_path)
        has_file_changes = bool(new_file_paths or delete_file_ids)
        if not has_metadata_changes and not has_cover_change and not has_file_changes:
            raise ValueError("No source changes detected.")

        with session_scope() as session:
            _ensure_sources_db()
            source_row = session.execute(
                text(
                    """
                    SELECT
                        id,
                        bucket,
                        folder_prefix,
                        max_bytes,
                        COALESCE(cover_media_url, '') AS cover_media_url
                    FROM app.sources_cards
                    WHERE slug = :slug
                    FOR UPDATE
                    """
                ),
                {"slug": normalized_slug},
            ).mappings().one_or_none()

            if source_row is None:
                raise ValueError("Selected source was not found.")

            source_id = int(source_row["id"])
            bucket_value = str(source_row["bucket"] or DEFAULT_SOURCE_BUCKET).strip() or DEFAULT_SOURCE_BUCKET
            folder_prefix = str(source_row["folder_prefix"] or "").strip().strip("/")
            max_bytes = max(1, int(source_row["max_bytes"] or DEFAULT_SOURCE_MAX_BYTES))
            cover_media_value = str(source_row["cover_media_url"] or "").strip()

            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")
            actor_identity = _actor_storage_identity(user, actor_user_id)

            if cover_data_url:
                cover_media_value = _persist_uploaded_source_cover_image_data_url(
                    cover_data_url,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    actor_identity=actor_identity,
                    uploaded_blob_refs=uploaded_blob_refs,
                )
            elif cover_upload_path:
                cover_media_value = _persist_uploaded_source_cover_image(
                    cover_upload_path,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    actor_identity=actor_identity,
                    uploaded_blob_refs=uploaded_blob_refs,
                )

            if delete_file_ids:
                deleted_file_count = _delete_source_files_for_source(
                    session,
                    source_id=source_id,
                    bucket_value=bucket_value,
                    file_ids=delete_file_ids,
                )

            if new_file_paths:
                uploaded_bytes = _store_uploaded_files_for_source(
                    session,
                    source_id=source_id,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    max_bytes=max_bytes,
                    file_paths=new_file_paths,
                    origin_urls=file_origin_values,
                    actor_user_id=actor_user_id,
                    uploaded_blob_refs=uploaded_blob_refs,
                )
                uploaded_file_count = len(new_file_paths)

            session.execute(
                text(
                    """
                    UPDATE app.sources_cards
                    SET name = :name,
                        description_markdown = :description_markdown,
                        cover_media_url = :cover_media_url,
                        updated_at = now()
                    WHERE id = :source_id
                    """
                ),
                {
                    "name": name_value,
                    "description_markdown": description_markdown_value,
                    "cover_media_url": cover_media_value,
                    "source_id": source_id,
                },
            )
            _set_source_tags(session, source_id, tags_value)
    except Exception as exc:  # noqa: BLE001
        for bucket_value, blob_name in uploaded_blob_refs:
            try:
                get_bucket(bucket_value).blob(blob_name).delete()
            except Exception:
                pass
        return (
            f"❌ Could not update source: {exc}",
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
            gr.update(),
        )

    (
        _sources,
        title_html,
        _source_selector_update,
        resolved_selected_slug,
        head_meta_html,
        head_description_markdown,
        files_html,
    ) = _safe_individual_source_updates(normalized_slug, file_view_mode)

    refreshed_source, refreshed_files = _fetch_source_and_files(resolved_selected_slug)
    if refreshed_source is None:
        refreshed_name = name_value
        refreshed_description_markdown = description_markdown_value
        refreshed_cover_media_url = str(base_source.get("cover_media_url") or "").strip() if base_source else ""
        refreshed_tags = ", ".join(tags_value)
        refreshed_delete_editor_markup = _render_source_edit_delete_files_editor([])
        has_existing_files = False
    else:
        refreshed_name = str(refreshed_source.get("name") or "").strip()
        refreshed_description_markdown = str(refreshed_source.get("description_markdown") or "").strip()
        refreshed_cover_media_url = str(refreshed_source.get("cover_media_url") or "").strip()
        refreshed_tags = ", ".join(
            str(tag).strip()
            for tag in refreshed_source.get("tags", [])
            if str(tag).strip()
        )
        has_existing_files = bool(refreshed_files)
        refreshed_delete_editor_markup = _render_source_edit_delete_files_editor(refreshed_files)

    proposed_payload = _serialize_source_snapshot(refreshed_source, refreshed_files)
    if base_payload != proposed_payload and source_id > 0 and actor_user_id > 0:
        note_parts: List[str] = []
        if uploaded_file_count > 0:
            note_parts.append(f"added {uploaded_file_count} file(s)")
        if deleted_file_count > 0:
            note_parts.append(f"deleted {deleted_file_count} file(s)")
        note_value = "Source edit submitted from Sources individual page."
        if note_parts:
            note_value += " Changes: " + ", ".join(note_parts) + "."
        try:
            with session_scope() as session:
                proposal_id = int(
                    session.execute(
                        text(
                            """
                            INSERT INTO app.sources_change_proposals (
                                source_slug,
                                source_id,
                                proposer_user_id,
                                proposal_scope,
                                base_payload,
                                proposed_payload,
                                note,
                                status,
                                reviewed_at,
                                reviewer_user_id,
                                review_note
                            )
                            VALUES (
                                :source_slug,
                                :source_id,
                                :proposer_user_id,
                                :proposal_scope,
                                :base_payload,
                                :proposed_payload,
                                :note,
                                :status,
                                :reviewed_at,
                                :reviewer_user_id,
                                :review_note
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "source_slug": normalized_slug,
                            "source_id": int(source_id),
                            "proposer_user_id": int(actor_user_id),
                            "proposal_scope": PROPOSAL_SCOPE_SOURCE,
                            "base_payload": base_payload,
                            "proposed_payload": proposed_payload,
                            "note": note_value,
                            "status": "accepted" if auto_accept else "pending",
                            "reviewed_at": datetime.utcnow() if auto_accept else None,
                            "reviewer_user_id": int(actor_user_id) if auto_accept else None,
                            "review_note": "Auto-accepted from Sources editor (editor privilege)." if auto_accept else None,
                        },
                    ).scalar_one()
                )
                _record_source_proposal_event(
                    session,
                    proposal_id,
                    event_type="source_proposal_submitted",
                    actor_user_id=actor_user_id,
                    notes=note_value,
                    payload={
                        "source_slug": normalized_slug,
                        "proposal_scope": PROPOSAL_SCOPE_SOURCE,
                    },
                )
                if auto_accept:
                    _record_source_proposal_event(
                        session,
                        proposal_id,
                        event_type="source_proposal_auto_accepted",
                        actor_user_id=actor_user_id,
                        notes="Auto-accepted from Sources editor (editor privilege).",
                        payload={
                            "source_slug": normalized_slug,
                            "proposal_scope": PROPOSAL_SCOPE_SOURCE,
                        },
                    )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Could not persist source change proposal: %s", exc)
            proposal_warning = f" ⚠️ Proposal tracking failed: {exc}"

    if proposal_id > 0:
        if auto_accept:
            status_message = f"✅ Source proposal #{proposal_id} submitted and auto-accepted (Editor privilege)."
        else:
            status_message = f"✅ Source proposal #{proposal_id} submitted. It is pending reviewer verification."
    else:
        status_message = "✅ Source updated."

    change_parts: List[str] = []
    if uploaded_file_count > 0:
        change_parts.append(f"{uploaded_file_count} file(s) added ({_format_bytes(uploaded_bytes)})")
    if deleted_file_count > 0:
        change_parts.append(f"{deleted_file_count} file(s) deleted")
    if change_parts:
        status_message = f"{status_message} {'; '.join(change_parts)}."
    if proposal_warning:
        status_message = f"{status_message}{proposal_warning}"

    has_description = bool(head_description_markdown.strip())
    has_tags = bool(head_meta_html.strip())
    has_head_content = has_description or has_tags
    return (
        status_message,
        title_html,
        gr.update(visible=bool(resolved_selected_slug) and has_head_content),
        gr.update(value=head_meta_html, visible=bool(resolved_selected_slug) and bool(head_meta_html.strip())),
        gr.update(
            value=head_description_markdown,
            visible=bool(resolved_selected_slug) and bool(head_description_markdown.strip()),
        ),
        gr.update(value=files_html, visible=True),
        gr.update(visible=bool(resolved_selected_slug)),
        gr.update(value=refreshed_name),
        gr.update(value=refreshed_description_markdown, visible=not default_preview_mode),
        gr.update(value=_render_source_description_markdown(refreshed_description_markdown), visible=default_preview_mode),
        gr.update(value=DEFAULT_MARKDOWN_VIEW),
        gr.update(value=None),
        gr.update(value=""),
        gr.update(value=_render_cover_image_preview_markup(refreshed_cover_media_url)),
        gr.update(value=refreshed_tags),
        gr.update(value=None),
        gr.update(value="", visible=False),
        gr.update(value=""),
        gr.update(value=refreshed_delete_editor_markup, visible=has_existing_files),
        gr.update(value=""),
    )


def _load_sources_page(request: gr.Request):
    selected_source_slug = _query_param(request, "source").lower()
    selected_tags = _parse_tag_query_values(_query_param(request, "tag"))
    catalog_view_mode = _normalize_catalog_view_mode(_query_param(request, "view"))
    file_view_mode = _normalize_file_view_mode(_query_param(request, "files_view"))

    (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    ) = _safe_dashboard_updates(selected_source_slug, selected_tags, catalog_view_mode, file_view_mode)

    return (
        "<h2>Sources</h2>",
        gr.update(visible=True),
        tag_filter_update,
        tag_selection,
        gr.update(value=catalog_view_mode),
        gr.update(value=file_view_mode),
        sources,
        gr.update(value=catalog_html, visible=True),
        source_selector_update,
        resolved_selected_slug,
        gr.update(value=files_html, visible=True),
        "",
        "",
    )


def _update_sources_catalog_by_tag_filter(
    current_selection: Sequence[object] | None,
    previous_selection: Sequence[object] | None,
    all_sources_state: Sequence[Dict[str, object]] | None,
    catalog_view_mode: str,
):
    sources = _coerce_sources_state(all_sources_state)
    if not sources:
        sources = _fetch_all_sources()

    choices = _build_tag_filter_choices(sources)
    choice_values = _choice_values(choices)
    all_key = _normalize_tag(TAG_FILTER_ALL_OPTION)
    allowed_values = [value for value in choice_values if _normalize_tag(value) != all_key]

    current_norm = {_normalize_tag(value) for value in _normalize_selection(current_selection)}
    previous_norm = {_normalize_tag(value) for value in _normalize_selection(previous_selection)}
    current_filtered = [value for value in allowed_values if _normalize_tag(value) in current_norm]
    previous_filtered = [value for value in allowed_values if _normalize_tag(value) in previous_norm]

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

    filtered_sources = _filter_sources_for_tag_selection(sources, next_selection)
    catalog_html = _render_sources_catalog(filtered_sources, _normalize_catalog_view_mode(catalog_view_mode))

    return dropdown_update, next_selection, gr.update(value=catalog_html, visible=True)


def _rerender_sources_catalog(
    catalog_view_mode: str,
    selected_tags: Sequence[object] | None,
    all_sources_state: Sequence[Dict[str, object]] | None,
):
    sources = _coerce_sources_state(all_sources_state)
    if not sources:
        sources = _fetch_all_sources()
    filtered_sources = _filter_sources_for_tag_selection(sources, selected_tags)
    html_value = _render_sources_catalog(filtered_sources, _normalize_catalog_view_mode(catalog_view_mode))
    return gr.update(value=html_value, visible=True)


def _select_source_for_browser(source_slug: str, file_view_mode: str):
    normalized_slug = str(source_slug or "").strip().lower()
    browser_html = _render_source_files_panel(normalized_slug, _normalize_file_view_mode(file_view_mode))
    return normalized_slug, gr.update(value=browser_html, visible=True)


def _rerender_source_files(selected_source_slug: str, file_view_mode: str):
    browser_html = _render_source_files_panel(
        str(selected_source_slug or "").strip().lower(),
        _normalize_file_view_mode(file_view_mode),
    )
    return gr.update(value=browser_html, visible=True)


def _rerender_source_files_for_individual_page(selected_source_slug: str, file_view_mode: str):
    normalized_slug = str(selected_source_slug or "").strip().lower()
    normalized_view_mode = _normalize_file_view_mode(file_view_mode)
    if not normalized_slug:
        return gr.update(value="<div class='source-empty'>Select a source to browse its uploaded files.</div>", visible=True)

    source, files = _fetch_source_and_files(normalized_slug)
    if source is None:
        return gr.update(value="<div class='source-empty'>The selected source no longer exists.</div>", visible=True)

    return gr.update(value=_render_source_files_content(files, normalized_view_mode), visible=True)


def _refresh_sources_dashboard(
    selected_source_slug: str,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    file_view_mode: str,
):
    return _safe_dashboard_updates(selected_source_slug, selected_tag_values, catalog_view_mode, file_view_mode)


def _refresh_sources_list(
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
):
    return _safe_catalog_updates(selected_tag_values, catalog_view_mode)


def _refresh_sources_individual(
    selected_source_slug: str,
    file_view_mode: str,
):
    (
        sources,
        title_html,
        source_selector_update,
        resolved_selected_slug,
        _head_meta_html,
        _head_description_markdown,
        files_html,
    ) = _safe_individual_source_updates(selected_source_slug, file_view_mode)
    return (
        title_html,
        sources,
        source_selector_update,
        resolved_selected_slug,
        gr.update(value=files_html, visible=True),
    )


def _select_source_for_individual(
    source_slug: str,
    file_view_mode: str,
    all_sources_state: Sequence[Dict[str, object]] | None,
):
    normalized_slug, files_update = _select_source_for_browser(source_slug, file_view_mode)
    sources = _coerce_sources_state(all_sources_state)
    title_html = _source_title_html_for_slug(sources, normalized_slug)
    return (
        title_html,
        normalized_slug,
        files_update,
    )


def _rerender_source_files_for_individual(
    selected_source_slug: str,
    file_view_mode: str,
    all_sources_state: Sequence[Dict[str, object]] | None,
):
    files_update = _rerender_source_files(selected_source_slug, file_view_mode)
    sources = _coerce_sources_state(all_sources_state)
    title_html = _source_title_html_for_slug(sources, selected_source_slug)
    return (
        title_html,
        files_update,
    )


def _create_source_card(
    source_name: str,
    source_description_markdown: str,
    source_cover_media: object,
    source_cover_media_data: str,
    source_tags: str,
    uploaded_files: object,
    source_file_origins: object,
    current_selected_slug: str,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    file_view_mode: str,
    request: gr.Request,
    selected_unsorted_file_ids: object = None,
):
    status_message = ""
    next_selected_slug = str(current_selected_slug or "").strip().lower()
    uploaded_blob_refs: List[Tuple[str, str]] = []

    try:
        user, can_submit = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to create a source.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")

        name_value = str(source_name or "").strip()
        if not name_value:
            raise ValueError("Source name is required.")

        description_markdown_value = str(source_description_markdown or "").strip()
        cover_upload_path = _extract_upload_path(source_cover_media)
        cover_data_url = str(source_cover_media_data or "").strip()
        tag_values = _parse_tags_input(source_tags)
        file_paths = _resolve_upload_paths(uploaded_files)

        selected_unsorted_ids = _parse_unsorted_file_ids(selected_unsorted_file_ids)
        if not selected_unsorted_ids:
            selected_unsorted_ids = sorted(
                {
                    _parse_unsorted_id_from_origin_key(str(file_label or "").strip())
                    for file_label, _origin_value, _display in _coerce_file_origin_rows(source_file_origins)
                    if _parse_unsorted_id_from_origin_key(str(file_label or "").strip()) > 0
                }
            )

        with session_scope() as session:
            _ensure_sources_db()
            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

            unsorted_rows = _fetch_unsorted_files_by_ids(session, selected_unsorted_ids)
            if selected_unsorted_ids and len(unsorted_rows) != len(selected_unsorted_ids):
                found_ids = {int(row.get("id") or 0) for row in unsorted_rows}
                missing = [str(file_id) for file_id in selected_unsorted_ids if file_id not in found_ids]
                missing_text = ", ".join(missing)
                raise ValueError(f"Some selected unsorted files no longer exist: {missing_text}")

            file_paths, unsorted_rows = _filter_source_create_selected_files(
                file_paths,
                unsorted_rows,
                source_file_origins,
            )
            if not file_paths and not unsorted_rows:
                raise ValueError("Add at least one file before creating a source.")

            file_origin_values, unsorted_origin_values = _resolve_source_create_origin_values(
                file_paths,
                unsorted_rows,
                source_file_origins,
            )

            slug = _next_available_source_slug(session, name_value)
            folder_prefix = _build_source_folder_prefix(slug)
            bucket_value = DEFAULT_SOURCE_BUCKET
            actor_identity = _actor_storage_identity(user, actor_user_id)

            cover_media_value = ""
            if cover_data_url:
                cover_media_value = _persist_uploaded_source_cover_image_data_url(
                    cover_data_url,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    actor_identity=actor_identity,
                    uploaded_blob_refs=uploaded_blob_refs,
                )
            elif cover_upload_path:
                cover_media_value = _persist_uploaded_source_cover_image(
                    cover_upload_path,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    actor_identity=actor_identity,
                    uploaded_blob_refs=uploaded_blob_refs,
                )

            inserted = session.execute(
                text(
                    """
                    INSERT INTO app.sources_cards (
                        slug,
                        name,
                        description_markdown,
                        cover_media_url,
                        bucket,
                        folder_prefix,
                        max_bytes,
                        created_by_user_id
                    )
                    VALUES (
                        :slug,
                        :name,
                        :description_markdown,
                        :cover_media_url,
                        :bucket,
                        :folder_prefix,
                        :max_bytes,
                        :created_by_user_id
                    )
                    RETURNING id, slug
                    """
                ),
                {
                    "slug": slug,
                    "name": name_value,
                    "description_markdown": description_markdown_value,
                    "cover_media_url": cover_media_value,
                    "bucket": bucket_value,
                    "folder_prefix": folder_prefix,
                    "max_bytes": DEFAULT_SOURCE_MAX_BYTES,
                    "created_by_user_id": actor_user_id,
                },
            ).mappings().one()

            source_id = int(inserted["id"])
            _set_source_tags(session, source_id, tag_values)
            incoming_uploaded_bytes = 0
            if file_paths:
                incoming_uploaded_bytes = _store_uploaded_files_for_source(
                    session,
                    source_id=source_id,
                    bucket_value=bucket_value,
                    folder_prefix=folder_prefix,
                    max_bytes=DEFAULT_SOURCE_MAX_BYTES,
                    file_paths=file_paths,
                    origin_urls=file_origin_values,
                    actor_user_id=actor_user_id,
                    uploaded_blob_refs=uploaded_blob_refs,
                )
            incoming_unsorted_bytes = _store_unsorted_files_for_source(
                session,
                source_id=source_id,
                source_slug=slug,
                bucket_value=bucket_value,
                folder_prefix=folder_prefix,
                max_bytes=DEFAULT_SOURCE_MAX_BYTES,
                unsorted_files=unsorted_rows,
                origin_urls=unsorted_origin_values,
                actor_user_id=actor_user_id,
                uploaded_blob_refs=uploaded_blob_refs,
            )

            next_selected_slug = str(inserted["slug"])

        total_file_count = len(file_paths) + len(unsorted_rows)
        total_incoming_bytes = incoming_uploaded_bytes + incoming_unsorted_bytes
        status_message = (
            f"✅ Source `{name_value}` created and {total_file_count} file(s) added "
            f"({_format_bytes(total_incoming_bytes)})."
        )
        clear_name = gr.update(value="")
        clear_description_markdown = gr.update(value="")
        clear_cover_media = gr.update(value=None)
        clear_cover_media_data = gr.update(value="")
        clear_cover_media_preview = gr.update(value=_render_cover_image_preview_markup(""))
        clear_tags = gr.update(value="")
        clear_origins = gr.update(value="")
        clear_origins_editor = gr.update(value="", visible=False)
        clear_files = gr.update(value=None)
    except Exception as exc:  # noqa: BLE001
        for bucket_value, blob_name in uploaded_blob_refs:
            try:
                get_bucket(bucket_value).blob(blob_name).delete()
            except Exception:
                pass
        status_message = f"❌ Could not create source: {exc}"
        clear_name = gr.update()
        clear_description_markdown = gr.update()
        clear_cover_media = gr.update()
        clear_cover_media_data = gr.update()
        clear_cover_media_preview = gr.update()
        clear_tags = gr.update()
        clear_origins = gr.update()
        clear_origins_editor = gr.update()
        clear_files = gr.update()

    (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    ) = _safe_dashboard_updates(next_selected_slug, selected_tag_values, catalog_view_mode, file_view_mode)

    return (
        status_message,
        clear_name,
        clear_description_markdown,
        clear_cover_media,
        clear_cover_media_data,
        clear_cover_media_preview,
        clear_tags,
        clear_files,
        clear_origins,
        clear_origins_editor,
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    )


def _upload_source_files(
    selected_source_slug: str,
    uploaded_files: object,
    uploaded_file_origins: object,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    file_view_mode: str,
    request: gr.Request,
):
    normalized_slug = str(selected_source_slug or "").strip().lower()
    uploaded_blob_refs: List[Tuple[str, str]] = []
    source_name = normalized_slug
    file_paths: List[Path] = []

    try:
        user, can_submit = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to upload files.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")
        if not normalized_slug:
            raise ValueError("Create or select a source before uploading files.")

        file_paths = _resolve_upload_paths(uploaded_files)
        file_origin_values = _resolve_file_origins_for_upload(file_paths, uploaded_file_origins)

        with session_scope() as session:
            _ensure_sources_db()
            source_row = session.execute(
                text(
                    """
                    SELECT
                        id,
                        name,
                        bucket,
                        folder_prefix,
                        max_bytes
                    FROM app.sources_cards
                    WHERE slug = :slug
                    FOR UPDATE
                    """
                ),
                {"slug": normalized_slug},
            ).mappings().one_or_none()

            if source_row is None:
                raise ValueError("Selected source was not found.")

            source_id = int(source_row["id"])
            source_name = str(source_row["name"] or normalized_slug)
            bucket_value = str(source_row["bucket"] or DEFAULT_SOURCE_BUCKET).strip() or DEFAULT_SOURCE_BUCKET
            folder_prefix = str(source_row["folder_prefix"] or "").strip().strip("/")
            max_bytes = max(1, int(source_row["max_bytes"] or DEFAULT_SOURCE_MAX_BYTES))

            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

            incoming_bytes = _store_uploaded_files_for_source(
                session,
                source_id=source_id,
                bucket_value=bucket_value,
                folder_prefix=folder_prefix,
                max_bytes=max_bytes,
                file_paths=file_paths,
                origin_urls=file_origin_values,
                actor_user_id=actor_user_id,
                uploaded_blob_refs=uploaded_blob_refs,
            )

        status_message = (
            f"✅ Uploaded {len(file_paths)} file(s) to `{source_name}` "
            f"({_format_bytes(incoming_bytes)})."
        )
    except Exception as exc:  # noqa: BLE001
        for bucket_value, blob_name in uploaded_blob_refs:
            try:
                get_bucket(bucket_value).blob(blob_name).delete()
            except Exception:
                pass
        status_message = f"❌ Could not upload files: {exc}"

    (
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    ) = _safe_dashboard_updates(normalized_slug, selected_tag_values, catalog_view_mode, file_view_mode)

    return (
        status_message,
        gr.update(value=None),
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    )


def _create_source_card_for_list(
    source_name: str,
    source_description_markdown: str,
    source_cover_media: object,
    source_cover_media_data: str,
    source_tags: str,
    uploaded_files: object,
    source_file_origins: object,
    selected_tag_values: Sequence[object] | None,
    catalog_view_mode: str,
    request: gr.Request,
):
    (
        status_message,
        clear_name,
        clear_description_markdown,
        clear_cover_media,
        clear_cover_media_data,
        clear_cover_media_preview,
        clear_tags,
        clear_files,
        clear_origins,
        clear_origins_editor,
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
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
        selected_tag_values,
        catalog_view_mode,
        FILE_VIEW_ICONS,
        request,
    )

    return (
        status_message,
        clear_name,
        clear_description_markdown,
        clear_cover_media,
        clear_cover_media_data,
        clear_cover_media_preview,
        clear_tags,
        clear_files,
        clear_origins,
        clear_origins_editor,
        tag_filter_update,
        tag_selection,
        sources,
        catalog_html,
    )


def _upload_source_files_for_individual(
    selected_source_slug: str,
    uploaded_files: object,
    uploaded_file_origins: object,
    file_view_mode: str,
    request: gr.Request,
):
    (
        status_message,
        clear_upload_input,
        _tag_filter_update,
        _tag_selection,
        sources,
        _catalog_html,
        source_selector_update,
        resolved_selected_slug,
        files_html,
    ) = _upload_source_files(
        selected_source_slug,
        uploaded_files,
        uploaded_file_origins,
        None,
        CATALOG_VIEW_ICONS,
        file_view_mode,
        request,
    )
    title_html = _source_title_html_for_slug(sources, resolved_selected_slug)

    return (
        status_message,
        clear_upload_input,
        title_html,
        sources,
        source_selector_update,
        resolved_selected_slug,
        gr.update(value=files_html, visible=True),
    )
