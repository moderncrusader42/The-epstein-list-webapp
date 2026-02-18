from __future__ import annotations

import html
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
from src.pages.sources_list.core_sources import _ensure_sources_db

logger = logging.getLogger(__name__)

ACTION_TOO_REDACTED = "too_redacted"
ACTION_PUSH_TO_SOURCE = "push_to_source"
ACTION_CREATE_NEW_SOURCE = "create_new_source"
ACTION_USELESS = "useless"

_ACTION_VALUES = {
    ACTION_TOO_REDACTED,
    ACTION_PUSH_TO_SOURCE,
    ACTION_CREATE_NEW_SOURCE,
    ACTION_USELESS,
}
_ACTION_LABELS = {
    ACTION_TOO_REDACTED: "Too redacted",
    ACTION_PUSH_TO_SOURCE: "Push to source",
    ACTION_CREATE_NEW_SOURCE: "Create new source",
    ACTION_USELESS: "Useless",
}

TRUE_VALUES = {"1", "true", "yes", "on"}
_RUNTIME_BOOTSTRAP_DEFAULT = "0" if os.getenv("INSTANCE_CONNECTION_NAME") else "1"
RUNTIME_SCHEMA_BOOTSTRAP = (
    str(os.getenv("THE_LIST_RUNTIME_SCHEMA_BOOTSTRAP", _RUNTIME_BOOTSTRAP_DEFAULT)).strip().lower() in TRUE_VALUES
)

DEFAULT_BUCKET = (os.getenv("BUCKET_NAME") or configured_bucket_name() or "media-db-dev").strip() or "media-db-dev"
UNSORTED_MEDIA_PREFIX = (os.getenv("UNSORTED_FILES_MEDIA_PREFIX") or "unsorted-files").strip("/ ")

SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

_DB_INIT_LOCK = threading.Lock()
_DB_INIT_DONE = False


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in TRUE_VALUES


def _sanitize_filename(name: str) -> str:
    cleaned = SAFE_FILENAME_RE.sub("-", str(name or "").strip())
    cleaned = cleaned.strip(" .-")
    if not cleaned:
        return ""
    return cleaned[:220]


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


def _extract_upload_original_label(uploaded_file: object, path_obj: Path) -> str:
    if isinstance(uploaded_file, dict):
        for key in ("orig_name", "origName", "name"):
            value = uploaded_file.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    for attr in ("orig_name", "origName", "name"):
        value = getattr(uploaded_file, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return path_obj.name


def _normalize_original_path(raw_value: str, fallback_name: str) -> str:
    candidate = str(raw_value or "").replace("\\", "/").strip()
    if not candidate:
        return str(fallback_name or "").strip()

    while candidate.startswith("./"):
        candidate = candidate[2:]
    candidate = candidate.lstrip("/")
    if not candidate:
        return str(fallback_name or "").strip()

    candidate_path = Path(candidate)
    if candidate_path.is_absolute():
        return candidate_path.name or str(fallback_name or "").strip()
    return candidate[:400]


def _resolve_upload_entries(uploaded_files: object) -> List[Tuple[Path, str]]:
    if uploaded_files is None:
        return []

    if isinstance(uploaded_files, (list, tuple)):
        candidates = list(uploaded_files)
    else:
        candidates = [uploaded_files]

    entries: List[Tuple[Path, str]] = []
    seen_paths: set[str] = set()
    for candidate in candidates:
        upload_path = _extract_upload_path(candidate)
        if not upload_path:
            continue
        path_obj = Path(upload_path)
        if not path_obj.is_file():
            continue
        resolved_path = str(path_obj.resolve())
        if resolved_path in seen_paths:
            continue
        seen_paths.add(resolved_path)
        original_label = _extract_upload_original_label(candidate, path_obj)
        normalized_original = _normalize_original_path(original_label, path_obj.name)
        entries.append((path_obj, normalized_original))

    return entries


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


def _is_pdf_mime(mime_value: str) -> bool:
    return mime_value == "application/pdf" or mime_value.endswith("/pdf")


def _render_media_preview(media_url: str, mime_type: str, file_name: str) -> str:
    safe_url = html.escape(str(media_url or ""), quote=True)
    safe_name = html.escape(str(file_name or "file"))
    resolved_mime = _resolve_mime_type(mime_type, file_name, media_url)

    if safe_url and resolved_mime.startswith("image/"):
        return f"<img class='source-preview' src='{safe_url}' alt='{safe_name}' loading='lazy' />"

    if safe_url and resolved_mime.startswith("video/"):
        return (
            f"<video class='source-preview' src='{safe_url}' controls preload='metadata' "
            "playsinline></video>"
        )

    if safe_url and _is_pdf_mime(resolved_mime):
        return (
            "<iframe class='source-preview source-preview--pdf' "
            f"src='{safe_url}#toolbar=0&navpanes=0&scrollbar=0' title='{safe_name}' loading='lazy'></iframe>"
        )

    extension = Path(str(file_name or "")).suffix.lower().lstrip(".")
    extension_label = html.escape(extension.upper() if extension else "FILE")
    return (
        "<div class='source-preview source-preview--file'>"
        f"<span>{extension_label}</span>"
        "</div>"
    )


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


def _role_flags_from_request(request: gr.Request | None) -> tuple[Dict[str, object], bool, bool]:
    user = get_user(request) or {}
    privileges = user.get("privileges") or {}
    can_submit = _is_truthy(privileges.get("base_user"))
    is_admin = _is_truthy(privileges.get("admin")) or _is_truthy(privileges.get("creator"))
    return user, can_submit, is_admin


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


def _ensure_unsorted_db() -> None:
    global _DB_INIT_DONE
    if _DB_INIT_DONE:
        return

    with _DB_INIT_LOCK:
        if _DB_INIT_DONE:
            return
        if not RUNTIME_SCHEMA_BOOTSTRAP:
            _DB_INIT_DONE = True
            return
        _ensure_unsorted_db_once()
        _DB_INIT_DONE = True


def _ensure_unsorted_db_once() -> None:
    _ensure_sources_db()

    with session_scope() as session:
        session.execute(text("CREATE SCHEMA IF NOT EXISTS app"))

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.unsorted_files (
                    id BIGSERIAL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    blob_path TEXT NOT NULL UNIQUE,
                    file_name TEXT NOT NULL,
                    original_path TEXT NOT NULL DEFAULT '',
                    origin_text TEXT NOT NULL DEFAULT '',
                    mime_type TEXT,
                    size_bytes BIGINT NOT NULL DEFAULT 0,
                    uploaded_by_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT chk_unsorted_files_size_bytes CHECK (size_bytes >= 0)
                )
                """
            )
        )

        session.execute(text("ALTER TABLE app.unsorted_files ADD COLUMN IF NOT EXISTS original_path TEXT NOT NULL DEFAULT ''"))
        session.execute(text("ALTER TABLE app.unsorted_files ADD COLUMN IF NOT EXISTS origin_text TEXT NOT NULL DEFAULT ''"))
        session.execute(text("ALTER TABLE app.unsorted_files ADD COLUMN IF NOT EXISTS mime_type TEXT"))
        session.execute(text("ALTER TABLE app.unsorted_files ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"))

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.unsorted_file_actions (
                    id BIGSERIAL PRIMARY KEY,
                    unsorted_file_id BIGINT NOT NULL REFERENCES app.unsorted_files(id) ON DELETE CASCADE,
                    actor_user_id BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    action_type TEXT NOT NULL,
                    source_id BIGINT REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    source_slug TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT chk_unsorted_file_action_type CHECK (
                        lower(action_type) IN ('too_redacted', 'push_to_source', 'create_new_source', 'useless')
                    ),
                    UNIQUE (unsorted_file_id, actor_user_id)
                )
                """
            )
        )

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.unsorted_file_push_proposals (
                    id BIGSERIAL PRIMARY KEY,
                    unsorted_file_id BIGINT NOT NULL REFERENCES app.unsorted_files(id) ON DELETE CASCADE,
                    source_id BIGINT NOT NULL REFERENCES app.sources_cards(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    source_slug TEXT NOT NULL,
                    proposer_user_id BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    reviewed_at TIMESTAMPTZ,
                    CONSTRAINT chk_unsorted_push_status CHECK (
                        lower(status) IN ('pending', 'accepted', 'declined')
                    ),
                    UNIQUE (unsorted_file_id, source_id, proposer_user_id)
                )
                """
            )
        )

        session.execute(text("CREATE INDEX IF NOT EXISTS idx_unsorted_files_created_at ON app.unsorted_files(created_at)"))
        session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_unsorted_files_uploaded_by ON app.unsorted_files(uploaded_by_user_id)")
        )
        session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_unsorted_actions_file_id ON app.unsorted_file_actions(unsorted_file_id)")
        )
        session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_unsorted_actions_actor ON app.unsorted_file_actions(actor_user_id)")
        )
        session.execute(
            text("CREATE INDEX IF NOT EXISTS idx_unsorted_actions_type ON app.unsorted_file_actions(action_type)")
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_unsorted_push_proposals_file_source "
                "ON app.unsorted_file_push_proposals(unsorted_file_id, source_id)"
            )
        )


def _coerce_file_id(raw_value: object) -> int:
    try:
        parsed = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _normalize_action(raw_value: object) -> str:
    normalized = str(raw_value or "").strip().lower()
    if normalized in _ACTION_VALUES:
        return normalized
    return ""


def _fetch_source_choices() -> List[Tuple[str, str]]:
    _ensure_unsorted_db()

    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "sources_cards"):
            return []

        rows = session.execute(
            text(
                """
                SELECT
                    slug,
                    name
                FROM app.sources_cards
                ORDER BY lower(name), id
                """
            )
        ).mappings().all()

    choices: List[Tuple[str, str]] = []
    for row in rows:
        slug = str(row.get("slug") or "").strip().lower()
        name = str(row.get("name") or "").strip() or slug
        if not slug:
            continue
        choices.append((name, slug))
    return choices


def _fetch_unsorted_files(actor_user_id: int) -> List[Dict[str, object]]:
    _ensure_unsorted_db()

    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "unsorted_files"):
            return []

        rows = session.execute(
            text(
                """
                WITH
                useless_counts AS (
                    SELECT
                        ufa.unsorted_file_id,
                        COUNT(*)::bigint AS useless_count
                    FROM app.unsorted_file_actions ufa
                    WHERE lower(ufa.action_type) = 'useless'
                    GROUP BY ufa.unsorted_file_id
                ),
                user_action AS (
                    SELECT DISTINCT ON (ufa.unsorted_file_id)
                        ufa.unsorted_file_id,
                        ufa.action_type,
                        COALESCE(ufa.source_slug, '') AS source_slug,
                        ufa.updated_at
                    FROM app.unsorted_file_actions ufa
                    WHERE ufa.actor_user_id = :actor_user_id
                    ORDER BY ufa.unsorted_file_id, ufa.updated_at DESC, ufa.id DESC
                )
                SELECT
                    uf.id,
                    uf.bucket,
                    uf.blob_path,
                    uf.file_name,
                    COALESCE(uf.original_path, '') AS original_path,
                    COALESCE(uf.origin_text, '') AS origin_text,
                    COALESCE(uf.mime_type, '') AS mime_type,
                    COALESCE(uf.size_bytes, 0)::bigint AS size_bytes,
                    uf.created_at,
                    COALESCE(uc.useless_count, 0)::bigint AS useless_count,
                    COALESCE(ua.action_type, '') AS user_action,
                    COALESCE(ua.source_slug, '') AS user_source_slug
                FROM app.unsorted_files uf
                LEFT JOIN useless_counts uc
                    ON uc.unsorted_file_id = uf.id
                LEFT JOIN user_action ua
                    ON ua.unsorted_file_id = uf.id
                ORDER BY uf.created_at DESC, uf.id DESC
                """
            ),
            {"actor_user_id": int(max(0, actor_user_id))},
        ).mappings().all()

    files: List[Dict[str, object]] = []
    for row in rows:
        file_name = str(row.get("file_name") or "").strip() or "file"
        original_path = str(row.get("original_path") or "").strip()
        blob_path = str(row.get("blob_path") or "").strip()
        media_url = media_path(blob_path)

        files.append(
            {
                "id": int(row.get("id") or 0),
                "bucket": str(row.get("bucket") or "").strip(),
                "blob_path": blob_path,
                "media_url": media_url,
                "file_name": file_name,
                "original_path": original_path,
                "origin_text": str(row.get("origin_text") or "").strip(),
                "mime_type": _resolve_mime_type(row.get("mime_type"), file_name, media_url),
                "size_bytes": int(row.get("size_bytes") or 0),
                "created_at": row.get("created_at"),
                "useless_count": int(row.get("useless_count") or 0),
                "user_action": _normalize_action(row.get("user_action")),
                "user_source_slug": str(row.get("user_source_slug") or "").strip().lower(),
            }
        )

    return files


def _query_param(request: gr.Request | None, name: str) -> str:
    if request is None:
        return ""
    return str(request.query_params.get(name, "")).strip()


def _unsorted_type_badge(mime_type: str, file_name: str) -> str:
    resolved = _resolve_mime_type(mime_type, file_name, "")
    extension = Path(str(file_name or "")).suffix.lower().lstrip(".")
    if _is_pdf_mime(resolved):
        return "PDF"
    if resolved.startswith("image/"):
        return "IMG"
    if resolved.startswith("video/"):
        return "VID"
    if resolved.startswith("text/"):
        return "TXT"
    if extension:
        return extension[:4].upper()
    return "FILE"


def _unsorted_type_label(mime_type: str, file_name: str) -> str:
    resolved = _resolve_mime_type(mime_type, file_name, "")
    if _is_pdf_mime(resolved):
        return "PDF document"
    if resolved.startswith("image/"):
        return "Image"
    if resolved.startswith("video/"):
        return "Video"
    if resolved.startswith("text/"):
        return "Text file"
    extension = Path(str(file_name or "")).suffix.lower().lstrip(".")
    if extension:
        return f"{extension.upper()} file"
    return "File"


def _unsorted_uploaded_label(created_at: object) -> str:
    if isinstance(created_at, datetime):
        return created_at.strftime("%Y-%m-%d %H:%M")
    return "-"


def _unsorted_file_href(file_id: int) -> str:
    normalized = _coerce_file_id(file_id)
    if normalized <= 0:
        return "/unsorted-files/"
    return f"/unsorted-files/?file={quote(str(normalized), safe='')}"


def _render_unsorted_explorer(files: Sequence[Dict[str, object]] | None) -> str:
    rows = list(files or [])
    if not rows:
        return (
            "<section class='unsorted-browser unsorted-browser--empty'>"
            "<h3>No unsorted files yet</h3>"
            "<p>Use Upload to add files, then review them from here.</p>"
            "</section>"
        )

    list_rows: List[str] = []
    grid_cards: List[str] = []
    for row in rows:
        file_id = int(row.get("id") or 0)
        file_name = str(row.get("file_name") or "file").strip() or "file"
        mime_type = str(row.get("mime_type") or "").strip()
        created_label = _unsorted_uploaded_label(row.get("created_at"))
        size_label = _format_bytes(int(row.get("size_bytes") or 0))
        type_label = _unsorted_type_label(mime_type, file_name)
        badge_label = _unsorted_type_badge(mime_type, file_name)
        href = _unsorted_file_href(file_id)

        safe_href = html.escape(href, quote=True)
        safe_name = html.escape(file_name)
        safe_type = html.escape(type_label)
        safe_badge = html.escape(badge_label)
        safe_size = html.escape(size_label)
        safe_created = html.escape(created_label)

        list_rows.append(
            "<a class='unsorted-browser__row' "
            f"href='{safe_href}' title='Open {safe_name}'>"
            "<span class='unsorted-browser__name'>"
            f"<span class='unsorted-browser__badge'>{safe_badge}</span>"
            f"<span class='unsorted-browser__name-text'>{safe_name}</span>"
            "</span>"
            f"<span class='unsorted-browser__type'>{safe_type}</span>"
            f"<span class='unsorted-browser__size'>{safe_size}</span>"
            f"<span class='unsorted-browser__date'>{safe_created}</span>"
            "</a>"
        )

        grid_cards.append(
            "<a class='unsorted-browser__tile' "
            f"href='{safe_href}' title='Open {safe_name}'>"
            f"<span class='unsorted-browser__tile-badge'>{safe_badge}</span>"
            f"<span class='unsorted-browser__tile-name'>{safe_name}</span>"
            f"<span class='unsorted-browser__tile-meta'>{safe_type} • {safe_size}</span>"
            "</a>"
        )

    return (
        "<section class='unsorted-browser'>"
        "<div class='unsorted-browser__toolbar'>"
        "<div class='unsorted-browser__title'>"
        f"<strong>{len(rows)} file(s)</strong>"
        "<span>Choose a file to open the review workspace.</span>"
        "</div>"
        "<div class='unsorted-browser__view-switch'>"
        "<input type='radio' id='unsorted-view-list' name='unsorted-view-mode' checked>"
        "<label for='unsorted-view-list'>List</label>"
        "<input type='radio' id='unsorted-view-icons' name='unsorted-view-mode'>"
        "<label for='unsorted-view-icons'>Icons</label>"
        "</div>"
        "</div>"
        "<div class='unsorted-browser__surface'>"
        "<div class='unsorted-browser__list-header'>"
        "<span>Name</span><span>Type</span><span>Size</span><span>Uploaded</span>"
        "</div>"
        "<div class='unsorted-browser__list'>"
        f"{''.join(list_rows)}"
        "</div>"
        "<div class='unsorted-browser__grid'>"
        f"{''.join(grid_cards)}"
        "</div>"
        "</div>"
        "</section>"
    )


def _render_unsorted_file_preview(file_row: Dict[str, object] | None) -> str:
    if not isinstance(file_row, dict):
        return "<div class='source-empty'>No unsorted files uploaded yet.</div>"

    file_name = str(file_row.get("file_name") or "file").strip() or "file"
    media_url = str(file_row.get("media_url") or "").strip()
    mime_type = str(file_row.get("mime_type") or "").strip()
    preview_markup = _render_media_preview(media_url, mime_type, file_name)
    preview_class = "unsorted-preview-card"
    if _is_pdf_mime(_resolve_mime_type(mime_type, file_name, media_url)):
        preview_class += " unsorted-preview-card--pdf"
    return f"<section class='{preview_class}'>{preview_markup}</section>"


def _render_unsorted_file_meta(file_row: Dict[str, object] | None) -> str:
    if not isinstance(file_row, dict):
        return ""

    file_name = str(file_row.get("file_name") or "file").strip() or "file"
    media_url = str(file_row.get("media_url") or "").strip()
    mime_type = str(file_row.get("mime_type") or "").strip()
    origin_text = str(file_row.get("origin_text") or "").strip()
    size_bytes = int(file_row.get("size_bytes") or 0)
    created_at = file_row.get("created_at")
    created_label = _unsorted_uploaded_label(created_at)
    type_label = _unsorted_type_label(mime_type, file_name)
    size_label = _format_bytes(size_bytes)

    return (
        "<section class='unsorted-file-meta'>"
        f"<h3>{html.escape(file_name)}</h3>"
        f"<p><strong>Origin/Description:</strong> {_render_origin_value(origin_text)}</p>"
        f"<p><strong>Type:</strong> {html.escape(type_label)} | <strong>Size:</strong> {html.escape(size_label)}</p>"
        f"<p><strong>Uploaded:</strong> {html.escape(created_label or '-')}</p>"
        "<p><a class='source-table__link' href='/unsorted-files/'>Back to files</a></p>"
        f"<p><a class='source-table__link' href='{html.escape(media_url, quote=True)}' target='_blank' rel='noopener'>Open file in new tab</a></p>"
        "</section>"
    )


def _action_summary_markup(file_row: Dict[str, object] | None) -> str:
    if not isinstance(file_row, dict):
        return ""

    user_action = _normalize_action(file_row.get("user_action"))
    if not user_action:
        return ""

    label = _ACTION_LABELS.get(user_action, user_action.replace("_", " ").title())
    if user_action == ACTION_PUSH_TO_SOURCE:
        source_slug = str(file_row.get("user_source_slug") or "").strip()
        if source_slug:
            label = f"{label} (`{source_slug}`)"
    return f"Your current choice: **{label}**"


def _find_index_by_file_id(files: Sequence[Dict[str, object]], file_id: int, fallback_index: int) -> int:
    normalized_id = _coerce_file_id(file_id)
    if normalized_id > 0:
        for idx, row in enumerate(files):
            if int(row.get("id") or 0) == normalized_id:
                return idx

    if not files:
        return 0

    try:
        parsed_fallback = int(fallback_index)
    except (TypeError, ValueError):
        parsed_fallback = 0
    return max(0, min(len(files) - 1, parsed_fallback))


def _build_create_source_link(file_id: int) -> str:
    normalized = _coerce_file_id(file_id)
    target = "/source-create/"
    if normalized > 0:
        target = f"/source-create/?from_unsorted={quote(str(normalized), safe='')}"
    return (
        "<a class='unsorted-action-link' "
        f"href='{html.escape(target, quote=True)}'>"
        "Create new source"
        "</a>"
    )


def _build_viewer_updates(
    files: Sequence[Dict[str, object]] | None,
    requested_index: int,
    *,
    can_interact: bool,
    show_detail: bool,
) -> tuple[
    int,
    int,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
]:
    rows = list(files or [])
    total = len(rows)
    if total <= 0:
        return (
            0,
            0,
            gr.update(value=_render_unsorted_explorer([]), visible=True),
            gr.update(visible=False),
            gr.update(value="<div class='source-empty'>No unsorted files uploaded yet.</div>", visible=True),
            gr.update(value="", visible=False),
            gr.update(value="0 / 0", visible=True),
            gr.update(value="", visible=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(value="Useless (0)", interactive=False),
            gr.update(value=_build_create_source_link(0), visible=False),
        )

    if not show_detail:
        return (
            0,
            0,
            gr.update(value=_render_unsorted_explorer(rows), visible=True),
            gr.update(visible=False),
            gr.update(value="", visible=False),
            gr.update(value="", visible=False),
            gr.update(value=f"0 / {total}", visible=True),
            gr.update(value="", visible=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(interactive=False),
            gr.update(value="Useless (0)", interactive=False),
            gr.update(value=_build_create_source_link(0), visible=False),
        )

    try:
        parsed_index = int(requested_index)
    except (TypeError, ValueError):
        parsed_index = 0
    resolved_index = max(0, min(total - 1, parsed_index))
    selected = rows[resolved_index]
    selected_id = int(selected.get("id") or 0)
    useless_count = max(0, int(selected.get("useless_count") or 0))
    action_summary = _action_summary_markup(selected)
    action_enabled = bool(can_interact)

    return (
        resolved_index,
        selected_id,
        gr.update(value=_render_unsorted_explorer(rows), visible=False),
        gr.update(visible=True),
        gr.update(value=_render_unsorted_file_preview(selected), visible=True),
        gr.update(value=_render_unsorted_file_meta(selected), visible=True),
        gr.update(value=f"{resolved_index + 1} / {total}", visible=True),
        gr.update(value=action_summary, visible=bool(action_summary)),
        gr.update(interactive=resolved_index > 0),
        gr.update(interactive=resolved_index < (total - 1)),
        gr.update(interactive=action_enabled),
        gr.update(interactive=action_enabled),
        gr.update(value=f"Useless ({useless_count})", interactive=action_enabled),
        gr.update(value=_build_create_source_link(selected_id), visible=True),
    )


def _refresh_files_and_view(
    actor_user_id: int,
    *,
    current_file_id: int,
    fallback_index: int,
    can_interact: bool,
    show_detail: bool | None = None,
) -> tuple[
    List[Dict[str, object]],
    int,
    int,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
    gr.update,
]:
    files = _fetch_unsorted_files(actor_user_id)
    open_detail = bool(show_detail)
    if show_detail is None:
        open_detail = _coerce_file_id(current_file_id) > 0
    if not files:
        open_detail = False

    next_index = _find_index_by_file_id(files, current_file_id, fallback_index)
    (
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    ) = _build_viewer_updates(
        files,
        next_index,
        can_interact=can_interact,
        show_detail=open_detail,
    )

    return (
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    )


def _load_unsorted_files_page(request: gr.Request):
    user, can_submit, is_admin = _role_flags_from_request(request)
    actor_user_id = _resolve_request_user_id(user)
    selected_file_id = _coerce_file_id(_query_param(request, "file"))

    (
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    ) = _refresh_files_and_view(
        actor_user_id,
        current_file_id=selected_file_id,
        fallback_index=0,
        can_interact=can_submit,
        show_detail=selected_file_id > 0,
    )

    status_message = ""
    if not user:
        status_message = "You must sign in to review unsorted files."
    elif not can_submit:
        status_message = "Your `base_user` privilege is currently disabled, so actions are blocked."

    return (
        bool(can_submit),
        bool(is_admin),
        gr.update(visible=bool(is_admin)),
        gr.update(visible=False),
        gr.update(value="", visible=False),
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
        gr.update(value=status_message, visible=bool(status_message)),
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(choices=[], value=None, interactive=False),
        gr.update(value=""),
    )


def _next_unsorted_file(files_state: Sequence[Dict[str, object]] | None, current_index: int, can_submit: bool):
    return _build_viewer_updates(
        files_state,
        int(current_index or 0) + 1,
        can_interact=bool(can_submit),
        show_detail=True,
    )


def _previous_unsorted_file(files_state: Sequence[Dict[str, object]] | None, current_index: int, can_submit: bool):
    return _build_viewer_updates(
        files_state,
        int(current_index or 0) - 1,
        can_interact=bool(can_submit),
        show_detail=True,
    )


def _upsert_user_action(
    session,
    *,
    unsorted_file_id: int,
    actor_user_id: int,
    action_type: str,
    source_id: int | None = None,
    source_slug: str = "",
) -> None:
    normalized_action = _normalize_action(action_type)
    if not normalized_action:
        raise ValueError("Invalid action type.")

    source_id_value = int(source_id) if source_id else None
    source_slug_value = str(source_slug or "").strip().lower()

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
            ON CONFLICT (unsorted_file_id, actor_user_id) DO UPDATE
            SET action_type = EXCLUDED.action_type,
                source_id = EXCLUDED.source_id,
                source_slug = EXCLUDED.source_slug,
                updated_at = now()
            """
        ),
        {
            "unsorted_file_id": int(unsorted_file_id),
            "actor_user_id": int(actor_user_id),
            "action_type": normalized_action,
            "source_id": source_id_value,
            "source_slug": source_slug_value,
        },
    )


def _mark_unsorted_action(
    action_type: str,
    current_file_id: int,
    current_index: int,
    request: gr.Request,
):
    normalized_action = _normalize_action(action_type)
    normalized_file_id = _coerce_file_id(current_file_id)

    try:
        user, can_submit, _is_admin = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to review unsorted files.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")
        if normalized_file_id <= 0:
            raise ValueError("Select a file first.")

        with session_scope() as session:
            _ensure_unsorted_db()
            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

            exists = session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM app.unsorted_files uf
                        WHERE uf.id = :file_id
                    )
                    """
                ),
                {"file_id": normalized_file_id},
            ).scalar_one()
            if not exists:
                raise ValueError("The selected file no longer exists.")

            _upsert_user_action(
                session,
                unsorted_file_id=normalized_file_id,
                actor_user_id=actor_user_id,
                action_type=normalized_action,
            )

        status_message = f"✅ File marked as **{_ACTION_LABELS.get(normalized_action, normalized_action)}**."
    except Exception as exc:  # noqa: BLE001
        status_message = f"❌ Could not save action: {exc}"

    user, can_submit, _ = _role_flags_from_request(request)
    actor_user_id = _resolve_request_user_id(user)
    (
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    ) = _refresh_files_and_view(
        actor_user_id,
        current_file_id=normalized_file_id,
        fallback_index=int(current_index or 0),
        can_interact=can_submit,
        show_detail=True,
    )

    return (
        gr.update(value=status_message, visible=bool(status_message)),
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    )


def _mark_unsorted_too_redacted(current_file_id: int, current_index: int, request: gr.Request):
    return _mark_unsorted_action(ACTION_TOO_REDACTED, current_file_id, current_index, request)


def _mark_unsorted_useless(current_file_id: int, current_index: int, request: gr.Request):
    return _mark_unsorted_action(ACTION_USELESS, current_file_id, current_index, request)


def _open_unsorted_push_modal(current_file_id: int, request: gr.Request):
    normalized_file_id = _coerce_file_id(current_file_id)

    user, can_submit, _is_admin = _role_flags_from_request(request)
    if not user:
        return (
            gr.update(visible=False),
            gr.update(value="You must sign in to submit a push proposal.", visible=True),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(value=""),
        )
    if not can_submit:
        return (
            gr.update(visible=False),
            gr.update(value="Your `base_user` privilege is disabled.", visible=True),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(value=""),
        )
    if normalized_file_id <= 0:
        return (
            gr.update(visible=False),
            gr.update(value="Select a file first.", visible=True),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(value=""),
        )

    source_choices = _fetch_source_choices()
    if not source_choices:
        return (
            gr.update(visible=False),
            gr.update(value="No sources exist yet. Create one first.", visible=True),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(value=""),
        )

    default_slug = source_choices[0][1]
    return (
        gr.update(visible=True),
        gr.update(value="", visible=False),
        gr.update(choices=source_choices, value=default_slug, interactive=True),
        gr.update(value=""),
    )


def _cancel_unsorted_push_modal():
    return (
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(choices=[], value=None, interactive=False),
        gr.update(value=""),
    )


def _submit_unsorted_push_to_source(
    current_file_id: int,
    selected_source_slug: str,
    push_note: str,
    current_index: int,
    request: gr.Request,
):
    normalized_file_id = _coerce_file_id(current_file_id)
    normalized_source_slug = str(selected_source_slug or "").strip().lower()
    proposal_id = 0

    try:
        user, can_submit, _is_admin = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to submit push proposals.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")
        if normalized_file_id <= 0:
            raise ValueError("Select a file first.")
        if not normalized_source_slug:
            raise ValueError("Select a source first.")

        with session_scope() as session:
            _ensure_unsorted_db()
            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

            source_row = session.execute(
                text(
                    """
                    SELECT
                        id,
                        slug,
                        name
                    FROM app.sources_cards
                    WHERE slug = :slug
                    """
                ),
                {"slug": normalized_source_slug},
            ).mappings().one_or_none()
            if source_row is None:
                raise ValueError("Selected source was not found.")

            file_exists = session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM app.unsorted_files uf
                        WHERE uf.id = :file_id
                    )
                    """
                ),
                {"file_id": normalized_file_id},
            ).scalar_one()
            if not file_exists:
                raise ValueError("Selected unsorted file was not found.")

            source_id = int(source_row.get("id") or 0)
            source_slug = str(source_row.get("slug") or "").strip().lower()
            source_name = str(source_row.get("name") or source_slug).strip() or source_slug

            proposal_id = int(
                session.execute(
                    text(
                        """
                        INSERT INTO app.unsorted_file_push_proposals (
                            unsorted_file_id,
                            source_id,
                            source_slug,
                            proposer_user_id,
                            note,
                            status
                        )
                        VALUES (
                            :unsorted_file_id,
                            :source_id,
                            :source_slug,
                            :proposer_user_id,
                            :note,
                            'pending'
                        )
                        ON CONFLICT (unsorted_file_id, source_id, proposer_user_id) DO UPDATE
                        SET note = EXCLUDED.note,
                            status = 'pending',
                            created_at = now(),
                            reviewed_at = NULL
                        RETURNING id
                        """
                    ),
                    {
                        "unsorted_file_id": normalized_file_id,
                        "source_id": source_id,
                        "source_slug": source_slug,
                        "proposer_user_id": actor_user_id,
                        "note": str(push_note or "").strip(),
                    },
                ).scalar_one()
            )

            _upsert_user_action(
                session,
                unsorted_file_id=normalized_file_id,
                actor_user_id=actor_user_id,
                action_type=ACTION_PUSH_TO_SOURCE,
                source_id=source_id,
                source_slug=source_slug,
            )

        status_message = f"✅ Push proposal #{proposal_id} submitted for source `{source_name}`."
        modal_update = gr.update(visible=False)
        push_status_update = gr.update(value="", visible=False)
        push_dropdown_update = gr.update(choices=[], value=None, interactive=False)
        push_note_update = gr.update(value="")
    except Exception as exc:  # noqa: BLE001
        status_message = f"❌ Could not submit push proposal: {exc}"
        modal_update = gr.update(visible=True)
        push_status_update = gr.update(value=str(exc), visible=True)
        push_dropdown_update = gr.update()
        push_note_update = gr.update()

    user, can_submit, _ = _role_flags_from_request(request)
    actor_user_id = _resolve_request_user_id(user)
    (
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    ) = _refresh_files_and_view(
        actor_user_id,
        current_file_id=normalized_file_id,
        fallback_index=int(current_index or 0),
        can_interact=can_submit,
        show_detail=True,
    )

    return (
        gr.update(value=status_message, visible=True),
        modal_update,
        push_status_update,
        push_dropdown_update,
        push_note_update,
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    )


def _open_unsorted_upload_panel(is_admin: bool):
    if not is_admin:
        return gr.update(visible=False), gr.update(value="Admin credentials are required.", visible=True)
    return gr.update(visible=True), gr.update(value="", visible=False)


def _close_unsorted_upload_panel():
    return (
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(value=None),
        gr.update(value=None),
        gr.update(value=""),
    )


def _store_uploaded_unsorted_entries(
    session,
    *,
    entries: Sequence[Tuple[Path, str]],
    origin_text: str,
    actor_user_id: int,
    uploaded_blob_refs: List[Tuple[str, str]],
    progress: gr.Progress | None = None,
) -> int:
    if not entries:
        raise ValueError("No uploaded files were detected.")

    if not origin_text:
        raise ValueError("Origin/Description is required.")

    rows: List[Dict[str, object]] = []
    total_bytes = 0
    total_entries = len(entries)
    if progress is not None:
        progress(0.0, desc=f"Uploading 0 / {total_entries} files...")

    for entry_index, (path_obj, original_path) in enumerate(entries, start=1):
        raw_name = Path(str(original_path or path_obj.name)).name or path_obj.name
        safe_name = _sanitize_filename(raw_name) or f"file-{uuid4().hex[:8]}"
        stored_name = f"{uuid4().hex[:12]}-{safe_name}"

        prefix = UNSORTED_MEDIA_PREFIX or "unsorted-files"
        day_prefix = datetime.utcnow().strftime("%Y/%m/%d")
        blob_name = f"{prefix}/{day_prefix}/{stored_name}"

        content_type = _resolve_mime_type(None, safe_name, "") or "application/octet-stream"
        blob = get_bucket(DEFAULT_BUCKET).blob(blob_name)
        blob.cache_control = "public, max-age=3600"
        blob.upload_from_filename(str(path_obj), content_type=content_type)
        uploaded_blob_refs.append((DEFAULT_BUCKET, blob_name))

        size_bytes = int(path_obj.stat().st_size)
        total_bytes += size_bytes
        rows.append(
            {
                "bucket": DEFAULT_BUCKET,
                "blob_path": blob_name,
                "file_name": safe_name,
                "original_path": _normalize_original_path(original_path, safe_name),
                "origin_text": origin_text,
                "mime_type": content_type,
                "size_bytes": size_bytes,
                "uploaded_by_user_id": int(actor_user_id),
            }
        )
        if progress is not None:
            progress(
                (entry_index, total_entries),
                desc=f"Uploaded {entry_index} / {total_entries} files...",
            )

    session.execute(
        text(
            """
            INSERT INTO app.unsorted_files (
                bucket,
                blob_path,
                file_name,
                original_path,
                origin_text,
                mime_type,
                size_bytes,
                uploaded_by_user_id
            )
            VALUES (
                :bucket,
                :blob_path,
                :file_name,
                :original_path,
                :origin_text,
                :mime_type,
                :size_bytes,
                :uploaded_by_user_id
            )
            """
        ),
        rows,
    )

    if progress is not None:
        progress(1.0, desc=f"Uploaded {total_entries} / {total_entries} files.")

    return total_bytes


def _upload_unsorted_files(
    upload_files: object,
    upload_folder: object,
    origin_text: str,
    current_file_id: int,
    current_index: int,
    request: gr.Request,
    progress=gr.Progress(track_tqdm=False),
):
    uploaded_blob_refs: List[Tuple[str, str]] = []

    try:
        if progress is not None:
            progress(0.0, desc="Preparing upload...")

        user, _can_submit, is_admin = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to upload unsorted files.")
        if not is_admin:
            raise ValueError("Admin credential is required to upload unsorted files.")

        files_entries = _resolve_upload_entries(upload_files)
        folder_entries = _resolve_upload_entries(upload_folder)
        all_entries = files_entries + folder_entries

        deduped_entries: List[Tuple[Path, str]] = []
        seen_paths: set[str] = set()
        for path_obj, original_label in all_entries:
            key = str(path_obj.resolve())
            if key in seen_paths:
                continue
            seen_paths.add(key)
            deduped_entries.append((path_obj, original_label))

        if not deduped_entries:
            raise ValueError("Upload at least one file or one folder.")

        origin_value = str(origin_text or "").strip()
        if not origin_value:
            raise ValueError("Origin/Description is required.")

        with session_scope() as session:
            _ensure_unsorted_db()
            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

            total_bytes = _store_uploaded_unsorted_entries(
                session,
                entries=deduped_entries,
                origin_text=origin_value,
                actor_user_id=actor_user_id,
                uploaded_blob_refs=uploaded_blob_refs,
                progress=progress,
            )

        status_message = (
            f"✅ Uploaded {len(deduped_entries)} unsorted file(s) "
            f"({_format_bytes(total_bytes)})."
        )
        panel_update = gr.update(visible=False)
        files_input_update = gr.update(value=None)
        folder_input_update = gr.update(value=None)
        origin_update = gr.update(value="")
        file_id_hint = 0
        index_hint = 0
    except Exception as exc:  # noqa: BLE001
        for bucket_name, blob_name in uploaded_blob_refs:
            try:
                get_bucket(bucket_name).blob(blob_name).delete()
            except Exception:
                logger.warning("Could not cleanup unsorted blob %s/%s", bucket_name, blob_name, exc_info=True)

        status_message = f"❌ Could not upload unsorted files: {exc}"
        panel_update = gr.update(visible=True)
        files_input_update = gr.update()
        folder_input_update = gr.update()
        origin_update = gr.update()
        file_id_hint = _coerce_file_id(current_file_id)
        index_hint = int(current_index or 0)

    user, can_submit, _ = _role_flags_from_request(request)
    actor_user_id = _resolve_request_user_id(user)
    (
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
    ) = _refresh_files_and_view(
        actor_user_id,
        current_file_id=file_id_hint,
        fallback_index=index_hint,
        can_interact=can_submit,
        show_detail=_coerce_file_id(file_id_hint) > 0,
    )

    return (
        gr.update(value=status_message, visible=True),
        panel_update,
        files_input_update,
        folder_input_update,
        origin_update,
        files,
        resolved_index,
        resolved_file_id,
        explorer_update,
        detail_shell_update,
        preview_update,
        meta_update,
        counter_update,
        action_summary_update,
        prev_update,
        next_update,
        too_redacted_update,
        push_update,
        useless_update,
        create_source_update,
        gr.update(value="Upload", interactive=True),
        gr.update(interactive=True),
    )
