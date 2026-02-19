from __future__ import annotations

import html
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
from src.employees import ensure_user, lookup_technician_id_by_email
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


def _normalize_tag(value: object) -> str:
    return str(value or "").strip().lower()


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


def _decode_tags_json(raw_value: object) -> List[str]:
    if isinstance(raw_value, list):
        candidates = raw_value
    else:
        text_value = str(raw_value or "").strip()
        if not text_value:
            return []
        try:
            parsed = json.loads(text_value)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            candidates = parsed
        else:
            return _parse_tags_input(text_value)

    tags: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_tag(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        tags.append(normalized)
    return tags


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

    # Some auth payloads include only email; resolve the canonical app.user id.
    email = str(user.get("email") or "").strip().lower()
    if email:
        try:
            with readonly_session_scope() as session:
                resolved = lookup_technician_id_by_email(session, email)
            if resolved:
                return int(resolved)
        except Exception:  # noqa: BLE001
            logger.debug("Could not resolve request user id by email.", exc_info=True)
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
                    CONSTRAINT uq_unsorted_file_actions_file_actor_action_type
                        UNIQUE (unsorted_file_id, actor_user_id, action_type)
                )
                """
            )
        )

        session.execute(
            text(
                """
                DO $$
                DECLARE
                    old_unsorted_file_id_attnum smallint;
                    old_actor_user_id_attnum smallint;
                    action_type_attnum smallint;
                    old_constraint_name text;
                    has_new_constraint boolean;
                BEGIN
                    SELECT attnum INTO old_unsorted_file_id_attnum
                    FROM pg_attribute
                    WHERE attrelid = 'app.unsorted_file_actions'::regclass
                      AND attname = 'unsorted_file_id'
                      AND NOT attisdropped
                    LIMIT 1;

                    SELECT attnum INTO old_actor_user_id_attnum
                    FROM pg_attribute
                    WHERE attrelid = 'app.unsorted_file_actions'::regclass
                      AND attname = 'actor_user_id'
                      AND NOT attisdropped
                    LIMIT 1;

                    SELECT attnum INTO action_type_attnum
                    FROM pg_attribute
                    WHERE attrelid = 'app.unsorted_file_actions'::regclass
                      AND attname = 'action_type'
                      AND NOT attisdropped
                    LIMIT 1;

                    IF old_unsorted_file_id_attnum IS NOT NULL AND old_actor_user_id_attnum IS NOT NULL THEN
                        SELECT c.conname INTO old_constraint_name
                        FROM pg_constraint c
                        WHERE c.conrelid = 'app.unsorted_file_actions'::regclass
                          AND c.contype = 'u'
                          AND c.conkey = ARRAY[old_unsorted_file_id_attnum, old_actor_user_id_attnum]::smallint[]
                        LIMIT 1;

                        IF old_constraint_name IS NOT NULL THEN
                            EXECUTE format(
                                'ALTER TABLE app.unsorted_file_actions DROP CONSTRAINT %I',
                                old_constraint_name
                            );
                        END IF;
                    END IF;

                    IF old_unsorted_file_id_attnum IS NOT NULL
                       AND old_actor_user_id_attnum IS NOT NULL
                       AND action_type_attnum IS NOT NULL THEN
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_constraint c
                            WHERE c.conrelid = 'app.unsorted_file_actions'::regclass
                              AND c.contype = 'u'
                              AND c.conkey = ARRAY[
                                old_unsorted_file_id_attnum,
                                old_actor_user_id_attnum,
                                action_type_attnum
                              ]::smallint[]
                        ) INTO has_new_constraint;

                        IF NOT has_new_constraint THEN
                            ALTER TABLE app.unsorted_file_actions
                            ADD CONSTRAINT uq_unsorted_file_actions_file_actor_action_type
                            UNIQUE (unsorted_file_id, actor_user_id, action_type);
                        END IF;
                    END IF;
                END $$;
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

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.unsorted_file_tag_proposals (
                    id BIGSERIAL PRIMARY KEY,
                    unsorted_file_id BIGINT NOT NULL REFERENCES app.unsorted_files(id) ON DELETE CASCADE,
                    proposer_user_id BIGINT NOT NULL REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE CASCADE,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    reviewed_at TIMESTAMPTZ,
                    reviewer_user_id BIGINT REFERENCES app."user"(id) ON UPDATE CASCADE ON DELETE SET NULL,
                    review_note TEXT,
                    CONSTRAINT chk_unsorted_tag_status CHECK (
                        lower(status) IN ('pending', 'accepted', 'declined')
                    ),
                    UNIQUE (unsorted_file_id, proposer_user_id)
                )
                """
            )
        )

        session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.unsorted_file_tag_proposal_tags (
                    proposal_id BIGINT NOT NULL REFERENCES app.unsorted_file_tag_proposals(id) ON DELETE CASCADE,
                    tag_code TEXT NOT NULL,
                    tag_label TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT pk_unsorted_file_tag_proposal_tags PRIMARY KEY (proposal_id, tag_code),
                    CONSTRAINT chk_unsorted_file_tag_proposal_tag_code CHECK (btrim(tag_code) <> ''),
                    CONSTRAINT chk_unsorted_file_tag_proposal_tag_label CHECK (btrim(tag_label) <> '')
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
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_unsorted_tag_proposals_file_status "
                "ON app.unsorted_file_tag_proposals(unsorted_file_id, status)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_unsorted_tag_proposals_proposer_file "
                "ON app.unsorted_file_tag_proposals(proposer_user_id, unsorted_file_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_unsorted_tag_proposal_tags_proposal "
                "ON app.unsorted_file_tag_proposal_tags(proposal_id)"
            )
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_unsorted_tag_proposal_tags_code "
                "ON app.unsorted_file_tag_proposal_tags(tag_code)"
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


def _fetch_source_tag_catalog() -> List[str]:
    _ensure_unsorted_db()

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
        normalized = _normalize_tag(row)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        catalog.append(normalized)
    return catalog


def _render_unsorted_tags_editor_markup(tag_catalog: Sequence[str]) -> str:
    normalized_catalog: List[str] = []
    seen: set[str] = set()
    for raw_tag in tag_catalog:
        normalized = _normalize_tag(raw_tag)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_catalog.append(normalized)

    tag_catalog_json = html.escape(json.dumps(normalized_catalog, ensure_ascii=True), quote=True)
    return (
        "<section id='unsorted-tags-editor' class='unsorted-tags-editor' "
        f"data-tag-catalog='{tag_catalog_json}'>"
        "<span class='unsorted-tags-editor__label'>Tags</span>"
        "<div class='person-detail-card__tags person-detail-card__tags--editing' aria-live='polite'></div>"
        "</section>"
    )


def _fetch_latest_unsorted_tag_proposal(actor_user_id: int, unsorted_file_id: int) -> Tuple[List[str], str, str]:
    normalized_actor_id = int(max(0, actor_user_id))
    normalized_file_id = _coerce_file_id(unsorted_file_id)
    if normalized_actor_id <= 0 or normalized_file_id <= 0:
        return [], "", ""

    _ensure_unsorted_db()
    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "unsorted_file_tag_proposals"):
            return [], "", ""
        has_tag_rows = _table_exists_in_app_schema(session, "unsorted_file_tag_proposal_tags")
        tags_select = (
            """
                    COALESCE(
                        (
                            SELECT json_agg(utpt.tag_label ORDER BY lower(utpt.tag_label), utpt.tag_label)
                            FROM app.unsorted_file_tag_proposal_tags utpt
                            WHERE utpt.proposal_id = utp.id
                        ),
                        '[]'::json
                    )::text AS tags_json,
            """
            if has_tag_rows
            else "COALESCE(utp.tags_json, '[]') AS tags_json,"
        )
        row = session.execute(
            text(
                """
                SELECT
                    """
                + tags_select
                + """
                    COALESCE(utp.note, '') AS note,
                    COALESCE(utp.status, '') AS status
                FROM app.unsorted_file_tag_proposals utp
                WHERE utp.unsorted_file_id = :unsorted_file_id
                  AND utp.proposer_user_id = :proposer_user_id
                ORDER BY utp.created_at DESC, utp.id DESC
                LIMIT 1
                """
            ),
            {
                "unsorted_file_id": normalized_file_id,
                "proposer_user_id": normalized_actor_id,
            },
        ).mappings().one_or_none()

    if row is None:
        return [], "", ""
    return (
        _decode_tags_json(row.get("tags_json")),
        str(row.get("note") or "").strip(),
        str(row.get("status") or "").strip().lower(),
    )


def _fetch_unsorted_files(actor_user_id: int) -> List[Dict[str, object]]:
    _ensure_unsorted_db()

    with readonly_session_scope() as session:
        if not _table_exists_in_app_schema(session, "unsorted_files"):
            return []

        has_tag_proposals = _table_exists_in_app_schema(session, "unsorted_file_tag_proposals")
        has_tag_proposal_tags = _table_exists_in_app_schema(session, "unsorted_file_tag_proposal_tags")
        has_push_proposals = _table_exists_in_app_schema(session, "unsorted_file_push_proposals")
        tag_json_select = (
            """
                    COALESCE(
                        (
                            SELECT json_agg(utpt.tag_label ORDER BY lower(utpt.tag_label), utpt.tag_label)
                            FROM app.unsorted_file_tag_proposal_tags utpt
                            WHERE utpt.proposal_id = utp.id
                        ),
                        '[]'::json
                    )::text AS tags_json,
            """
            if has_tag_proposal_tags
            else "COALESCE(utp.tags_json, '[]') AS tags_json,"
        )
        user_tag_proposal_cte = (
            """
            user_tag_proposal AS (
                SELECT DISTINCT ON (utp.unsorted_file_id)
                    utp.unsorted_file_id,
                    """
            + tag_json_select
            + """
                    COALESCE(utp.status, '') AS status
                FROM app.unsorted_file_tag_proposals utp
                WHERE utp.proposer_user_id = :actor_user_id
                ORDER BY utp.unsorted_file_id, utp.created_at DESC, utp.id DESC
            )
            """
            if has_tag_proposals
            else """
            user_tag_proposal AS (
                SELECT
                    NULL::bigint AS unsorted_file_id,
                    '[]'::text AS tags_json,
                    ''::text AS status
                WHERE FALSE
            )
            """
        )
        user_push_proposal_cte = (
            """
            user_push_proposal AS (
                SELECT DISTINCT ON (upp.unsorted_file_id)
                    upp.unsorted_file_id,
                    upp.id AS proposal_id,
                    COALESCE(upp.source_slug, '') AS source_slug,
                    COALESCE(upp.status, '') AS status
                FROM app.unsorted_file_push_proposals upp
                WHERE upp.proposer_user_id = :actor_user_id
                ORDER BY upp.unsorted_file_id, upp.created_at DESC, upp.id DESC
            )
            """
            if has_push_proposals
            else """
            user_push_proposal AS (
                SELECT
                    NULL::bigint AS unsorted_file_id,
                    0::bigint AS proposal_id,
                    ''::text AS source_slug,
                    ''::text AS status
                WHERE FALSE
            )
            """
        )

        rows = session.execute(
            text(
                f"""
                WITH
                action_counts AS (
                    SELECT
                        ufa.unsorted_file_id,
                        COUNT(*) FILTER (WHERE lower(ufa.action_type) = 'useless')::bigint AS useless_count,
                        COUNT(*) FILTER (WHERE lower(ufa.action_type) = 'too_redacted')::bigint AS too_redacted_count
                    FROM app.unsorted_file_actions ufa
                    GROUP BY ufa.unsorted_file_id
                ),
                user_action_flags AS (
                    SELECT
                        ufa.unsorted_file_id,
                        BOOL_OR(lower(ufa.action_type) = 'too_redacted') AS user_marked_too_redacted,
                        BOOL_OR(lower(ufa.action_type) = 'useless') AS user_marked_useless
                    FROM app.unsorted_file_actions ufa
                    WHERE ufa.actor_user_id = :actor_user_id
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
                ),
                source_usage AS (
                    SELECT
                        ufa.unsorted_file_id,
                        COUNT(*)::bigint AS used_in_source_count
                    FROM app.unsorted_file_actions ufa
                    WHERE lower(ufa.action_type) = 'create_new_source'
                    GROUP BY ufa.unsorted_file_id
                ),
                source_usage_latest AS (
                    SELECT DISTINCT ON (ufa.unsorted_file_id)
                        ufa.unsorted_file_id,
                        COALESCE(ufa.source_slug, '') AS source_slug
                    FROM app.unsorted_file_actions ufa
                    WHERE lower(ufa.action_type) = 'create_new_source'
                    ORDER BY ufa.unsorted_file_id, ufa.updated_at DESC, ufa.id DESC
                ),
                {user_tag_proposal_cte},
                {user_push_proposal_cte}
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
                    COALESCE(ac.useless_count, 0)::bigint AS useless_count,
                    COALESCE(ac.too_redacted_count, 0)::bigint AS too_redacted_count,
                    COALESCE(uaf.user_marked_too_redacted, FALSE) AS user_marked_too_redacted,
                    COALESCE(uaf.user_marked_useless, FALSE) AS user_marked_useless,
                    COALESCE(ua.action_type, '') AS user_action,
                    COALESCE(ua.source_slug, '') AS user_source_slug,
                    COALESCE(su.used_in_source_count, 0)::bigint AS used_in_source_count,
                    COALESCE(sul.source_slug, '') AS used_in_source_slug,
                    COALESCE(utp.tags_json, '[]') AS user_tag_proposal_tags_json,
                    COALESCE(utp.status, '') AS user_tag_proposal_status,
                    COALESCE(upp.proposal_id, 0)::bigint AS user_push_proposal_id,
                    COALESCE(upp.source_slug, '') AS user_push_proposal_source_slug,
                    COALESCE(upp.status, '') AS user_push_proposal_status
                FROM app.unsorted_files uf
                LEFT JOIN action_counts ac
                    ON ac.unsorted_file_id = uf.id
                LEFT JOIN user_action_flags uaf
                    ON uaf.unsorted_file_id = uf.id
                LEFT JOIN user_action ua
                    ON ua.unsorted_file_id = uf.id
                LEFT JOIN source_usage su
                    ON su.unsorted_file_id = uf.id
                LEFT JOIN source_usage_latest sul
                    ON sul.unsorted_file_id = uf.id
                LEFT JOIN user_tag_proposal utp
                    ON utp.unsorted_file_id = uf.id
                LEFT JOIN user_push_proposal upp
                    ON upp.unsorted_file_id = uf.id
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
                "too_redacted_count": int(row.get("too_redacted_count") or 0),
                "user_marked_too_redacted": _is_truthy(row.get("user_marked_too_redacted")),
                "user_marked_useless": _is_truthy(row.get("user_marked_useless")),
                "user_action": _normalize_action(row.get("user_action")),
                "user_source_slug": str(row.get("user_source_slug") or "").strip().lower(),
                "used_in_source_count": int(row.get("used_in_source_count") or 0),
                "used_in_source_slug": str(row.get("used_in_source_slug") or "").strip().lower(),
                "user_tag_proposal_tags": _decode_tags_json(row.get("user_tag_proposal_tags_json")),
                "user_tag_proposal_status": str(row.get("user_tag_proposal_status") or "").strip().lower(),
                "user_push_proposal_id": int(row.get("user_push_proposal_id") or 0),
                "user_push_proposal_source_slug": str(row.get("user_push_proposal_source_slug") or "").strip().lower(),
                "user_push_proposal_status": str(row.get("user_push_proposal_status") or "").strip().lower(),
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

    def _safe_count(raw_value: object) -> int:
        try:
            parsed = int(raw_value or 0)
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)

    list_rows: List[str] = []
    grid_cards: List[str] = []
    marked_used_in_source_files = 0
    marked_too_redacted_files = 0
    marked_useless_files = 0
    for row in rows:
        file_id = int(row.get("id") or 0)
        file_name = str(row.get("file_name") or "file").strip() or "file"
        mime_type = str(row.get("mime_type") or "").strip()
        created_label = _unsorted_uploaded_label(row.get("created_at"))
        size_label = _format_bytes(int(row.get("size_bytes") or 0))
        too_redacted_count = _safe_count(row.get("too_redacted_count"))
        useless_count = _safe_count(row.get("useless_count"))
        used_in_source_count = _safe_count(row.get("used_in_source_count"))
        used_in_source_slug = str(row.get("used_in_source_slug") or "").strip().lower()
        is_used_in_source = used_in_source_count > 0
        type_label = _unsorted_type_label(mime_type, file_name)
        badge_label = _unsorted_type_badge(mime_type, file_name)
        href = _unsorted_file_href(file_id)

        safe_href = html.escape(href, quote=True)
        safe_name = html.escape(file_name)
        safe_type = html.escape(type_label)
        safe_badge = html.escape(badge_label)
        safe_size = html.escape(size_label)
        safe_created = html.escape(created_label)
        row_flags: List[str] = []
        if is_used_in_source:
            marked_used_in_source_files += 1
            used_label = "Used in source"
            if used_in_source_slug:
                used_label = f"Used in source ({used_in_source_slug})"
            elif used_in_source_count > 1:
                used_label = f"Used in source ({used_in_source_count})"
            row_flags.append(
                "<span class='unsorted-browser__flag unsorted-browser__flag--source'>"
                f"{html.escape(used_label)}"
                "</span>"
            )
        if too_redacted_count > 0:
            marked_too_redacted_files += 1
            row_flags.append(
                "<span class='unsorted-browser__flag unsorted-browser__flag--redacted'>"
                f"Too redacted ({too_redacted_count})"
                "</span>"
            )
        if useless_count > 0:
            marked_useless_files += 1
            row_flags.append(
                "<span class='unsorted-browser__flag unsorted-browser__flag--useless'>"
                f"Useless ({useless_count})"
                "</span>"
            )
        row_flags_markup = f"<span class='unsorted-browser__flags'>{''.join(row_flags)}</span>" if row_flags else ""
        row_class = "unsorted-browser__row unsorted-browser__row--used" if is_used_in_source else "unsorted-browser__row"
        tile_class = "unsorted-browser__tile unsorted-browser__tile--used" if is_used_in_source else "unsorted-browser__tile"

        list_rows.append(
            f"<a class='{row_class}' "
            f"href='{safe_href}' title='Open {safe_name}'>"
            "<span class='unsorted-browser__name'>"
            f"<span class='unsorted-browser__badge'>{safe_badge}</span>"
            "<span class='unsorted-browser__name-body'>"
            f"<span class='unsorted-browser__name-text'>{safe_name}</span>"
            f"{row_flags_markup}"
            "</span>"
            "</span>"
            f"<span class='unsorted-browser__type'>{safe_type}</span>"
            f"<span class='unsorted-browser__size'>{safe_size}</span>"
            f"<span class='unsorted-browser__date'>{safe_created}</span>"
            "</a>"
        )

        grid_cards.append(
            f"<a class='{tile_class}' "
            f"href='{safe_href}' title='Open {safe_name}'>"
            f"<span class='unsorted-browser__tile-badge'>{safe_badge}</span>"
            f"<span class='unsorted-browser__tile-name'>{safe_name}</span>"
            f"<span class='unsorted-browser__tile-meta'>{safe_type} • {safe_size}</span>"
            f"{row_flags_markup}"
            "</a>"
        )

    marked_summary_parts: List[str] = []
    if marked_used_in_source_files > 0:
        marked_summary_parts.append(f"Used in source: {marked_used_in_source_files}")
    if marked_too_redacted_files > 0:
        marked_summary_parts.append(f"Too redacted: {marked_too_redacted_files}")
    if marked_useless_files > 0:
        marked_summary_parts.append(f"Useless: {marked_useless_files}")
    if marked_summary_parts:
        marked_summary = "Marked files - " + ", ".join(marked_summary_parts)
    else:
        marked_summary = "Marked files - none yet"

    return (
        "<section class='unsorted-browser'>"
        "<div class='unsorted-browser__toolbar'>"
        "<div class='unsorted-browser__title'>"
        f"<strong>{len(rows)} file(s)</strong>"
        "<span>Choose a file to open the review workspace.</span>"
        f"<span class='unsorted-browser__summary'>{html.escape(marked_summary)}</span>"
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
    safe_media_url = html.escape(media_url, quote=True)
    preview_markup = _render_media_preview(media_url, mime_type, file_name)
    preview_class = "unsorted-preview-card"
    if _is_pdf_mime(_resolve_mime_type(mime_type, file_name, media_url)):
        preview_class += " unsorted-preview-card--pdf"
    return (
        f"<section class='unsorted-preview-wrap' data-unsorted-media-url='{safe_media_url}'>"
        "<a href='#' class='unsorted-preview-fullscreen' role='button' "
        "title='Toggle full screen preview' aria-label='Toggle full screen preview' aria-pressed='false'>"
        "Full screen"
        "</a>"
        f"<section class='{preview_class}'>{preview_markup}</section>"
        "</section>"
    )


def _render_unsorted_file_meta(file_row: Dict[str, object] | None, *, can_edit_tags: bool = False) -> str:
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
    used_in_source_count = max(0, int(file_row.get("used_in_source_count") or 0))
    used_in_source_slug = str(file_row.get("used_in_source_slug") or "").strip().lower()
    used_in_source_markup = ""
    if used_in_source_count > 0:
        if used_in_source_slug:
            used_in_source_text = used_in_source_slug
        elif used_in_source_count > 1:
            used_in_source_text = f"{used_in_source_count} source(s)"
        else:
            used_in_source_text = "yes"
        used_in_source_markup = (
            "<p><strong>Used in source:</strong> "
            f"{html.escape(used_in_source_text)}"
            "</p>"
        )
    push_proposal_id = max(0, int(file_row.get("user_push_proposal_id") or 0))
    push_proposal_source_slug = str(file_row.get("user_push_proposal_source_slug") or "").strip().lower()
    push_proposal_status = str(file_row.get("user_push_proposal_status") or "").strip().lower()
    push_proposal_status_class = (
        push_proposal_status
        if push_proposal_status in {"pending", "accepted", "declined"}
        else "unknown"
    )
    push_proposal_markup = ""
    if push_proposal_id > 0:
        source_label = push_proposal_source_slug or "source"
        push_status_markup = ""
        if push_proposal_status:
            push_status_markup = (
                " "
                f"<span class='unsorted-tag-status unsorted-tag-status--{push_proposal_status_class}'>"
                f"{html.escape(push_proposal_status)}"
                "</span>"
            )
        push_proposal_markup = (
            "<p class='unsorted-file-meta__push-proposal'><strong>Your push proposal:</strong> "
            f"#{push_proposal_id} to <code>{html.escape(source_label)}</code>"
            f"{push_status_markup}</p>"
        )
    proposal_tags = [
        _normalize_tag(tag)
        for tag in (file_row.get("user_tag_proposal_tags") or [])
        if _normalize_tag(tag)
    ]
    proposal_status = str(file_row.get("user_tag_proposal_status") or "").strip().lower()
    if proposal_tags:
        proposal_tags_markup = "".join(
            f"<span class='person-tag'>{html.escape(tag)}</span>"
            for tag in proposal_tags
        )
    else:
        proposal_tags_markup = "<span class='person-tag person-tag--muted'>no-tags</span>"
    proposal_tags_json = html.escape(json.dumps(proposal_tags, ensure_ascii=True), quote=True)

    if can_edit_tags:
        proposal_tags_editor_markup = (
            "<div class='unsorted-file-meta__tags-editor' "
            f"data-unsorted-meta-tags='{proposal_tags_json}'>"
            "<div class='person-detail-card__tags person-detail-card__tags--editing unsorted-file-meta__tags-host' aria-live='polite'>"
            f"{proposal_tags_markup}"
            "</div>"
            "<div class='unsorted-file-meta__tags-actions' hidden>"
            "<button type='button' class='unsorted-file-meta__tags-save-btn'>Save</button>"
            "<button type='button' class='unsorted-file-meta__tags-cancel-btn'>Cancel</button>"
            "</div>"
            "</div>"
        )
    else:
        proposal_tags_editor_markup = (
            f"<div class='person-detail-card__tags person-detail-card__tags--editing'>{proposal_tags_markup}</div>"
        )

    status_class = proposal_status if proposal_status in {"pending", "accepted", "declined"} else "unknown"
    proposal_status_markup = ""
    if proposal_status:
        proposal_status_markup = (
            "<p class='unsorted-file-meta__tags-status'>"
            "<strong>Status:</strong> "
            f"<span class='unsorted-tag-status unsorted-tag-status--{status_class}'>{html.escape(proposal_status)}</span>"
            "</p>"
        )
    meta_class = "unsorted-file-meta unsorted-file-meta--used" if used_in_source_count > 0 else "unsorted-file-meta"

    return (
        f"<section class='{meta_class}'>"
        f"<h3>{html.escape(file_name)}</h3>"
        f"<p><strong>Origin/Description:</strong> {_render_origin_value(origin_text)}</p>"
        f"<p><strong>Type:</strong> {html.escape(type_label)} | <strong>Size:</strong> {html.escape(size_label)}</p>"
        f"{used_in_source_markup}"
        f"{push_proposal_markup}"
        "<div class='unsorted-file-meta__tags-block'>"
        "<p class='unsorted-file-meta__tags-label'><strong>Your tag proposal:</strong></p>"
        f"{proposal_tags_editor_markup}"
        f"{proposal_status_markup}"
        "</div>"
        f"<p><strong>Uploaded:</strong> {html.escape(created_label or '-')}</p>"
        "<p><a class='source-table__link' href='/unsorted-files/'>Back to files</a></p>"
        f"<p><a class='source-table__link' href='{html.escape(media_url, quote=True)}' target='_blank' rel='noopener'>Open file in new tab</a></p>"
        "</section>"
    )


def _action_summary_markup(file_row: Dict[str, object] | None) -> str:
    if not isinstance(file_row, dict):
        return ""

    user_action = _normalize_action(file_row.get("user_action"))
    lines: List[str] = []
    if user_action:
        label = _ACTION_LABELS.get(user_action, user_action.replace("_", " ").title())
        if user_action == ACTION_PUSH_TO_SOURCE:
            source_slug = str(file_row.get("user_source_slug") or "").strip()
            if source_slug:
                label = f"{label} (`{source_slug}`)"
        lines.append(f"Your current choice: **{label}**")

    used_in_source_count = max(0, int(file_row.get("used_in_source_count") or 0))
    used_in_source_slug = str(file_row.get("used_in_source_slug") or "").strip().lower()
    if used_in_source_count > 0:
        if used_in_source_slug:
            lines.append(f"Used in source: `{used_in_source_slug}`")
        elif used_in_source_count > 1:
            lines.append(f"Used in source: {used_in_source_count} source(s)")
        else:
            lines.append("Used in source: yes")

    proposed_tags = [
        _normalize_tag(tag)
        for tag in (file_row.get("user_tag_proposal_tags") or [])
        if _normalize_tag(tag)
    ]
    proposal_status = str(file_row.get("user_tag_proposal_status") or "").strip().lower()
    if proposed_tags:
        tags_text = ", ".join(f"`{tag}`" for tag in proposed_tags)
        status_suffix = f" ({proposal_status})" if proposal_status else ""
        lines.append(f"Your tag proposal{status_suffix}: {tags_text}")

    push_proposal_id = max(0, int(file_row.get("user_push_proposal_id") or 0))
    if push_proposal_id > 0:
        push_target = str(file_row.get("user_push_proposal_source_slug") or "").strip().lower()
        push_status = str(file_row.get("user_push_proposal_status") or "").strip().lower()
        target_text = f" to `{push_target}`" if push_target else ""
        status_text = f" ({push_status})" if push_status else ""
        lines.append(f"Your push proposal #{push_proposal_id}{status_text}{target_text}")

    return "  \n".join(lines)


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
            gr.update(value="Too redacted (0)", interactive=False, variant="secondary"),
            gr.update(interactive=False),
            gr.update(value="Useless (0)", interactive=False, variant="secondary"),
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
            gr.update(value="Too redacted (0)", interactive=False, variant="secondary"),
            gr.update(interactive=False),
            gr.update(value="Useless (0)", interactive=False, variant="secondary"),
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
    too_redacted_count = max(0, int(selected.get("too_redacted_count") or 0))
    too_redacted_active = _is_truthy(selected.get("user_marked_too_redacted"))
    useless_active = _is_truthy(selected.get("user_marked_useless"))
    action_summary = _action_summary_markup(selected)
    action_enabled = bool(can_interact)

    return (
        resolved_index,
        selected_id,
        gr.update(value=_render_unsorted_explorer(rows), visible=False),
        gr.update(visible=True),
        gr.update(value=_render_unsorted_file_preview(selected), visible=True),
        gr.update(value=_render_unsorted_file_meta(selected, can_edit_tags=can_interact), visible=True),
        gr.update(value=f"{resolved_index + 1} / {total}", visible=True),
        gr.update(value=action_summary, visible=bool(action_summary)),
        gr.update(interactive=resolved_index > 0),
        gr.update(interactive=resolved_index < (total - 1)),
        gr.update(
            value=f"Too redacted ({too_redacted_count})",
            interactive=action_enabled,
            variant="primary" if too_redacted_active else "secondary",
        ),
        gr.update(interactive=action_enabled),
        gr.update(
            value=f"Useless ({useless_count})",
            interactive=action_enabled,
            variant="primary" if useless_active else "secondary",
        ),
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
        gr.update(visible=False),
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
        gr.update(interactive=bool(can_submit and resolved_file_id > 0)),
        useless_update,
        create_source_update,
        gr.update(value=status_message, visible=bool(status_message)),
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(choices=[], value=None, interactive=False),
        gr.update(value=""),
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(value=""),
        gr.update(value=_render_unsorted_tags_editor_markup(_fetch_source_tag_catalog())),
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
            ON CONFLICT (unsorted_file_id, actor_user_id, action_type) DO UPDATE
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


def _delete_user_action(
    session,
    *,
    unsorted_file_id: int,
    actor_user_id: int,
    action_type: str,
) -> None:
    normalized_action = _normalize_action(action_type)
    if not normalized_action:
        raise ValueError("Invalid action type.")

    session.execute(
        text(
            """
            DELETE FROM app.unsorted_file_actions
            WHERE unsorted_file_id = :unsorted_file_id
              AND actor_user_id = :actor_user_id
              AND lower(action_type) = :action_type
            """
        ),
        {
            "unsorted_file_id": int(unsorted_file_id),
            "actor_user_id": int(actor_user_id),
            "action_type": normalized_action,
        },
    )


def _has_user_action(
    session,
    *,
    unsorted_file_id: int,
    actor_user_id: int,
    action_type: str,
) -> bool:
    normalized_action = _normalize_action(action_type)
    if not normalized_action:
        return False

    return bool(
        session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM app.unsorted_file_actions ufa
                    WHERE ufa.unsorted_file_id = :unsorted_file_id
                      AND ufa.actor_user_id = :actor_user_id
                      AND lower(ufa.action_type) = :action_type
                )
                """
            ),
            {
                "unsorted_file_id": int(unsorted_file_id),
                "actor_user_id": int(actor_user_id),
                "action_type": normalized_action,
            },
        ).scalar_one()
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

            already_marked = _has_user_action(
                session,
                unsorted_file_id=normalized_file_id,
                actor_user_id=actor_user_id,
                action_type=normalized_action,
            )

            if already_marked:
                _delete_user_action(
                    session,
                    unsorted_file_id=normalized_file_id,
                    actor_user_id=actor_user_id,
                    action_type=normalized_action,
                )
                status_message = (
                    f"✅ Removed **{_ACTION_LABELS.get(normalized_action, normalized_action)}** from this file."
                )
            else:
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


def _open_unsorted_tags_modal(current_file_id: int, request: gr.Request):
    normalized_file_id = _coerce_file_id(current_file_id)
    tag_catalog = _fetch_source_tag_catalog()
    editor_markup = _render_unsorted_tags_editor_markup(tag_catalog)

    user, can_submit, _is_admin = _role_flags_from_request(request)
    if not user:
        return (
            gr.update(visible=False),
            gr.update(value="You must sign in to submit a tag proposal.", visible=True),
            gr.update(value=""),
            gr.update(value=editor_markup),
            gr.update(value=""),
        )
    if not can_submit:
        return (
            gr.update(visible=False),
            gr.update(value="Your `base_user` privilege is disabled.", visible=True),
            gr.update(value=""),
            gr.update(value=editor_markup),
            gr.update(value=""),
        )
    if normalized_file_id <= 0:
        return (
            gr.update(visible=False),
            gr.update(value="Select a file first.", visible=True),
            gr.update(value=""),
            gr.update(value=editor_markup),
            gr.update(value=""),
        )

    actor_user_id = _resolve_request_user_id(user)
    proposed_tags: List[str] = []
    proposal_note = ""
    proposal_status = ""
    if actor_user_id > 0:
        proposed_tags, proposal_note, proposal_status = _fetch_latest_unsorted_tag_proposal(actor_user_id, normalized_file_id)

    status_message = ""
    if proposal_status == "pending":
        status_message = "Latest tag proposal is pending review."
    elif proposal_status == "accepted":
        status_message = "Latest tag proposal was accepted."
    elif proposal_status == "declined":
        status_message = "Latest tag proposal was declined."

    return (
        gr.update(visible=True),
        gr.update(value=status_message, visible=bool(status_message)),
        gr.update(value=", ".join(proposed_tags)),
        gr.update(value=editor_markup),
        gr.update(value=proposal_note),
    )


def _cancel_unsorted_tags_modal():
    return (
        gr.update(visible=False),
        gr.update(value="", visible=False),
        gr.update(value=""),
        gr.update(value=_render_unsorted_tags_editor_markup(_fetch_source_tag_catalog())),
        gr.update(value=""),
    )


def _submit_unsorted_tags_proposal(
    current_file_id: int,
    proposed_tags: str,
    proposal_note: str,
    current_index: int,
    request: gr.Request,
):
    normalized_file_id = _coerce_file_id(current_file_id)
    parsed_tags = _parse_tags_input(proposed_tags)
    proposal_id = 0

    try:
        user, can_submit, _is_admin = _role_flags_from_request(request)
        if not user:
            raise ValueError("You must be logged in to submit tag proposals.")
        if not can_submit:
            raise ValueError("Your `base_user` privilege is disabled. Ask a creator to restore access.")
        if normalized_file_id <= 0:
            raise ValueError("Select a file first.")
        if not parsed_tags:
            raise ValueError("Add at least one tag before submitting.")

        with session_scope() as session:
            _ensure_unsorted_db()
            actor_user_id = _resolve_or_create_actor_user_id(session, user)
            if actor_user_id <= 0:
                raise ValueError("Could not resolve your user id.")

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

            proposal_id = int(
                session.execute(
                    text(
                        """
                        INSERT INTO app.unsorted_file_tag_proposals (
                            unsorted_file_id,
                            proposer_user_id,
                            tags_json,
                            note,
                            status
                        )
                        VALUES (
                            :unsorted_file_id,
                            :proposer_user_id,
                            :tags_json,
                            :note,
                            'pending'
                        )
                        ON CONFLICT (unsorted_file_id, proposer_user_id) DO UPDATE
                        SET tags_json = EXCLUDED.tags_json,
                            note = EXCLUDED.note,
                            status = 'pending',
                            created_at = now(),
                            reviewed_at = NULL,
                            reviewer_user_id = NULL,
                            review_note = NULL
                        RETURNING id
                        """
                    ),
                    {
                        "unsorted_file_id": normalized_file_id,
                        "proposer_user_id": actor_user_id,
                        "tags_json": json.dumps(parsed_tags, ensure_ascii=True),
                        "note": str(proposal_note or "").strip(),
                    },
                ).scalar_one()
            )

            if _table_exists_in_app_schema(session, "unsorted_file_tag_proposal_tags"):
                session.execute(
                    text(
                        """
                        DELETE FROM app.unsorted_file_tag_proposal_tags
                        WHERE proposal_id = :proposal_id
                        """
                    ),
                    {"proposal_id": proposal_id},
                )
                if parsed_tags:
                    session.execute(
                        text(
                            """
                            INSERT INTO app.unsorted_file_tag_proposal_tags (
                                proposal_id,
                                tag_code,
                                tag_label
                            )
                            VALUES (
                                :proposal_id,
                                :tag_code,
                                :tag_label
                            )
                            ON CONFLICT (proposal_id, tag_code) DO UPDATE
                            SET tag_label = EXCLUDED.tag_label
                            """
                        ),
                        [
                            {
                                "proposal_id": proposal_id,
                                "tag_code": tag_value,
                                "tag_label": tag_value,
                            }
                            for tag_value in parsed_tags
                        ],
                    )

        status_message = f"✅ Tag proposal #{proposal_id} submitted with {len(parsed_tags)} tag(s)."
        modal_update = gr.update(visible=False)
        tags_status_update = gr.update(value="", visible=False)
        tags_input_update = gr.update(value="")
        tags_note_update = gr.update(value="")
    except Exception as exc:  # noqa: BLE001
        status_message = f"❌ Could not submit tag proposal: {exc}"
        modal_update = gr.update(visible=True)
        tags_status_update = gr.update(value=str(exc), visible=True)
        tags_input_update = gr.update()
        tags_note_update = gr.update()

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
        tags_status_update,
        tags_input_update,
        gr.update(value=_render_unsorted_tags_editor_markup(_fetch_source_tag_catalog())),
        tags_note_update,
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

        status_message = (
            f"✅ Push proposal #{proposal_id} submitted for source `{source_name}`. "
            "Track it on this file in Unsorted (it is not listed on The List Review page)."
        )
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
