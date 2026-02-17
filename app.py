# ---- Resolve & inject ALL secrets BEFORE importing modules that read env ----
from src.secrets import get_secret

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse, Response
from starlette.staticfiles import StaticFiles
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote


def _install_proxy_headers(app: FastAPI) -> None:
    """Attach a proxy-aware middleware even on stripped Starlette builds."""

    try:
        from starlette.middleware.proxy_headers import ProxyHeadersMiddleware as _Proxy

        app.add_middleware(_Proxy)
        return
    except ImportError:
        pass

    try:
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware as _Proxy  # type: ignore

        app.add_middleware(_Proxy, trusted_hosts="*")
        return
    except ImportError:
        pass

    from starlette.middleware.base import BaseHTTPMiddleware

    class _Proxy(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            forwarded_proto = request.headers.get("x-forwarded-proto")
            if forwarded_proto:
                request.scope["scheme"] = forwarded_proto.split(",")[0].strip()

            forwarded_host = request.headers.get("x-forwarded-host")
            forwarded_port = request.headers.get("x-forwarded-port")
            server = request.scope.get("server", (None, None))

            host = forwarded_host.split(",")[0].strip() if forwarded_host else server[0]
            port = (
                int(forwarded_port.split(",")[0])
                if forwarded_port and forwarded_port.split(",")[0].isdigit()
                else server[1]
            )
            if host or port:
                request.scope["server"] = (host, port)

            return await call_next(request)

    app.add_middleware(_Proxy)

import os
import mimetypes
import gradio as gr

from src.login_logic import register_oauth_provider, add_login_snippet_route
from src.pages.ui_login import make_login_page
from src.pages.ui_profile import make_profile_app
from src.mount_gradio_app import mount_gradio_app
from src.pages.ui_home import make_home_app
from src.pages.admin.app_admin import make_admin_app
from src.pages.the_list.app_the_list import make_the_list_app
from src.pages.theories.app_theories import make_theories_app
from src.pages.sources_list.app_sources import make_sources_app
from src.pages.sources_individual.app_sources_individual import make_sources_individual_app
from src.pages.people_display.app_people_display import make_people_display_app
from src.pages.theory_display.app_theory_display import make_theory_display_app
from src.pages.people_display.app_people_create import make_people_create_app
from src.pages.theory_display.app_theory_create import make_theory_create_app
from src.pages.review_display.app_review_display import make_review_display_app
from src.pages.privileges.app_privileges import make_privileges_app
from src.gcs_storage import blob_http_metadata, download_bytes

app = FastAPI()
_install_proxy_headers(app)

MEDIA_CACHE_CONTROL_REVALIDATE = "public, max-age=0, must-revalidate"
MEDIA_CACHE_CONTROL_VERSIONED = "public, max-age=31536000, immutable"


def _quote_etag(raw_etag: str | None) -> str:
    value = str(raw_etag or "").strip()
    if not value:
        return ""
    if value.startswith("W/"):
        value = value[2:].strip()
    value = value.strip('"')
    if not value:
        return ""
    return f'"{value}"'


def _etag_matches(header_value: str | None, current_etag: str) -> bool:
    if not header_value or not current_etag:
        return False
    current = current_etag.removeprefix("W/").strip().strip('"')
    if not current:
        return False
    for token in str(header_value).split(","):
        candidate = token.strip()
        if not candidate:
            continue
        if candidate == "*":
            return True
        if candidate.removeprefix("W/").strip().strip('"') == current:
            return True
    return False


def _parse_http_date(header_value: str | None) -> datetime | None:
    if not header_value:
        return None
    try:
        parsed = parsedate_to_datetime(header_value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_http_date(value: datetime | None) -> str:
    if value is None:
        return ""
    return format_datetime(value.astimezone(timezone.utc).replace(microsecond=0), usegmt=True)


def _is_not_modified(
    request: Request,
    *,
    etag: str,
    updated_at: datetime | None,
) -> bool:
    if_none_match = request.headers.get("if-none-match")
    if _etag_matches(if_none_match, etag):
        return True

    if if_none_match:
        return False

    if_modified_since = _parse_http_date(request.headers.get("if-modified-since"))
    if if_modified_since is None or updated_at is None:
        return False
    return updated_at.astimezone(timezone.utc).replace(microsecond=0) <= if_modified_since

@app.get("/_routes")
def _routes():
    return [getattr(r, "path", str(r)) for r in app.router.routes]


@app.middleware("http")
async def redirect_the_list_slug_to_people_display(request: Request, call_next):
    normalized_path = (request.url.path or "/").rstrip("/") or "/"
    if normalized_path == "/the-list":
        slug = str(request.query_params.get("slug", "")).strip().lower()
        if slug:
            target = f"/people-display/?slug={quote(slug, safe='-')}"
            return RedirectResponse(url=target, status_code=307)
    if normalized_path == "/theories":
        slug = str(request.query_params.get("slug", "")).strip().lower()
        if slug:
            target = f"/theory-display/?slug={quote(slug, safe='-')}"
            return RedirectResponse(url=target, status_code=307)
    if normalized_path == "/sources":
        slug = str(request.query_params.get("source", "")).strip().lower()
        if slug:
            target = f"/sources-individual/?slug={quote(slug, safe='-')}"
            return RedirectResponse(url=target, status_code=307)
    return await call_next(request)

# OAuth client config (now guaranteed in env; also available via get_secret)
GOOGLE_CLIENT_ID     = get_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = get_secret("GOOGLE_CLIENT_SECRET")

register_oauth_provider(
    name="google",
    icon="google",
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    client_kwargs={
        "scope": "openid email profile",
        "timeout": 30,
    },
)
add_login_snippet_route(app, provider_name="google")

# --- Static assets
os.makedirs("images", exist_ok=True)
FAVICON_FILE = Path("images") / "The-list-logo2.png"
app.mount(
    "/images",
    StaticFiles(directory="images", check_dir=False),
    name="images",
)


@app.get("/favicon.ico")
async def favicon() -> FileResponse:
    if FAVICON_FILE.exists():
        return FileResponse(FAVICON_FILE)
    raise HTTPException(status_code=404)


@app.get("/media/{blob_path:path}")
async def media_blob(blob_path: str, request: Request) -> Response:
    normalized = (blob_path or "").strip().lstrip("/")
    if not normalized:
        raise HTTPException(status_code=404)

    # Versioned URLs (`?v=...`) are content-addressed from app data, so we can cache
    # aggressively and skip metadata round-trips.
    version_token = str(request.query_params.get("v", "")).strip()
    if version_token:
        guessed_type = mimetypes.guess_type(normalized)[0]
        try:
            payload = download_bytes(normalized)
        except FileNotFoundError:
            raise HTTPException(status_code=404)
        except Exception:
            raise HTTPException(status_code=500, detail="Media fetch failed")
        return Response(
            content=payload,
            media_type=guessed_type or "application/octet-stream",
            headers={"Cache-Control": MEDIA_CACHE_CONTROL_VERSIONED},
        )

    try:
        content_type, blob_etag, blob_updated_at = blob_http_metadata(normalized)
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    except Exception:
        raise HTTPException(status_code=500, detail="Media fetch failed")

    etag = _quote_etag(blob_etag)
    last_modified = _format_http_date(blob_updated_at)
    headers = {
        "Cache-Control": MEDIA_CACHE_CONTROL_REVALIDATE,
    }
    if etag:
        headers["ETag"] = etag
    if last_modified:
        headers["Last-Modified"] = last_modified

    if _is_not_modified(request, etag=etag, updated_at=blob_updated_at):
        return Response(status_code=304, headers=headers)

    try:
        payload = download_bytes(normalized)
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    except Exception:
        raise HTTPException(status_code=500, detail="Media fetch failed")

    return Response(content=payload, media_type=content_type or "application/octet-stream", headers=headers)

# --- Simple pages
home_app       = make_home_app()
profile_app    = make_profile_app()
the_list_app   = make_the_list_app()
theories_app   = make_theories_app()
sources_app    = make_sources_app()
sources_individual_app = make_sources_individual_app()
people_display_app = make_people_display_app()
theory_display_app = make_theory_display_app()
people_create_app = make_people_create_app()
theory_create_app = make_theory_create_app()
the_list_review_app = make_review_display_app()
admin_app      = make_admin_app()
privileges_app = make_privileges_app()
login_page     = make_login_page()

# Optional: session secret via secret manager (fallback default set in bootstrap)
session_secret = get_secret("SESSION_SECRET", default="dev-session-secret")
mount_gradio_app(app, home_app,     "/app", secret_key=session_secret)
mount_gradio_app(app, profile_app,   "/profile", secret_key=session_secret)
mount_gradio_app(app, the_list_app,  "/the-list", secret_key=session_secret)
mount_gradio_app(app, theories_app,  "/theories", secret_key=session_secret)
mount_gradio_app(app, sources_app,  "/sources", secret_key=session_secret)
mount_gradio_app(app, sources_individual_app,  "/sources-individual", secret_key=session_secret)
mount_gradio_app(app, people_display_app,  "/people-display", secret_key=session_secret)
mount_gradio_app(app, theory_display_app,  "/theory-display", secret_key=session_secret)
mount_gradio_app(app, people_create_app,  "/people-create", secret_key=session_secret)
mount_gradio_app(app, theory_create_app,  "/theory-create", secret_key=session_secret)
mount_gradio_app(app, the_list_review_app,  "/the-list-review", secret_key=session_secret)
mount_gradio_app(app, admin_app,         "/admin", secret_key=session_secret)
mount_gradio_app(app, privileges_app,   "/privileges", secret_key=session_secret)


@app.get("/people")
@app.get("/people/")
async def legacy_people_redirect() -> RedirectResponse:
    return RedirectResponse(url="/the-list/")


gr.mount_gradio_app(app, login_page, "/")
