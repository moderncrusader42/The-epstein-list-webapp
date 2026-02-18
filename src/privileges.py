from __future__ import annotations

from dataclasses import dataclass
import os
from typing import List, Mapping, Set, Optional


@dataclass(frozen=True)
class PageLink:
    key: str
    label: str
    path: str
    css_class: str


NAVIGATION_ORDER: tuple[str, ...] = (
    "home",
    "the-list",
    "theories",
    "sources",
    "unsorted-files",
    "the-list-review",
    "privileges",
    "admin",
)

PUBLIC_PAGE_KEYS: tuple[str, ...] = (
    "home",
    "the-list",
    "theories",
    "sources",
)

PAGE_REGISTRY: dict[str, PageLink] = {
    "home": PageLink("home", "Home", "/app/", "hdr-link hdr-link--home"),
    "the-list": PageLink("the-list", "The List", "/the-list/", "hdr-link hdr-link--the-list"),
    "theories": PageLink("theories", "Theories", "/theories/", "hdr-link hdr-link--theories"),
    "sources": PageLink("sources", "Sources", "/sources/", "hdr-link hdr-link--sources"),
    "unsorted-files": PageLink(
        "unsorted-files",
        "Unsorted files",
        "/unsorted-files/",
        "hdr-link hdr-link--unsorted-files",
    ),
    "the-list-review": PageLink(
        "the-list-review",
        "The List Review",
        "/the-list-review/",
        "hdr-link hdr-link--the-list-review",
    ),
    "privileges": PageLink(
        "privileges",
        "Privileges",
        "/privileges/",
        "hdr-link hdr-link--privileges",
    ),
    "admin": PageLink("admin", "Administration", "/admin/", "hdr-link hdr-link--admin"),
}

_EMPTY: set[str] = set()
PRIVILEGE_PAGE_MAP: dict[str, set[str]] = {
    "base_user": {"home", "the-list", "theories", "sources", "unsorted-files"},
    "reviewer": {"home", "the-list", "theories", "sources", "unsorted-files", "the-list-review"},
    "editor": {"home", "the-list", "theories", "sources", "unsorted-files"},
    "admin": {"home", "the-list", "theories", "sources", "unsorted-files", "privileges"},
    "creator": {"home", "the-list", "theories", "sources", "unsorted-files", "privileges", "admin"},
}

DEBUG_PRIVILEGES_ENV = "THELIST_DEBUG_PRIVILEGES"
_DEBUG_TRUE_VALUES = {"1", "true", "yes", "on"}

PATH_TO_PAGE_KEY: dict[str, Optional[str]] = {
    "/app": "home",
    "/the-list": "the-list",
    "/theories": "theories",
    "/sources": "sources",
    "/source-create": "sources",
    "/sources-individual": "sources",
    "/unsorted-files": "unsorted-files",
    "/people-display": "the-list",
    "/theory-display": "theories",
    "/people-create": "the-list",
    "/the-list-review": "the-list-review",
    "/admin": "admin",
    "/privileges": "privileges",
    "/profile": None,
}

PrivilegeMapping = Mapping[str, bool]


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _enabled_privilege_keys(privileges: PrivilegeMapping | None) -> tuple[str, ...]:
    if not privileges:
        return ()
    normalized: list[str] = []
    for key, enabled in privileges.items():
        if not enabled:
            continue
        normalized_key = (key or "").strip().lower()
        if not normalized_key:
            continue
        normalized.append(normalized_key)
    return tuple(normalized)


def resolve_nav_links(privileges: PrivilegeMapping | None) -> List[PageLink]:
    """
    Produce the ordered list of PageLink entries that should be visible for the given privilege set.
    """
    if _is_debug_mode():
        return [PAGE_REGISTRY[key] for key in NAVIGATION_ORDER]
    allowed: set[str] = set(PUBLIC_PAGE_KEYS)
    normalized = _enabled_privilege_keys(privileges)
    for privilege in normalized:
        allowed.update(PRIVILEGE_PAGE_MAP.get(privilege, set()))

    links = [PAGE_REGISTRY[key] for key in NAVIGATION_ORDER if key in allowed]
    if links:
        return links
    return [PAGE_REGISTRY[key] for key in NAVIGATION_ORDER if key in PUBLIC_PAGE_KEYS]


def _normalize_route(route: str) -> str:
    route = route or "/"
    if not route.startswith("/"):
        route = f"/{route}"
    if route != "/" and route.endswith("/"):
        route = route.rstrip("/")
    return route


def page_key_for_route(route: str) -> Optional[str]:
    """
    Resolve a canonical route (e.g., '/app') to the corresponding page key.
    """
    return PATH_TO_PAGE_KEY.get(_normalize_route(route))


def accessible_page_keys(privileges: PrivilegeMapping | None) -> Set[str]:
    """
    Return the set of page keys the user is allowed to visit (derived from nav links).
    """
    if _is_debug_mode():
        return set(NAVIGATION_ORDER)
    return {link.key for link in resolve_nav_links(privileges)}


def user_can_access_page(privileges: PrivilegeMapping | None, page_key: str) -> bool:
    """
    Check if the given privilege list allows rendering/visiting the page key.
    """
    if page_key not in PAGE_REGISTRY:
        return True
    if _is_debug_mode():
        return True
    return page_key in accessible_page_keys(privileges)


def default_page_path(privileges: PrivilegeMapping | None) -> str:
    """
    Return the first path the user should land on, based on their privileges.
    """
    links = resolve_nav_links(privileges)
    return links[0].path


def _is_debug_mode() -> bool:
    value = os.getenv(DEBUG_PRIVILEGES_ENV, "")
    return value.strip().lower() in _DEBUG_TRUE_VALUES
