"""diarios.scrape.proxy — optional Decodo residential proxy egress (shared across repos).

INTENT
    Several BR gov portals only yield to a Brazilian RESIDENTIAL IP; a datacenter /
    commercial-VPN egress (e.g. PIA) is geoblocked or its reCAPTCHA score is bot-penalized
    (bllcompras invisible reCAPTCHA → "Captcha inválido"; TCE-MG geoblock + v3 score penalty).
    This centralizes the Decodo (formerly Smartproxy) endpoint so every scraper — in any repo —
    shares ONE proxy construction instead of re-deriving the (non-obvious) syntax.

CREDS  <research-root>/.secrets/research.env (see research/rules/secrets.md, "Residential proxy
    egress"). This shared file lives INSIDE the workspace tree so sandboxed sessions — which mount
    ~/research -> /workspace but NOT the host's ~/.config — can reach it. Resolution order (first
    existing file wins): $RESEARCH_SECRETS, then <root>/.secrets/research.env discovered relative to
    this module and by walking up from the CWD, then the legacy ~/.config/research/secrets.env.
    DECODO_HOST=gate.decodo.com   DECODO_PORT=7000
    DECODO_USER=<base>            DECODO_PASS=<pass>

REASONING (verified 2026-08-04, empirically against ip-api + bllcompras):
    Geo + session live in the USERNAME, not the port. The `user-` prefix is MANDATORY —
    `<base>-country-br` without it auth-denies. Port 7000 is the rotating gateway: plain
    `<base>` there draws a RANDOM country, so the country MUST be pinned in the username.
    Passing session= appends a sticky-session tag so an IP is held ~10 min — required when a
    captcha challenge and its follow-up request must egress from ONE IP.

ASSUMES the four DECODO_* vars are present. If any is missing every constructor returns None,
    and callers fall back to their existing direct egress — so importing this module is always
    safe even without creds. python-dotenv is an optional diarios extra; if absent, the module
    relies on the vars already being in the environment.
"""
from __future__ import annotations

import os
import urllib.request
from pathlib import Path

def _secret_files() -> list[Path]:
    """Candidate shared-secret files, most-preferred first (first existing one wins).

    The canonical location moved (2026-08-25) into the workspace tree — <root>/.secrets/research.env
    — so sandboxed sessions can reach it (they mount ~/research -> /workspace but not host ~/.config).
    We locate <root> two ways for robustness: relative to this module (…/packages/diarios/diarios/
    scrape/proxy.py -> parents[4]) and by walking up from the CWD; the legacy ~/.config path is kept
    last for backward compatibility. An explicit $RESEARCH_SECRETS overrides everything.
    """
    cands: list[Path] = []
    env = os.environ.get("RESEARCH_SECRETS")
    if env:
        cands.append(Path(env))
    try:  # module-relative research root (holds when diarios is imported from the tree)
        cands.append(Path(__file__).resolve().parents[4] / ".secrets/research.env")
    except IndexError:  # pragma: no cover — module unexpectedly shallow in the tree
        pass
    cwd = Path.cwd()
    for d in (cwd, *cwd.parents):  # handles launch from a subdir / non-editable install
        cands.append(d / ".secrets/research.env")
    cands.append(Path.home() / ".config/research/secrets.env")  # legacy shared location
    seen: set[Path] = set()
    return [p for p in cands if not (p in seen or seen.add(p))]


try:  # python-dotenv is an optional diarios extra; degrade gracefully to the ambient environment
    from dotenv import load_dotenv

    for _secret_file in _secret_files():
        if _secret_file.is_file():
            load_dotenv(_secret_file)
            break
except ModuleNotFoundError:  # noqa: S110 — vars may already be exported; nothing to load
    pass


def _creds() -> dict | None:
    try:
        return {
            "host": os.environ["DECODO_HOST"],
            "port": os.environ["DECODO_PORT"],
            "user": os.environ["DECODO_USER"],
            "pw": os.environ["DECODO_PASS"],
        }
    except KeyError:
        return None


def _egress_user(base: str, country: str, session: str | None) -> str:
    """Build the Decodo egress username: geo (+ optional sticky session) live here."""
    u = f"user-{base}-country-{country}"
    if session:
        u += f"-session-{session}-sessionduration-10"
    return u


def proxy_url(session: str | None = None, country: str = "br") -> str | None:
    """Full http://user:pass@host:port URL (None if creds absent)."""
    c = _creds()
    if not c:
        return None
    return f"http://{_egress_user(c['user'], country, session)}:{c['pw']}@{c['host']}:{c['port']}"


def playwright_proxy(session: str | None = None, country: str = "br") -> dict | None:
    """Dict for playwright chromium.launch(proxy=...) (None if creds absent)."""
    c = _creds()
    if not c:
        return None
    return {
        "server": f"http://{c['host']}:{c['port']}",
        "username": _egress_user(c["user"], country, session),
        "password": c["pw"],
    }


def urllib_proxy_handler(session: str | None = None, country: str = "br"):
    """urllib ProxyHandler routing http+https through the BR egress (None if creds absent).

    Stateless and read-only during requests → safe to build once and share across threads
    (unlike an OpenerDirector with a CookieJar)."""
    url = proxy_url(session=session, country=country)
    if not url:
        return None
    return urllib.request.ProxyHandler({"http": url, "https": url})
