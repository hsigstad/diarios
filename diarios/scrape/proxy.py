"""diarios.scrape.proxy — optional Decodo residential proxy egress (shared across repos).

INTENT
    Several BR gov portals only yield to a Brazilian RESIDENTIAL IP; a datacenter /
    commercial-VPN egress (e.g. PIA) is geoblocked or its reCAPTCHA score is bot-penalized
    (bllcompras invisible reCAPTCHA → "Captcha inválido"; TCE-MG geoblock + v3 score penalty).
    This centralizes the Decodo (formerly Smartproxy) endpoint so every scraper — in any repo —
    shares ONE proxy construction instead of re-deriving the (non-obvious) syntax.

CREDS  <workspace-root>/.secrets/research.env, located by `diarios.secrets` (see
    research/rules/secrets.md, "Residential proxy egress"). The shared file lives INSIDE the
    workspace tree so sandboxed sessions — which mount the workspace root -> /workspace but NOT
    the host's ~/.config — can reach it. diarios.secrets owns the resolution order.
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

from diarios.secrets import load_secrets

load_secrets()   # no-op when the file is absent or python-dotenv isn't installed


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
