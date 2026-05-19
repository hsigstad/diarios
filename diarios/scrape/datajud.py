"""DataJud (CNJ) public API client.

INTENT: shared infrastructure for any project that needs raw DataJud
JSONL on disk. ``download_datajud()`` writes one JSONL per
(tribunal, classes) query; helpers (``iter_search_after``,
``post_search``, ``build_classe_query``) are exposed for projects
with non-standard query shapes.

REASONING: each Brazilian court has its own DataJud endpoint
(``api_publica_<court>``); the ES query body is otherwise identical
across courts. A single function parameterized by a court alias
avoids ~60 near-duplicate scripts.

ASSUMES: the DATAJUD_APIKEY env var is set (or passed explicitly).
The public DataJud API is Elasticsearch-style; ``search_after``
pagination requires a deterministic sort on ``@timestamp`` then
``numeroProcesso.keyword``.

SOURCE: lifted from ``projects/saude/source/util/datajud_client.py``
and ``pipelines/bdata/source/scrape/datajud.py``; classe filter
switched from ``match`` on ``classe.nome`` to ``terms`` on
``classe.codigo`` for exact, code-driven queries.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://api-publica.datajud.cnj.jus.br"


def make_session() -> requests.Session:
    """Requests session with retries for transient failures (429/5xx)."""
    session = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def ensure_outdir(path: os.PathLike | str) -> None:
    """Create ``path`` (and parents) if it doesn't already exist."""
    os.makedirs(path, exist_ok=True)


def search_url(tribunal: str) -> str:
    """DataJud search URL for a court alias.

    Aliases follow CNJ's endpoint naming (``api_publica_<alias>``):
    state TJs (e.g. ``tjsp``, ``tjmg``), federal TRFs (``trf1``..``trf6``),
    superior courts (``stj``, ``stm``, ``tst``), electoral
    (``tse``, ``tre-ac``..``tre-to``), and labor TRTs (``trt1``..``trt24``).
    """
    return f"{API_BASE}/api_publica_{tribunal}/_search"


def require_apikey() -> str:
    """Read DATAJUD_APIKEY from the environment or raise."""
    apikey = os.environ.get("DATAJUD_APIKEY", "").strip()
    if not apikey:
        raise SystemExit("ERROR: Missing API key. Set env var DATAJUD_APIKEY.")
    return apikey


def post_search(
    session: requests.Session,
    apikey: str,
    body: dict[str, Any],
    url: str,
    timeout: int = 90,
) -> dict[str, Any]:
    """POST a search body to a DataJud URL and return the JSON response."""
    headers = {
        "Authorization": f"APIKey {apikey}",
        "Content-Type": "application/json",
    }
    r = session.post(url, headers=headers, json=body, timeout=timeout)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise RuntimeError(f"HTTP {r.status_code} from DataJud: {err}")
    return r.json()


def default_source_fields(include_movimentos: bool = False) -> list[str]:
    """Standard ``_source`` field list for slim DataJud queries."""
    fields = [
        "numeroProcesso",
        "classe",
        "assuntos",
        "grau",
        "orgaoJulgador",
        "tribunal",
        "dataAjuizamento",
        "dataHoraUltimaAtualizacao",
        "nivelSigilo",
        "@timestamp",
    ]
    if include_movimentos:
        fields.append("movimentos")
    return fields


def build_classe_query(
    classes: Optional[list[int]] = None,
    assunto_codigos: Optional[list[int]] = None,
) -> dict[str, Any]:
    """ES query body filtering by classe.codigo and (optionally) assuntos.codigo.

    With both args ``None``, returns the match-all query (full inventory).
    """
    filters: list[dict[str, Any]] = []
    if classes:
        filters.append({"terms": {"classe.codigo": list(classes)}})
    if assunto_codigos:
        filters.append({"terms": {"assuntos.codigo": list(assunto_codigos)}})
    if not filters:
        return {"query": {"match_all": {}}}
    return {"query": {"bool": {"filter": filters}}}


def iter_search_after(
    session: requests.Session,
    apikey: str,
    url: str,
    query_body: dict[str, Any],
    page_size: int,
    source_fields: Optional[list[str]],
    sleep_s: float,
    max_pages: Optional[int] = None,
) -> Iterable[dict[str, Any]]:
    """Generator yielding ES hits via ``search_after`` pagination.

    ``query_body`` must contain a ``"query"`` key; size, _source, sort,
    and search_after are added by this function.
    """
    search_after: Optional[list[Any]] = None
    pages = 0
    while True:
        body: dict[str, Any] = {
            "size": page_size,
            "track_total_hits": False,
            "query": query_body["query"],
            "sort": [
                {"@timestamp": {"order": "asc"}},
                {"numeroProcesso.keyword": {"order": "asc"}},
            ],
        }
        if source_fields is not None:
            body["_source"] = source_fields
        if search_after is not None:
            body["search_after"] = search_after

        data = post_search(session, apikey, body, url)
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            yield h

        last = hits[-1]
        if "sort" not in last:
            raise RuntimeError(
                "Missing 'sort' in last hit; cannot continue search_after pagination."
            )
        search_after = last["sort"]
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        if sleep_s > 0:
            time.sleep(sleep_s)


def write_jsonl(path: os.PathLike | str, rows: Iterable[dict[str, Any]]) -> int:
    """Write an iterable of ES hits to JSONL. Returns count written."""
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for hit in rows:
            out = {
                "_id": hit.get("_id"),
                "_index": hit.get("_index"),
                "_score": hit.get("_score"),
                "_source": hit.get("_source", {}),
                "sort": hit.get("sort"),
            }
            f.write(json.dumps(out, ensure_ascii=False) + "\n")
            n += 1
    return n


def count_datajud(
    tribunal: str,
    classes: Optional[list[int]],
    *,
    apikey: Optional[str] = None,
    assunto_codigos: Optional[list[int]] = None,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> int:
    """Return the total number of hits for a (tribunal, classes[, assuntos]) query.

    Issues a single ``size=0, track_total_hits=true`` request. Useful as a
    cheap pre-flight: validates the endpoint URL (404 on bad alias) and
    previews the pull size before committing to a full download.
    """
    if apikey is None:
        apikey = require_apikey()
    if session is None:
        session = make_session()

    query_body = build_classe_query(classes=classes, assunto_codigos=assunto_codigos)
    body: dict[str, Any] = {
        "size": 0,
        "track_total_hits": True,
        "query": query_body["query"],
    }
    data = post_search(session, apikey, body, search_url(tribunal), timeout=timeout)
    return int(data.get("hits", {}).get("total", {}).get("value", 0))


def download_datajud(
    tribunal: str,
    classes: Optional[list[int]],
    outpath: os.PathLike | str,
    *,
    apikey: Optional[str] = None,
    assunto_codigos: Optional[list[int]] = None,
    all_fields: bool = True,
    include_movimentos: bool = True,
    page_size: int = 2000,
    sleep_s: float = 0.2,
    max_pages: Optional[int] = None,
    session: Optional[requests.Session] = None,
) -> int:
    """Download all cases matching ``classes`` from a single court to JSONL.

    Args:
        tribunal: DataJud court alias (e.g. ``'tjsp'``, ``'trf1'``, ``'tre-sp'``).
        classes: ``classe.codigo`` values to filter on (e.g. ``[64, 65]``).
            Pass ``None`` for a match-all dump.
        outpath: JSONL output path; parent directory is created if missing.
        apikey: DATAJUD API key. If None, read from ``DATAJUD_APIKEY``.
        assunto_codigos: Optional ``assuntos.codigo`` filter applied
            server-side (AND'd with the classe filter).
        all_fields: If True, request the full ``_source``; overrides
            ``include_movimentos``.
        include_movimentos: If ``all_fields`` is False, whether to include
            ``movimentos`` in the requested ``_source`` field list.
        page_size: ES page size (1..10000).
        sleep_s: Sleep between pages (seconds).
        max_pages: Optional cap on pages, for debugging.
        session: Reuse an existing requests session; created if None.

    Returns:
        Count of hits written to ``outpath``.
    """
    if apikey is None:
        apikey = require_apikey()
    if session is None:
        session = make_session()
    if page_size <= 0 or page_size > 10000:
        raise ValueError("page_size must be in (0, 10000]")

    outpath = Path(outpath)
    outpath.parent.mkdir(parents=True, exist_ok=True)

    source_fields: Optional[list[str]]
    if all_fields:
        source_fields = None  # omit _source -> full document
    else:
        source_fields = default_source_fields(include_movimentos=include_movimentos)

    query_body = build_classe_query(classes=classes, assunto_codigos=assunto_codigos)
    url = search_url(tribunal)

    rows = iter_search_after(
        session=session,
        apikey=apikey,
        url=url,
        query_body=query_body,
        page_size=page_size,
        source_fields=source_fields,
        sleep_s=sleep_s,
        max_pages=max_pages,
    )
    return write_jsonl(outpath, rows)


__all__ = [
    "API_BASE",
    "build_classe_query",
    "count_datajud",
    "default_source_fields",
    "download_datajud",
    "ensure_outdir",
    "iter_search_after",
    "make_session",
    "post_search",
    "require_apikey",
    "search_url",
    "write_jsonl",
]
