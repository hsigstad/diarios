"""Parser for PJe public-consulta scrape JSONs (TJMG / TJMA / TRF1 / TJPE / TRF5).

The PJe scraper (``pipelines/bdata/source/scrape/PJe_consulta.py``) emits one
JSON per NPU at ``<scrape_root>/<TRIB>/<NPU>.json`` with schema_version=3:

  dados_processo       — labeled text blob (Número, Distribuição, Classe,
                         Assunto, Jurisdição, Órgão Julgador)
  polo_ativo,
  polo_passivo,
  outros_interessados  — table-tbody text dumps; each row is
                         ``<NAME> - (CPF|CNPJ|OAB): <masked>... (<ROLE>)``
  movimentacoes        — list[{text, doc_url}]; ``text`` opens with
                         ``DD/MM/YYYY HH:MM:SS - <description>``
  documents_downloaded — list[{mov_idx, filename, url, ...}] mapping each
                         downloaded HTML under ``<scrape_root>/<TRIB>/_docs/<NPU>/``

The result is five typed DataFrames: ``df`` (raw rows indexed by (npu,
tribunal)), ``proc`` (case metadata), ``parte`` (parties), ``mov`` (event
timeline), and ``inteiro_teor`` (one row per downloaded mov-attached HTML,
with raw prose text ready for ``diarios.decision.DecisionParser``).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from diarios.clean import clean_text, map_regex


# ── Party-line regex (lifted from improbidade/source/prep/PJe_consulta.py).
# Each line ends with a role tag in parens. Most rows embed a masked-document
# tag (CPF / CNPJ / OAB) before the role, but some don't — e.g. parties
# whose CPF the court didn't have on file appear as ``ELOISA FERREIRA MIGUEL
# (RÉU/RÉ)`` with no document tag. The doc tag is therefore optional. Roles
# can hold one level of nested parens (e.g. ``REQUERIDO(A)``).
_LINE_RE = re.compile(
    r"([A-ZÁÀÂÃÉÊÍÓÔÕÚÇÑ][A-ZÁÀÂÃÉÊÍÓÔÕÚÇÑ \-'/.]+?)"
    r"(?:\s*-\s*(CPF|CNPJ|OAB)[^()]*?:\s*([^()]*?))?"
    r"\s*\(\s*((?:[^()]|\([^()]*\))+?)\s*\)"
)

# Representatives, not parties — dropped at party-parsing time.
_LAWYER_ROLES = {
    "ADVOGADO", "ADVOGADA", "PROCURADOR", "PROCURADORA",
    "DEFENSOR", "DEFENSORA", "REPRESENTANTE",
    "PERITO(A)", "PERITO", "PERITA",
    "ASSISTENTE", "ASSISTENTE TÉCNICO", "ASSISTENTE TECNICO",
}

# Mov text leads with ``DD/MM/YYYY HH:MM:SS - <description>``.
_MOV_PREFIX_RE = re.compile(
    r"^\s*(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s*-\s*(.*)$",
    re.DOTALL,
)

# Filename format from the scraper:
#   mov_<NNNN>_<scrubbed_text>.html
# where <scrubbed_text> is the mov text with non-word chars → '_'.
_DOC_FILENAME_RE = re.compile(r"^mov_(\d{4})_(.*)\.html$")

# Document-type classifier (lifted from the PJe documentos vocabulary).
# Order matters — Sentença first because "Sentença" can also appear in
# "Sentença Mantida" / appellate movs. Acórdão next for the same reason.
_DOC_TYPES: List[Tuple[str, str]] = [
    (r"\bsenten[çc]a\b", "SENTENCA"),
    (r"\bac[óo]rd[ãa]o\b", "ACORDAO"),
    (r"\bdecis[ãa]o", "DECISAO"),
    (r"\bdespacho", "DESPACHO"),
    (r"\bata de audi[êe]ncia", "ATA"),
]


# ─── top-level ──────────────────────────────────────────────────────────────

def parse_consulta_pje(
    scrape_root: str | Path,
    case_numbers: Optional[Iterable[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse a PJe consulta scrape tree into relational DataFrames.

    Args:
        scrape_root: directory holding ``<TRIB>/<NPU>.json`` files (e.g.
            ``pipelines/bdata/build/scrape/PJe``).
        case_numbers: optional set of NPUs to restrict to. If None, all
            ``status == 'OK'`` records under scrape_root are loaded.

    Returns:
        (df, proc, parte, mov, inteiro_teor).
        - df indexed by (npu, tribunal), one row per OK scrape record.
        - proc, parte, mov, inteiro_teor are typed tables keyed off (npu, ...).
    """
    df = get_df(scrape_root, case_numbers)
    proc = get_proc(df)
    parte = get_parte(df)
    mov = get_mov(df)
    inteiro_teor = get_inteiro_teor(df, scrape_root)
    return df, proc, parte, mov, inteiro_teor


# ─── load ───────────────────────────────────────────────────────────────────

def get_df(
    scrape_root: str | Path,
    case_numbers: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Load all OK scrape records under scrape_root into one frame.

    Indexed by (npu, tribunal). NOT_FOUND / SEGREDO / ERROR rows are dropped
    silently — callers can re-run their own audit via ``scraped.csv`` if they
    care about per-status counts.
    """
    base = Path(scrape_root)
    targets = frozenset(case_numbers) if case_numbers is not None else None
    rows: list[dict] = []
    for tdir in sorted(base.iterdir()) if base.exists() else []:
        if not tdir.is_dir() or tdir.name.startswith("_"):
            continue
        tribunal = tdir.name
        for jf in sorted(tdir.glob("*.json")):
            try:
                rec = json.loads(jf.read_text(encoding="utf8"))
            except Exception:
                continue
            if rec.get("status") != "OK":
                continue
            npu = rec.get("npu")
            if targets is not None and npu not in targets:
                continue
            rows.append({
                "npu": npu,
                "tribunal": tribunal,
                "dados_processo": rec.get("dados_processo"),
                "polo_ativo": rec.get("polo_ativo"),
                "polo_passivo": rec.get("polo_passivo"),
                "outros_interessados": rec.get("outros_interessados"),
                "movimentacoes": rec.get("movimentacoes") or [],
                "documents_downloaded": rec.get("documents_downloaded") or [],
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df.set_index(["npu", "tribunal"]) if {"npu", "tribunal"}.issubset(df.columns) else df
    df = df.drop_duplicates("npu", keep="first").set_index(["npu", "tribunal"])
    return df


# ─── proc ───────────────────────────────────────────────────────────────────

def get_proc(df: pd.DataFrame) -> pd.DataFrame:
    """Extract case metadata from dados_processo into one row per (npu, tribunal)."""
    keys = {
        "Número Processo": "numero_processo",
        "Data da Distribuição": "data_distribuicao",
        "Classe Judicial": "classe",
        "Assunto": "assunto",
        "Jurisdição": "jurisdicao",
        "Órgão Julgador": "orgao_julgador",
        "Processo Referência": "processo_referencia",
    }
    text = df["dados_processo"].fillna("")
    out = pd.DataFrame(index=df.index)
    for label, col in keys.items():
        # PJe dados_processo is a multi-line dump with each label on its
        # own line followed by tab/blank-padded value. Capture text after
        # the label up to the next blank-line / next label / EOS.
        pat = rf"{re.escape(label)}\s*\n[\s\n]*([^\n]+?)\s*\n"
        out[col] = text.str.extract(pat, flags=re.IGNORECASE | re.DOTALL)[0].str.strip()
        out[col] = out[col].replace({"": None})

    out["data_distribuicao"] = pd.to_datetime(
        out["data_distribuicao"], dayfirst=True, errors="coerce"
    )

    # Map raw classe text to short codes BEFORE clean_text strips diacritics
    # (otherwise "AÇÃO" → "ACAO" and the patterns miss). PJe classe values
    # look like "[CÍVEL] AÇÃO CIVIL DE IMPROBIDADE ADMINISTRATIVA (64)".
    classes = {
        "AÇÃO CIVIL DE IMPROBIDADE ADMINISTRATIVA": "ACIA",
        "AÇÃO CIVIL PÚBLICA": "ACP",
        "AÇÃO PENAL": "APN",
    }
    out["classe_short"] = map_regex(out["classe"], classes)

    for col in ("classe", "assunto", "jurisdicao", "orgao_julgador"):
        out[col] = clean_text(out[col])
    return out


# ─── parte ──────────────────────────────────────────────────────────────────

def get_parte(df: pd.DataFrame) -> pd.DataFrame:
    """Consolidate polo_ativo / polo_passivo / outros_interessados into one table.

    Output columns: npu, tribunal, parte, papel, tipo_parte, cpf_masked.
    tipo_parte ∈ {PLAINTIFF, DEFENDANT, OUTROS}. cpf_masked is the
    document fragment as displayed by PJe (e.g. ``013.***.***-**``) —
    useful only as a debug hint; the join key for politico_id matching
    is the party name, since PJe masks digits.
    """
    rows: list[dict] = []
    for (npu, tribunal), row in df.iterrows():
        for col, tipo in (
            ("polo_ativo", "PLAINTIFF"),
            ("polo_passivo", "DEFENDANT"),
            ("outros_interessados", "OUTROS"),
        ):
            for name, doc_kind, doc_val, role in _parse_polo(row.get(col)):
                rows.append({
                    "npu": npu,
                    "tribunal": tribunal,
                    "parte": name,
                    "papel": role,
                    "tipo_parte": tipo,
                    "cpf_masked": doc_val if doc_kind in ("CPF", "CNPJ") else None,
                })
    parte = pd.DataFrame(
        rows, columns=["npu", "tribunal", "parte", "papel", "tipo_parte", "cpf_masked"]
    )
    return parte.drop_duplicates(["npu", "parte", "papel", "tipo_parte"])


def _parse_polo(text: Optional[str]) -> list[tuple[str, Optional[str], Optional[str], str]]:
    if not text or not isinstance(text, str):
        return []
    out: list[tuple[str, Optional[str], Optional[str], str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in text.splitlines():
        m = _LINE_RE.search(raw)
        if not m:
            continue
        name = re.sub(r"\s+", " ", m.group(1)).strip(" -")
        doc_kind = m.group(2)
        doc_val = m.group(3).strip() if m.group(3) else None
        role = m.group(4).strip()
        if role.upper() in _LAWYER_ROLES:
            continue
        key = (name, role)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, doc_kind, doc_val, role))
    return out


# ─── mov ────────────────────────────────────────────────────────────────────

def get_mov(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (npu, mov_idx). mov_idx is the position in the JSON list.

    PJe paginates movimentacoes newest-first, so mov_idx 0 is the most
    recent event and the highest mov_idx is filing.
    """
    rows: list[dict] = []
    for (npu, tribunal), row in df.iterrows():
        for idx, item in enumerate(row.get("movimentacoes") or []):
            text = (item.get("text") or "").strip()
            doc_url = item.get("doc_url")
            m = _MOV_PREFIX_RE.match(text)
            if m:
                date_str = f"{m.group(1)} {m.group(2)}"
                description = m.group(3).strip()
            else:
                date_str = None
                description = text
            rows.append({
                "npu": npu,
                "tribunal": tribunal,
                "mov_idx": idx,
                "date": date_str,
                "text": description,
                "has_doc": doc_url is not None,
                "doc_url": doc_url,
            })
    mov = pd.DataFrame(
        rows, columns=["npu", "tribunal", "mov_idx", "date", "text", "has_doc", "doc_url"]
    )
    if not mov.empty:
        mov["date"] = pd.to_datetime(mov["date"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    return mov


# ─── inteiro teor ───────────────────────────────────────────────────────────

def get_inteiro_teor(
    df: pd.DataFrame,
    scrape_root: str | Path,
    read_text: bool = True,
) -> pd.DataFrame:
    """One row per downloaded inteiro-teor HTML.

    Joins documents_downloaded back to the mov text (for date + a richer
    description), classifies the doc_type (SENTENCA / ACORDAO / DECISAO /
    DESPACHO / ATA / OUTRO), and optionally reads + cleans the HTML text.

    HTML files are written by the scraper to
    ``<scrape_root>/<TRIB>/_docs/<NPU>/<filename>`` in ISO-8859-1.
    """
    base = Path(scrape_root)
    rows: list[dict] = []
    for (npu, tribunal), row in df.iterrows():
        movs = row.get("movimentacoes") or []
        for d in row.get("documents_downloaded") or []:
            if d.get("http_status") != 200:
                continue
            mov_idx = d.get("mov_idx")
            filename = d.get("filename")
            mov_text = movs[mov_idx]["text"] if (
                mov_idx is not None and mov_idx < len(movs)
            ) else None
            doc_type = _classify_doc(filename or "", mov_text or "")
            doc_path = base / tribunal / "_docs" / npu / filename if filename else None
            text = None
            if read_text and doc_path is not None and doc_path.exists():
                try:
                    text = _read_html_text(doc_path)
                except Exception:
                    text = None
            rows.append({
                "npu": npu,
                "tribunal": tribunal,
                "mov_idx": mov_idx,
                "doc_type": doc_type,
                "filename": filename,
                "doc_path": str(doc_path) if doc_path is not None else None,
                "text": text,
            })
    return pd.DataFrame(
        rows,
        columns=["npu", "tribunal", "mov_idx", "doc_type", "filename", "doc_path", "text"],
    )


def _classify_doc(filename: str, mov_text: str) -> str:
    haystack = f"{filename} {mov_text}".lower()
    for pat, label in _DOC_TYPES:
        if re.search(pat, haystack):
            return label
    return "OUTRO"


def _read_html_text(path: Path) -> str:
    """Read a PJe inteiro-teor HTML and return cleaned body text.

    PJe serves these as ISO-8859-1; HTML entities are decoded.
    """
    import html as _html

    raw = path.read_text(encoding="iso-8859-1")
    body = re.search(r"<body[^>]*>(.*?)</body>", raw, re.DOTALL | re.IGNORECASE)
    src = body.group(1) if body else raw
    src = re.sub(r"<script.*?</script>", " ", src, flags=re.DOTALL | re.IGNORECASE)
    src = re.sub(r"<style.*?</style>", " ", src, flags=re.DOTALL | re.IGNORECASE)
    src = re.sub(r"<[^>]+>", " ", src)
    src = _html.unescape(src)
    src = re.sub(r"\s+", " ", src).strip()
    return src
