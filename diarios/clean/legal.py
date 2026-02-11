"""Legal domain cleaning and extraction functions."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import numpy as np
import re

from diarios.clean.text import clean_text, get_data, map_regex, split_series, extract_from_list
from diarios.clean.numbers import clean_number


def clean_parte(
    partes: pd.Series,
    delete: Optional[Union[str, List[str]]] = None,
    remove: Union[str, List[str]] = "[^ ]+:.*",
    remove_after: Union[str, List[str]] = [
        "(^| )DRA?S? ",
        "^(OS?|AS?|S) ",
        " E OUTRO.*",
        " E$",
    ],
    mapping: Dict[str, str] = {"MINISTERIO PUBLICO": "MP", "JUSTICA PUBLICA": "MP"},
    **kwargs: Any,
) -> pd.Series:
    """Clean party names by removing titles, suffixes, and applying mappings.

    Args:
        partes: Series of party name strings.
        delete: Regex pattern(s) to delete matching entries entirely.
        remove: Regex pattern(s) to remove from text.
        remove_after: Regex pattern(s) to strip trailing content.
        mapping: Dict mapping regex patterns to replacement values.
        **kwargs: Additional keyword arguments passed to ``clean_text``.

    Returns:
        Cleaned party name series.
    """
    if isinstance(remove, list):
        remove = "|".join(remove)
    if isinstance(remove_after, list):
        remove_after = "|".join(remove_after)
    if isinstance(delete, list):
        delete = "|".join(delete)
    partes = partes.str.replace(remove, "", regex=True)
    partes = clean_text(partes, **kwargs)
    partes = map_regex(partes, mapping)
    partes = partes.str.replace(remove_after, "", regex=True)
    if delete:
        partes.loc[partes.str.contains(delete, regex=True)] = ""
    return partes


def clean_parte_key(keywords: pd.Series) -> pd.Series:
    """Clean party keywords by removing trailing articles."""
    return clean_text(keywords).str.replace(" A?O?S?$", "", regex=True).str.strip()


def clean_tipo_parte(keywords: pd.Series) -> pd.Series:
    """Map Portuguese party type keywords to English labels.

    Args:
        keywords: Series of party type descriptions in Portuguese.

    Returns:
        Series with standardized English party type labels.
    """
    mapping = {
        "INTERESSADO": "THIRD PARTY",
        "VITIM": "VICTIM",
        "ADV|DR|REPR": "LAWYER",  # HAS TO BE BEFORE DEFD AND PLAINTIFF
        "AUTOR DO FATO": "DEFENDANT",
        "AUTOR|ENTE$|ANTE$|REQTE|EXEQTE": "PLAINTIFF",
        "LIT AT|ATIV": "PLAINTIFF",
        "^-$|^\*\*$": "PLAINTIFF",
        "PROMOTOR": "PLAINTIFF",
        "REU|^RES?$|PARTE RE|DOS?$|DAS?$": "DEFENDANT",
        "REQUERID": "DEFENDANT",
        "REQUERENT": "PLAINTIFF",
        "REQD|EXECTD": "DEFENDANT",
        "^X$": "DEFENDANT",
        "PASSIV|LIT PA?S|LITISPA": "DEFENDANT",
        "PACIENTE": "PACIENTE",
    }
    return map_regex(keywords, mapping)


def clean_classe(classes: pd.Series) -> pd.Series:
    """Clean and map legal case class descriptions to abbreviations.

    Args:
        classes: Series of case class descriptions.

    Returns:
        Series of abbreviated class codes.
    """
    classes = clean_text(classes)
    mapping = {
        "IMPROB": "ACIA",
        "POPULAR": "APop",
        "PUBLICA": "ACP",
        "AGRAVO DE INSTRUMENTO": "AI",
        "APELACAO": "Ap",
        "PROCEDIMENTO ORDINARIO": "ProOrd",
        "PROCEDIMENTO SUMARIO": "ProSum",
    }
    return map_regex(classes, mapping)


def clean_decision(decisions: pd.Series, grau: str = "1") -> pd.Series:
    """Clean and map judicial decision text to standardized labels.

    Args:
        decisions: Series of decision text strings.
        grau: Court level (``"1"`` for first instance, ``"2"`` for appellate).

    Returns:
        Series of standardized decision labels.
    """
    decisions = clean_text(decisions)
    mapping = {
        "JULGO .{0,5}PAR.{0,10}PROCEDENTE": "PARCIALMENTE PROCEDENTE",
        "JULGO.{0,5} PROCEDENTES? ((PARCIALMENTE)|(EM PARTE))": "PARCIALMENTE PROCEDENTE",
        "JULGO.{0,5} PROCEDENTE": "PROCEDENTE",
        "JULGO.{0,5} IMPROCEDENTE": "IMPROCEDENTE",
        "INDEFIRO.{0,20}DESBLOQUEIO": "INDEFIRO DESBLOQUEIO",
        "^DEFIRO.{0,20}DESBLOQUEIO": "DEFIRO DESBLOQUEIO",
        "RECEBO.{0,20}( ACAO|INICIAL)": "RECEBO INICIAL",
        "^DEFIRO.{0,20}BLOQUEIO": "DEFIRO BLOQUEIO",
        "^DEFIRO.{0,20}LIMINAR": "DEFIRO LIMINAR",
        "INDEFIRO.{0,20}BLOQUEIO": "INDEFIRO BLOQUEIO",
        "INDEFIRO.{0,20}LIMINAR": "INDEFIRO LIMINAR",
        "MANTENHO.{0,20}BLOQUEIO": "MANTENHO BLOQUEIO",
        "EMBARGOS.{0,30}REJEIT": "REJEITO EMBARGOS",
        "PRELIMINAR.{0,10}NAO.{0,10}ACHOLIDAS": "INDEFIRO LIMINAR",
        "(INDEF|REJEIT).{0,20}( ACAO|INICIAL)": "REJEITO INICIAL",
        "REJEITO.{0,20}EMBARGOS": "REJEITO EMBARGOS",
        "HOMOLOGO.{0,70}ACORDO": "HOMOLOGO ACORDO",
        "HOMOLOGADA.{0,10}TRANSACAO": "HOMOLOGO ACORDO",
        "HOMOLOGO.{0,40}PEDIDO DE DESISTENCIA": "HOMOLOGO DESISTENCIA",
        "SEM .{10,20} MERITO": "EXTINTO SEM MERITO",
        "EXTINT.{0,20}PUNIBILIDADE": "EXTINTO PUNIBILIDADE",
    }
    if grau == "2":
        mapping = {
            "DERAM": "DERAM",
            "NEGAR": "NEGAR",
            "DENEGAR": "DENEGAR",
            "REJEIT": "REJEIT",
            "NAO CONHECER": "NAO CONHECER",
        }
    return map_regex(decisions, mapping)


def get_decision(texts: pd.Series, grau: str = "1") -> pd.Series:
    """Extract judicial decision fragments from text using prioritized regex list.

    Args:
        texts: Series of full decision text strings.
        grau: Court level (``"1"`` or ``"2"``).

    Returns:
        Series of extracted decision fragments.
    """
    regex_list = [
        "(JULGO .{0,20}PROCEDENTES?)( PARCIALMENTE )?( EM PARTE.? )?",
        "RECEBO .{0,20}( ACAO|INICIAL)",
        "INICIAL .{0,20}RECEBIDA",
        "DOU .{0,20}SANEADO",
        "PRELIMINARES.{0,20}ACOLHIDAS",
        "MANTENHO A DECISAO AGRAVADA",
        "RECEBO A APELACAO",
        "DECLARO ENCERRADA A INSTRUCAO PROCESSUAL",
        "MANTENHO .{0,20}BLOQUEIOS?",
        "(IN)?DEFIRO .{0,20}DESBLOQUEIO",
        "RECEBO OS EMBARGOS.{0,40}REJEITO",
        "DECLARO SUSPENSO",
        "DEFIRO .{0,20}BLOQUEIO",
        "CONHECO .{0,10}EMBARGOS.{0,20}PROVIMENTO",
        "(IN)?DEFIRO .{0,20}LIMINAR",
        "REJEIT(O|A) .{0,30}( ACAO|INICIAL|EMBARGOS)",
        "INDEF(ERIDA|IRO).{0,20}INICIAL",
        "HOMOLOGO .{0,70}ACORDO",
        "HOMOLOGADA .{0,10}TRANSACAO",
        "HOMOLOGO .{0,40}PEDIDO DE DESISTENCIA",
        "EXTINGO .{10,30} SEM .{10,20} MERITO",
        "EXTINT.{0,20}PUNIBILIDADE",
        "HOMOLOGO .{0,10}DESISTENCIA.{0,20}TESTEMUNHAS?",
    ]
    if grau == "2":
        regex_list = [
            (
                "(REJEI|DERAM|NEGAR|DENEGAR|NAO CONHECER)"
                ".{0,40}(RECURSO|INTERNO|AGRAVO|EMBARGOS|ORDEM)"
                "(.{0,5}V\. ?U\.)?"
            ),
            "(RECURSO.{0,20}PROVIDO)(.{0,5}V\. ?U\.)?",
        ]
    return extract_from_list(texts, regex_list)


def get_procedencia(
    texts: pd.Series,
    regex: Union[str, List[str]] = (
        "(?s)(?i)((julgo\s.{0,20}procedentes?)" "(\sparcialmente\s)?(\sem\sparte.?\s)?)"
    ),
    mapping: Dict[str, str] = {
        "PAR.{0,10}PROCEDENTE": "PARCIALMENTE PROCEDENTE",
        "PROCEDENTES? ((PARCIALMENTE)|(EM PARTE))": "PARCIALMENTE PROCEDENTE",
        r"\bPROCEDENTE": "PROCEDENTE",
        r"\bIMPROCEDENTE": "IMPROCEDENTE",
    },
    keep_unmatched: bool = True,
) -> pd.Series:
    """Extract procedencia (ruling outcome) from legal decision texts.

    Args:
        texts: Series of decision text strings.
        regex: Regex pattern(s) to extract the ruling fragment.
        mapping: Dict mapping regex patterns to standardized outcomes.
        keep_unmatched: If True, keep original text when no mapping matches.

    Returns:
        Series of standardized ruling outcome labels.
    """
    if isinstance(regex, str):
        regex = [regex]
    decision = texts.str.extract(regex[0])[0]
    if len(regex) > 1:
        for r in regex[1:]:
            decision.loc[decision.isnull()] = texts.str.extract(r)[0]
    decision = clean_text(decision)
    return map_regex(decision, mapping, keep_unmatched=keep_unmatched)


def get_plaintiffwins(decision: pd.Series, parcial: int = 1) -> pd.Series:
    """Map decision labels to plaintiff win indicators (0/1).

    Args:
        decision: Series of standardized decision labels.
        parcial: Value to assign for partially favorable outcomes.

    Returns:
        Series of numeric win indicators.
    """
    return decision.map(get_plaintiffwins_mapping(parcial=parcial))


def get_plaintiffwins_mapping(parcial: int = 1) -> Dict[str, int]:
    """Return mapping from decision labels to plaintiff win indicators.

    Args:
        parcial: Value for partially favorable outcomes (0 or 1).

    Returns:
        Dict mapping decision label strings to numeric indicators.
    """
    return {
        "IMPROCEDENTE": 0,
        "PARCIALMENTE PROCEDENTE": parcial,
        "PROCEDENTE": 1,
        "RECEBO INICIAL": 1,
        "REJEITO INICIAL": 0,
        "DEFIRO LIMINAR": 1,
        "INDEFIRO LIMINAR": 0,
        "DEFIRO DESBLOQUEIO": 0,
        "INDEFIRO DESBLOQUEIO": 1,
        "DEFIRO BLOQUEIO": 1,
        "INDEFIRO BLOQUEIO": 0,
        "MANTENHO BLOQUEIO": 1,
        "REJEITO EMBARGOS": 1,
        "PRELIMINAR NÃO ACHOLIDA": 1,
        "EXTINTO SEM MERITO": 0,
        "EXTINTO PUNIBILIDADE": 0,
        "DERAM": 1,
        "NEGAR": 0,
        "DENEGAR": 0,
        "REJEIT": 0,
        "NAO CONHECER": 0,
    }


def clean_valor(valores: pd.Series) -> pd.Series:
    """Convert Brazilian monetary values from dot-thousands/comma-decimal to numeric strings."""
    return valores.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)


def clean_date(dates: pd.Series) -> pd.DataFrame:
    """Extract YYYY-MM-DD dates, normalizing separators.

    Args:
        dates: Series of date strings.

    Returns:
        DataFrame with extracted date strings.
    """
    return (
        dates.fillna("")
        .astype(str)
        .str.replace("/", "-", regex=False)
        .str.extract("([0-9]{4}-[0-9]{2}-[0-9]{2})")
    )


def clean_line(lines: pd.Series) -> pd.Series:
    """Convert line numbers to numeric, coercing errors to NaN."""
    return pd.to_numeric(lines, errors="coerce")


def get_decisao_id(decisoes: pd.Series) -> pd.Series:
    """Map decision labels to their numeric IDs from lookup table."""
    ids = get_data("decisao.csv")
    mapping = dict(zip(ids.decisao, ids.decisao_id))
    return decisoes.map(mapping)


def get_tipo_parte_id(tipo_partes: pd.Series) -> pd.Series:
    """Map party type labels to their numeric IDs from lookup table."""
    ids = get_data("tipo_parte.csv")
    mapping = dict(zip(ids.tipo_parte, ids.tipo_parte_id))
    return tipo_partes.map(mapping)


def clean_lei(lei: pd.Series) -> pd.Series:
    """Standardize legal act references to short codes (e.g. ``"L8666/93"``).

    Args:
        lei: Series of legal act name strings.

    Returns:
        Series of standardized short code strings.
    """
    contains_number = lei.str.contains('[0-9]')
    is_lei = lei.str.contains('(?i)lei')
    decreto_lei = lei.str.contains(r'(?i)decreto|\bdl\b')
    lei_complementar = lei.str.contains(r'(?i)lei\s+complementar|\blc\b')
    number = (
        lei
        .str.replace('[,.]', '', regex=True)
        .str.extract('([0-9/]+)', expand=False)
    )
    lei.loc[contains_number & is_lei] = 'L' + number.loc[contains_number & is_lei]
    lei.loc[contains_number & decreto_lei] = 'DL' + number.loc[contains_number & decreto_lei]
    lei.loc[contains_number & lei_complementar] = 'LC' + number.loc[contains_number & lei_complementar]
    lei = lei.str.replace('/(19|20)([0-9]{2})', r'/\2', regex=True) # L8666/93 instead of L8666/1993
    return lei


def _clean_fundamento(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize fundamento (legal basis) fields in extracted citations."""
    df.loc[df.inciso=='CAPUT', 'paragrafo'] = "0"
    df.loc[df.inciso=='CAPUT', 'inciso'] = ""
    df.loc[df.paragrafo=='UNICO', 'paragrafo'] = "1"
    df['alinea'] = df.alinea.fillna(df.alinea_paragrafo).fillna('')
    df['citation'] = df.citation.str.replace('\s+', ' ', regex=True)
    return df


def extract_fundamentos(
        text: pd.Series,
        lei_regexes: List[str] = [
            'CPC', 'LIA', 'NCPC', 'CPP', 'CF', 'CP', 'CPB',
            r'\b(?:lei|dl|decreto.{0,2}lei|lei\s+complementar|lc)\b.{0,6}?[0-9][0-9./]+',
            'Lei\s+de\s+Improbidade',
            'Código\s+Penal',
            'Código\s+Eleitoral',
            'Código\s+d[oe]\s+Processo\s+Penal',
            'Código\s+d[oe]\s+Processo\s+Civil',
            'Novo\s+Código\s+d[oe]\s+Processo\s+Civil',
            'Constituição',
        ],
        lei_map: Dict[str, str] = {
            'L8429/92': 'LIA',
            'L8249/92': 'LIA', # Common misspelling
            'Lei\s+de\s+Improbidade': 'LIA',
            'Código\s+Penal': 'CP',
            'Código\s+Eleitoral': 'CE',
            'Código\s+d[oe]\s+Processo\s+Penal': 'CPP',
            'Novo\s+Código\s+d[oe]\s+Processo\s+Civil': 'NCPC',
            'Código\s+d[oe]\s+Processo\s+Civil': 'CPC',
            'Constituição': 'CF',
            '^CPB$': 'CP',
        },
        fundamento_regexes: List[str] = [
            r'(?P<artigos>\bart.{{0,5}}[0-9][^.]+?)(?P<lei>{})'
        ],
        # (?<!f) excludes paragrafo 45:
        artigo_regex: str = '(?<!f)[^§]\s+(?P<artigo>[0-9]+(?:-[A-D])?)(?=[º°,\s])',
        paragrafo_regex: str = r'(?i)(?:§|par[aá]grafo)\s*(?P<paragrafo>[0-9]+|[uú]nico)',
        inciso_regex: str = r'\s(?P<inciso>(?:[IXVL]+|caput|CAPUT))\b(?:.{0,3}(?:letra.{0,2}|LETRA.{0,2}|AL[IÍ]NEA.{0,2}|al[íi]nea.{0,2})?\b(?P<alinea>[a-d])\b)?',
        alinea_paragrafo_regex: str = '[""]([a-z])[""]',
        clean_lei: Callable[[pd.Series], pd.Series] = clean_lei,
        clean: Callable[[pd.DataFrame], pd.DataFrame] = _clean_fundamento,
        flags: str = '(?s)(?i)',
) -> pd.DataFrame:
    """Extract structured legal citations (law, article, paragraph, inciso) from text.

    Args:
        text: Series of legal text strings.
        lei_regexes: Regex patterns identifying legal act references.
        lei_map: Mapping from extracted law names to short codes.
        fundamento_regexes: Template regexes for extracting citation blocks.
        artigo_regex: Regex for extracting article numbers.
        paragrafo_regex: Regex for extracting paragraph numbers.
        inciso_regex: Regex for extracting inciso (subsection) numbers.
        alinea_paragrafo_regex: Regex for extracting alinea from paragraph text.
        clean_lei: Function to clean law references.
        clean: Function to clean the resulting DataFrame.
        flags: Regex flags prefix string.

    Returns:
        DataFrame with columns: citation, lei, artigo, paragrafo, inciso, alinea.
    """
    # DOES NOT CAPTURE CORRECTLY:
    # art 405, §§ 1° e 2°, do CPP (captures CPP art 2)
    # inciso VII do art 386 do CPP (captures CPP 386)
    # Art 2º, IV, "a", "b" e "c" (captures only alinea a)
    # art 4° do diploma legal (does not capture)
    lei_regexes = f'\\b(?:{"|".join(lei_regexes)})\\b'
    fund = pd.DataFrame()
    for regex in fundamento_regexes:
        regex = regex.format(lei_regexes)
        regex = f'{flags}(?P<citation>{regex})'
        fund = pd.concat([fund, text.str.extractall(regex)])
    fund = fund.reset_index().drop(columns='match')
    fund.index.name = "fund_id"
    artigo = split_series(
        fund.artigos,
        artigo_regex,
        drop_end=True,
        text_name='paragrafos',
        level_name="artigo_id"
    )
    paragrafo = split_series(
        artigo.paragrafos,
        paragrafo_regex,
        drop_end=False,
        text_name='incisos',
        level_name="paragrafo_id"
    )
    inciso = paragrafo.incisos.str.extractall(inciso_regex)
    paragrafo['alinea_paragrafo'] = get_alinea_paragrafo(
        paragrafo,
        inciso,
        alinea_paragrafo_regex
    )
    df = fund.join(artigo, how='outer').join(paragrafo).join(inciso).reset_index()
    df = _drop_empty_paragrafo(df)
    for c in ['inciso', 'paragrafo']:
        df[c] = clean_text(df[c])
    df = clean(df)
    df = df.set_index('ix')
    df['lei'] = clean_lei(df.lei)
    df['lei'] = map_regex(df.lei, lei_map, flags=re.I)
    cols = ['citation', 'lei', 'artigo', 'paragrafo', 'inciso', 'alinea']
    df = df[cols]
    return df


def get_alinea_paragrafo(
    paragrafo: pd.DataFrame, inciso: pd.DataFrame, regex: str
) -> pd.Series:
    """Extract alinea from paragraphs that have no inciso.

    Args:
        paragrafo: DataFrame with paragraph data including ``incisos`` column.
        inciso: DataFrame with inciso extraction results.
        regex: Regex pattern for extracting alinea.

    Returns:
        Series of alinea values.
    """
    paragrafo['has_inciso'] = inciso.groupby(
        ['fund_id', 'artigo_id', 'paragrafo_id']
    ).inciso.size() > 0
    paragrafo['has_inciso'] = paragrafo.has_inciso.fillna(False)
    paragrafo.loc[
        paragrafo.has_inciso==False,
        'alinea_paragrafo'
    ] = paragrafo.incisos.str.extract(regex)[0]
    paragrafo = paragrafo.drop(columns='has_inciso')
    return paragrafo.alinea_paragrafo


def _drop_empty_paragrafo(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with empty paragraphs when other paragraphs exist for the same article."""
    df['has_paragrafo'] = df.paragrafo.notnull()
    has_paragrafo = df.groupby(['fund_id', 'artigo_id']).has_paragrafo.transform('sum') > 0
    df = df.loc[~(has_paragrafo & (df.paragrafo_id==-1))]
    df = df.drop(columns='has_paragrafo')
    return df


def load_datajud_jsonl(path: str) -> List[Dict[str, Any]]:
    """Load a DataJud JSONL file into a list of dicts.

    Args:
        path: File path to the JSONL file.

    Returns:
        List of parsed JSON records.
    """
    import json
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def normalize_datajud(
    records: List[Dict[str, Any]], classe_codigos: Optional[List[int]] = None
) -> Dict[str, pd.DataFrame]:
    """Normalize DataJud JSONL records into relational DataFrames.

    Args:
        records: List of dicts, each a DataJud ES hit with ``'_id'`` and
            ``'_source'`` keys.
        classe_codigos: Optional list of classe codes to keep; if provided,
            records with other classes are dropped.

    Returns:
        Dict mapping table name to DataFrame:
            - ``'processos'``: one row per case
            - ``'processo_assuntos'``: bridge table (processo_id -> assunto_codigo)
            - ``'assuntos'``: dimension table of unique assuntos
            - ``'classes'``: dimension table of unique classes
            - ``'orgaos_julgadores'``: dimension table of unique orgaos
    """
    proc_rows = []
    bridge_rows = []
    classes_seen = {}
    orgaos_seen = {}
    assuntos_seen = {}

    for rec in records:
        pid = rec["_id"]
        src = rec.get("_source", {})

        # classe (1-to-1)
        classe = src.get("classe", {})
        classe_codigo = classe.get("codigo")

        if classe_codigos is not None and classe_codigo not in classe_codigos:
            continue

        if classe_codigo is not None and classe_codigo not in classes_seen:
            classes_seen[classe_codigo] = classe.get("nome", "")

        # orgaoJulgador (1-to-1)
        oj = src.get("orgaoJulgador", {})
        oj_codigo = oj.get("codigo")
        if oj_codigo is not None and oj_codigo not in orgaos_seen:
            orgaos_seen[oj_codigo] = {
                "orgao_codigo": oj_codigo,
                "orgao_nome": oj.get("nome", ""),
                "orgao_municipio_ibge": oj.get("codigoMunicipioIBGE"),
            }

        # assuntos (1-to-many, may contain nested lists)
        raw_assuntos = src.get("assuntos", [])
        flat_assuntos = []
        for item in raw_assuntos:
            if isinstance(item, list):
                flat_assuntos.extend(item)
            else:
                flat_assuntos.append(item)
        for assunto in flat_assuntos:
            ac = assunto.get("codigo")
            if ac is not None:
                bridge_rows.append({"processo_id": pid, "assunto_codigo": ac})
                if ac not in assuntos_seen:
                    assuntos_seen[ac] = assunto.get("nome", "")

        # flat processo row
        proc_rows.append({
            "processo_id": pid,
            "numero_processo": src.get("numeroProcesso"),
            "tribunal": src.get("tribunal"),
            "grau": src.get("grau"),
            "nivel_sigilo": src.get("nivelSigilo"),
            "data_ajuizamento": src.get("dataAjuizamento"),
            "data_ultima_atualizacao": src.get("dataHoraUltimaAtualizacao"),
            "timestamp": src.get("@timestamp"),
            "classe_codigo": classe_codigo,
            "orgao_codigo": oj_codigo,
        })

    df_processos = pd.DataFrame(proc_rows)
    if not df_processos.empty:
        df_processos["numero_processo"] = clean_number(df_processos["numero_processo"])
    df_bridge = pd.DataFrame(bridge_rows)
    df_classes = pd.DataFrame(
        [{"classe_codigo": k, "classe_nome": v} for k, v in classes_seen.items()]
    )
    df_orgaos = pd.DataFrame(list(orgaos_seen.values()))
    df_assuntos = pd.DataFrame(
        [{"assunto_codigo": k, "assunto_nome": v} for k, v in assuntos_seen.items()]
    )

    if not df_classes.empty:
        df_classes = df_classes.sort_values("classe_codigo").reset_index(drop=True)
    if not df_orgaos.empty:
        df_orgaos = df_orgaos.sort_values("orgao_codigo").reset_index(drop=True)
    if not df_assuntos.empty:
        df_assuntos = df_assuntos.sort_values("assunto_codigo").reset_index(drop=True)

    return {
        "processos": df_processos,
        "processo_assuntos": df_bridge,
        "assuntos": df_assuntos,
        "classes": df_classes,
        "orgaos_julgadores": df_orgaos,
    }
