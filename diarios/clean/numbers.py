"""Case number cleaning and numeric value functions."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from diarios.clean.text import get_data, get_estado_mapping, map_regex, transform, clean_text

__all__ = [
    "clean_number",
    "is_cnj_number",
    "clean_cnj_number",
    "get_number_regex",
    "get_number_regexes",
    "get_verificador_cnj",
    "clean_number_antigo",
    "clean_number_antigo1",
    "is_number_antigo",
    "convert_number_antigo",
    "get_old_format",
    "get_tribunal",
    "extract_info_from_case_numbers",
    "get_filing_year",
    "clean_reais",
    "clean_integer",
    "get_integer_mapping",
    "clean_oab",
    "clean_cpf",
    "extract_number",
    "get_ordinal_number_regex",
    "get_cardinal_number_regex",
]


def clean_number(numbers: pd.Series, types: Optional[List[str]] = None) -> pd.Series:
    """Clean case numbers, extracting digits and standardizing CNJ format.

    Args:
        numbers: Series of raw case number strings.
        types: List of number format types to clean (e.g. ``["CNJ"]``).

    Returns:
        Series of cleaned case number strings.
    """
    if types is None:
        types = ["CNJ"]
    numbers = numbers.str.extract("([0-9].*[0-9])", expand=False).str.replace(" ", "", regex=False)
    if "CNJ" in types:
        numbers = clean_cnj_number(numbers, errors="ignore")
    # if 'antigo' in types:
    #     numbers = clean_number_antigo(numbers, errors='ignore')
    return numbers


def is_cnj_number(numbers: pd.Series) -> pd.Series:
    """Check whether each number matches the CNJ format."""
    regex = get_number_regex("CNJ")
    return numbers.str.match(regex)


def clean_cnj_number(numbers: pd.Series, errors: str = "coerce") -> pd.Series:
    """Standardize case numbers to the CNJ unified format.

    Args:
        numbers: Series of case number strings.
        errors: How to handle non-conforming numbers (``"coerce"`` or ``"ignore"``).

    Returns:
        Series of standardized CNJ number strings.
    """
    cleaned = numbers.fillna("")
    # Remove any dot in ddddddd:
    cleaned = cleaned.str.replace("^[^\d]*(\d{1,5})\.(\d{1,5})-", r"\1\2-", regex=True)
    cleaned = cleaned.str.replace(
        "(\d+)(\-|\.)?(\d{2})\.?"
        "((20|19)\d{2})\.?"
        "(\d)\.?"
        "(\d{2})\.?"
        "(\d{4}).*",
        r"0000000\1-\3.\4.\6.\7.\8",
        regex=True,
    ).str[-25:]
    if errors == "ignore":
        # Not sure if this is needed any longer:
        cleaned.loc[cleaned.isnull()] = numbers
    if errors == "coerce":
        cleaned.loc[~cleaned.str.match("\d{7}-\d{2}.\d{4}.\d.\d{2}.\d{4}")] = pd.NA
    return cleaned


def get_number_regex(tp: str = "CNJ") -> str:
    """Return the regex pattern for a given case number type.

    Args:
        tp: Number type key (e.g. ``"CNJ"``, ``"TJSP"``).

    Returns:
        Regex pattern string with named capture groups.
    """
    regexes = get_number_regexes()
    return regexes[tp]


def get_number_regexes() -> Dict[str, str]:
    """Return dict mapping tribunal/number type to regex patterns."""
    return {
        "CNJ": (
            "(\d+)(\-|\.)?(\d{2})\."
            "(?P<filingyear>(20|19)\d{2})\."
            "(?P<code_j>\d)\.?"
            "(?P<code_tr>\d{2})\."
            "(?P<oooo>\d{4})"
        ),
        "TJAL": ("\d+\." "(?P<filingyear>\d{2})" "\.\d+-\d"),
        "TJAM": ("\d+\." "(?P<filingyear>\d{2})" "\.\d+-\d"),
        "TJCE": ("\d+\." "(?P<filingyear>(199|200|201)\d)" "\.\d{4}\.\d{3}"),
        "TJGO": ("(?P<filingyear>(199|200|201)\d)" "\d{7,10}"),
        "TJMA": ("\d{4,10}" "(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJMA_2": ("\d+-\d{2}\." "(?P<filingyear>(199|200|201)\d)" "\.\d{2}\.\d{4}"),
        "TJMG": ("\d{4}" "(?P<filingyear>\d{2})" "\d{6}\-\d"),
        "TJMS": ("\d+\." "(?P<filingyear>\d{2})" "\.\d{6}-\d"),
        "TJPA": ("\d{9}" "(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJPB": ("\d{3}" "(?P<filingyear>(199|200|201)\d)" "\d{6}-\d"),
        "TJPR": ("\d{1,6}/" "(?P<filingyear>\d{4})"),
        "TJSC": ("\d+\." "(?P<filingyear>\d{2})" "\.\d{6}-\d"),
        "TJSE": ("(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJSP": (
            "\d+\.\d{2}\." "(?P<filingyear>(199|200|201)\d)" "\.\d{6}(-\d)?"
        ),  # 625.01.1996.002168-3
        "TJSP_2": (
            "\d{3}." "(?P<filingyear>\d{2})" "\.\d{6}(-\d)?"
        ),  # 050.06.071816-1
        "TJTO": ("(?P<filingyear>(199|200|201)\d)" "\.\d{4}\.\d{4}(-|–)\d"),
        "TRF1": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}-\d"),
        "TRF2": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}-\d"),
        "TRF4": ("\d{1,4}\." "(?P<filingyear>\d{4})" "\.\d\.?\d{2}\.\d{4}"),
        "TRF4_2": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}"),
        "TRE-PB": ("\d+/" "(?P<filingyear>\d{4})"),
        "TRE-MA": ("\d+-\d{2}/" "(?P<filingyear>\d{2})"),
        "TRE-MT": ("\d+/" "(?P<filingyear>\d{4})"),
    }


def get_verificador_cnj(n: str, remainder: str) -> Optional[str]:
    """Compute the two-digit check code for a CNJ case number.

    Args:
        n: The NNNNNNN portion of the case number.
        remainder: The YYYY.J.TT.FFFF portion.

    Returns:
        Two-digit verification string, or None on error.

    Note:
        Cannot be vectorized since floats are imprecise.
    """
    base = "{}{}00".format(n, remainder)
    try:
        return str(int(98 - (int(base) % 97))).zfill(2)
    except ValueError:
        pass


def clean_number_antigo(
    number: pd.Series, tribunal: pd.Series, errors: str = "coerce"
) -> pd.Series:
    """Clean old-format (pre-CNJ) case numbers.

    Args:
        number: Series of old-format case number strings.
        tribunal: Series of tribunal identifiers.
        errors: Error handling mode (``"coerce"`` or ``"ignore"``).

    Returns:
        Series of cleaned case number strings.
    """
    # TJAL: 035.07.000018-7
    # TJGO: 200302975980
    # TJMG: 0024.09.000095-2
    # TJMA: 61-61.2012.10.0105
    # TJMS: 018.07.001979-4
    # TJPB: 0482001001567-4
    # TJPI: 2004.40.00.001847-7
    # TJPR: 226/2005
    # TJRO: 003.2008.006388-5
    # TJSC: 011.11.008037-9
    # TJSE: 20165200070
    # TJSP: 660.01.2010.002107-1
    # TJTO: 2011.0011.4133-0
    # TRF1: 2003.37.00.007789-9
    # TRF2: 2016.51.01.164775-3
    # TRF4: 2009.70.09.000477
    df = pd.DataFrame({"number": number, "tribunal": tribunal})
    df["number"] = df.number.fillna("")
    df.loc[df.tribunal == "TRF2", "number"] = clean_number_antigo1(
        df.number, errors=errors
    )
    return df.number


def clean_number_antigo1(number: pd.Series, errors: str = "coerce") -> pd.Series:
    """Clean old-format case numbers for TRF2-style patterns."""
    cleaned = number.fillna("").str.replace(
        "[^0-9]*((20|19)\d{2})\.?" "(\d{2})\.?" "(\d{2})\.?" "(\d{6})-?" "(\d)[^0-9]*",
        r"\1.\3.\4.\5-\6",
        regex=True,
    )
    if errors == "coerce":
        cleaned.loc[~cleaned.str.match("\d{4}.\d{2}.\d{2}.\d{6}-\d")] = pd.NA
    return cleaned


def is_number_antigo(number: pd.Series, tribunal: pd.Series) -> pd.Series:
    """Check whether each case number matches an old (pre-CNJ) format.

    Args:
        number: Series of case number strings.
        tribunal: Series of tribunal identifiers.

    Returns:
        Boolean series indicating old-format numbers.
    """
    df = pd.DataFrame({"number": number, "tribunal": tribunal})
    regexes = get_number_regexes()
    df["is_antigo"] = False
    for t, r in regexes.items():
        df.loc[df.tribunal == t.replace("_2", ""), "is_antigo"] = (
            df.is_antigo | df.number.str.match(r)
        )
    return df["is_antigo"]


# Réu-verified regex conversion rules (see the improbidade pipeline harness
# `source/figure/verify_number_antigo.py`). Each rule maps an old-format string
# to the CNJ components (NNNNNNN, AAAA, OOOO) via a pure formula; J and TR come
# from tribunal.csv. Only tribunals whose old sequential maps DETERMINISTICALLY
# to the CNJ number are listed — tribunals that re-sequenced their cases at the
# CNJ migration (TJCE, TJPE, TJPI, TJRR, ...) have no such formula and are
# deliberately absent; forcing a rule for them fails réu verification (~50% or
# worse) and would emit confident-but-wrong NPUs.
#
# TJPA does NOT re-sequence; its old numbers dk-match the CNJ sequential
# directly for ~half the cases, so no formula is added and none is needed.
#
# Exact-match rates against réu-matched (old, CNJ) gold pairs, 2026-08-02:
#   TJRO  94.0%  (8.2k parsed / 8.6k gold);  NNNNNNN = seq6 + trailing digit
#   TJPB  90.8%  (4.6k gold);                NNNNNNN = seq6, OOOO = comarca*10+1
#   TRF4  90.5%  (325 parsed / 408 gold);    OOOO = seção·100+vara, NNNNNNN = seq
# The residual is genuine re-filing / sub-foro reassignment, not rule error
# (TRF4 residual clusters at the 2008-2009 migration boundary and preserves the
# correct OOOO, so even a mismatched NPU lands in the right vara/municipality).
_ANTIGO_REGEX_SPECS: Dict[str, Any] = {
    # 001.2007.000825-5 -> 0008255-14.2007.8.22.0001
    "TJRO": (
        re.compile(r"^(\d{3})\.((?:19|20)\d{2})\.(\d{6})-?(\d)$"),
        lambda m: (m.group(3) + m.group(4), m.group(2), "0" + m.group(1)),
    ),
    # 0011979000259-4 -> 0000259-09.1979.8.15.0011
    "TJPB": (
        re.compile(r"^(\d{3})((?:19|20)\d{2})(\d{6})-?\d$"),
        lambda m: (m.group(3).zfill(7), m.group(2), str(int(m.group(1)) * 10 + 1).zfill(4)),
    ),
    # [4]1999.70.05.003713-2 -> 0003713-05.1999.4.04.7005 (réu-verified 2026-08-14).
    # Old federal format YYYY.SS.VV.NNNNNN-D: OOOO = SS·100+VV (seção+vara),
    # NNNNNNN = sequential preserved. The optional leading "4" is the justiça
    # segment the diário prepends. The tribunal HQ / 2nd-instance stratum
    # (VV == 00, e.g. 1999.71.00.*) RE-SEQUENCED at migration — the old seq has
    # no CNJ relation there — so it is excluded (negative lookahead) rather than
    # converted to a confident-wrong NPU. Same arithmetic as the TRF2 legacy path.
    "TRF4": (
        re.compile(r"^4?((?:19|20)\d{2})\.(\d{2})\.(?!00)(\d{2})\.(\d{6})-?\d?$"),
        lambda m: (m.group(4).zfill(7), m.group(1), m.group(2) + m.group(3)),
    ),
}


def _convert_antigo_regex(number: pd.Series, tribunal: pd.Series) -> pd.Series:
    """Convert old numbers to CNJ via the réu-verified per-tribunal formulas.

    Returns a Series aligned to ``number`` with a CNJ string where a rule for
    that tribunal matched the old format, else ``pd.NA``. The check digit is
    recomputed from scratch (the old trailing digit is not the CNJ verificador).
    """
    out = pd.Series(pd.NA, index=number.index, dtype="object")
    for trib, (rx, fn) in _ANTIGO_REGEX_SPECS.items():
        mask = (tribunal == trib).fillna(False)
        if not mask.any():
            continue
        j = str(transform(pd.Series([trib]), "tribunal", "code_j").iloc[0])
        tr = str(transform(pd.Series([trib]), "tribunal", "code_tr").iloc[0]).zfill(2)
        for idx in number.index[mask.values]:
            s = str(number[idx]).replace(" ", "")
            m = rx.match(s)
            if not m:
                continue
            n, aaaa, oooo = fn(m)
            dd = get_verificador_cnj(n, aaaa + j + tr + oooo)
            if dd is None:
                continue
            out.loc[idx] = f"{n}-{dd}.{aaaa}.{j}.{tr}.{oooo}"
    return out


def convert_number_antigo(
    number: Union[List[str], pd.Series],
    tribunal: pd.Series,
    errors: str = "ignore",
) -> pd.Series:
    """Convert old-format case numbers to CNJ unified numbering.

    Args:
        number: Series or list of old-format case number strings.
        tribunal: Series of tribunal identifiers.
        errors: Error handling mode (``"ignore"`` or ``"coerce"``).

    Returns:
        Series of CNJ-format case number strings.
    """
    # TRF1: 2009.37.00.009224-9 -> 0000628-30.2010.4.01.3700 (nnnnnnn?)
    # TRF2: 2016.51.01.164775-3 -> 0164775-04.2016.4.02.5101
    # TRF4: 2009.72.08.003061 -> (same as TRF2?)
    # TJSP: 660.01.2010.002107-1 -> 0002107-31.2010.8.26.0660
    # TJSC: 011.11.008037-9 -> 0008037-57.2011.8.24.0011
    # TJMS: 018.07.001979-4 -> 0001979-89.2007.8.12.0018
    # TJRO: 001.2007.000825-5 -> 0008255-14.2007.8.22.0001  (regex path)
    # TJPB: 0011979000259-4 -> 0000259-09.1979.8.15.0011    (regex path)
    # TJSE: not transitioned to numeracao unica
    # TJGO: 200803065609 -> 306560-09.2008.8.09.0120 (how to get oooo?)
    if isinstance(number, list):
        number = pd.Series(number)
    if not isinstance(tribunal, pd.Series):
        tribunal = pd.Series(list(tribunal), index=number.index)
    # Réu-verified regex tribunals (TJRO, TJPB, ...) take precedence; the
    # legacy positional-split path below handles TRF2/TJSP/TJMS/TJSC.
    regex_cnj = _convert_antigo_regex(number, tribunal)
    antigo = clean_number_antigo(number, tribunal)
    antigo.loc[is_number_antigo(antigo, tribunal) == False] = pd.NA
    df = antigo.str.split(r"[.\-]", expand=True)
    df.columns = df.columns.map(str)
    # Guarantee the columns the legacy _get_* helpers index into exist, so an
    # all-NA split (an unsupported tribunal) yields NA instead of a KeyError.
    for _c in ("0", "1", "2", "3"):
        if _c not in df.columns:
            df[_c] = pd.NA
    # via Int64 so an unknown tribunal (NaN code, which floats the column) does
    # not render as "8.0"/"24.0" and corrupt the check-digit input.
    df["j"] = transform(tribunal, "tribunal", "code_j").astype("Int64").astype("string")
    df["tr"] = (
        transform(tribunal, "tribunal", "code_tr")
        .astype("Int64").astype("string").str.zfill(2)
    )
    df["tribunal"] = tribunal
    df["aaaa"] = _get_aaaa(df)
    df["oooo"] = _get_oooo(df)
    df["n"] = _get_n(df)
    # Normalize the CNJ components to a single string dtype so concatenation is
    # NA-propagating and does not mix arrow-string with object/null columns
    # (a legacy tribunal absent from this batch leaves aaaa/oooo all-NA).
    for _c in ("n", "aaaa", "j", "tr", "oooo"):
        df[_c] = df[_c].astype("string")
    df["remainder"] = df["aaaa"] + df["j"] + df["tr"] + df["oooo"]
    df["dd"] = df.apply(
        lambda x: get_verificador_cnj(x.n, x.remainder), axis=1
    ).astype("string")
    cnj = (
        df["n"]
        + "-"
        + df["dd"]
        + "."
        + df["aaaa"]
        + "."
        + df["j"]
        + "."
        + df["tr"]
        + "."
        + df["oooo"]
    )
    # Regex-path conversions (TJRO, TJPB, ...) win where present.
    cnj = regex_cnj.combine_first(cnj)
    if errors == "ignore":
        cnj.loc[cnj.isnull()] = number
    return cnj


def _scol(df: pd.DataFrame, mask: pd.Series, col: str) -> pd.Series:
    """Masked split-column as ``string`` dtype (null-safe under arrow strings)."""
    return df.loc[mask, col].astype("string")


def _tjsp2_mask(df: pd.DataFrame) -> pd.Series:
    """TJSP rows written in the shorter 050.06.071816-1 form (year in col 1)."""
    return (df.tribunal == "TJSP") & (df["2"].astype("string").str.len() > 4)


def _get_aaaa(df: pd.DataFrame) -> pd.Series:
    """Extract the four-digit year component from old-format case numbers.

    Each per-tribunal rule is applied on its masked subset only, so an all-NA
    split (an unsupported / regex-path tribunal) leaves ``aaaa`` as NA instead
    of raising on arithmetic over null columns.
    """
    df = df.copy()
    df["aaaa"] = pd.NA
    for m, col in ((df.tribunal == "TRF2", "0"), (df.tribunal == "TJSP", "2")):
        if m.any():
            df.loc[m, "aaaa"] = _scol(df, m, col)
    mask = df.tribunal.isin(["TJMS", "TJSC"]) | _tjsp2_mask(df)
    if mask.any():
        # Two-digit year -> four-digit, pivoting at 30 so pre-2000 cases (YY
        # 31-99) become 19xx rather than a nonsensical 20xx (96 -> 1996).
        yy = _scol(df, mask, "1")
        prefix = pd.Series("20", index=yy.index, dtype="string")
        prefix[pd.to_numeric(yy, errors="coerce") > 30] = "19"
        df.loc[mask, "aaaa"] = prefix.str.cat(yy)
    return df.aaaa


def _get_oooo(df: pd.DataFrame) -> pd.Series:
    """Extract the four-digit origin court code from old-format case numbers."""
    df = df.copy()
    df["oooo"] = pd.NA
    trf2 = df.tribunal == "TRF2"
    if trf2.any():
        df.loc[trf2, "oooo"] = _scol(df, trf2, "1").str.cat(_scol(df, trf2, "2"))
    origin0 = df.tribunal.isin(["TJSP", "TJMS", "TJSC"])
    if origin0.any():
        df.loc[origin0, "oooo"] = "0" + _scol(df, origin0, "0")
    return df.oooo


def _get_n(df: pd.DataFrame) -> pd.Series:
    """Extract the seven-digit sequential number from old-format case numbers."""
    df = df.copy()
    df["n"] = pd.NA
    for m, col in ((df.tribunal == "TRF2", "3"), (df.tribunal == "TJSP", "3")):
        if m.any():
            df.loc[m, "n"] = _scol(df, m, col)
    mask = df.tribunal.isin(["TJMS", "TJSC"]) | _tjsp2_mask(df)
    if mask.any():
        df.loc[mask, "n"] = _scol(df, mask, "2")
    df["n"] = df["n"].fillna("").astype("string").str.zfill(7)
    return df.n


def get_old_format(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Filter DataFrame to rows with non-CNJ (old format) case numbers.

    Args:
        df: DataFrame containing a case number column.
        col: Column name with case numbers.

    Returns:
        Filtered DataFrame with only old-format numbers.
    """
    regex = r"\d{7}\-\d{2}\.[1-2]\d{3}\.\d.\d{2}\.\d{4}"
    df["valid"] = df[col].str.contains(regex, regex=True)
    df = df[df.valid.astype(str).str.contains("False", regex=True)]
    return df


def get_tribunal(
    series: pd.Series, input_type: str = "number", output: str = "tribunal"
) -> pd.Series:
    """Look up tribunal information from case numbers or diario names.

    Args:
        series: Series of case numbers or diario names.
        input_type: ``'number'`` to look up from case numbers, or
            ``'diario'`` to look up from diario names.
        output: Column to return (``'tribunal'`` or ``'tribunal_id'``).

    Returns:
        Series of tribunal values.
    """
    if input_type == "number":
        tribunal = get_data("tribunal.csv").set_index(["code_j", "code_tr"])
        info = extract_info_from_case_numbers(series, types=["CNJ"])
        info = info.join(tribunal, on=["code_j", "code_tr"])
        return info[output]
    if input_type == "diario":
        diario = get_data("diario.csv").set_index("diario")
        return series.to_frame(name="diario").join(diario, on="diario").loc[:, (output)]


def extract_info_from_case_numbers(
    number: pd.Series, types: Optional[List[str]] = None
) -> pd.DataFrame:
    """Extract structured info (year, court codes) from case numbers.

    Args:
        number: Series of case number strings.
        types: List of number format types to extract from.

    Returns:
        DataFrame with columns for each named capture group.
    """
    if types is None:
        types = ["CNJ"]
    regexes = map(get_number_regex, types)
    info = pd.DataFrame(index=number.index)
    for regex in regexes:
        df = number.str.extract(regex)
        new_cols = set(df.columns) - set(info.columns)
        info = info.join(df.loc[:, list(new_cols)])
        if info.isnull().any().any():
            info[info.isnull()] = df
    info = info.apply(pd.to_numeric, errors="coerce")
    return info


def get_filing_year(numbers: pd.Series, types: Optional[List[str]] = None) -> pd.Series:
    """Extract and normalize the filing year from case numbers.

    Args:
        numbers: Series of case number strings.
        types: List of number format types.

    Returns:
        Series of four-digit filing years.
    """
    if types is None:
        types = ["CNJ"]
    filingyear = extract_info_from_case_numbers(numbers, types).loc[:, "filingyear"]
    filingyear.loc[filingyear.between(0, 18)] = filingyear + 2000
    filingyear.loc[filingyear.between(80, 99)] = filingyear + 1900
    return filingyear


def clean_reais(sr: pd.Series) -> pd.Series:
    """Parse Brazilian Real monetary values to numeric.

    Args:
        sr: Series of monetary value strings (e.g. ``"1.234,56"``).

    Returns:
        Numeric series with values in reais (integer part only).
    """
    return pd.to_numeric(
        sr.str.replace(",\d{2}([^0-9].*|$)", "", regex=True).str.replace("[^0-9]", "", regex=True),
        errors="coerce",
    )


def clean_integer(sr: pd.Series) -> pd.Series:
    """Extract and convert integer values, including Portuguese number words.

    Args:
        sr: Series of strings that may contain digits or number words.

    Returns:
        Numeric series of extracted integers.
    """
    mapping = get_integer_mapping()
    regex = list(mapping.keys()) + ["\d+"]
    regex = "({})".format("|".join(regex))
    sr = sr.str.extract(regex, expand=False)
    sr = map_regex(sr, mapping)
    return pd.to_numeric(sr, errors="coerce")


def get_integer_mapping() -> Dict[str, str]:
    """Return mapping from Portuguese number words to digit strings."""
    return {
        "UMA?": "1",
        "DOIS": "2",
        "DUAS": "2",
        "DUPLO": "2",
        "TRES": "3",
        "TRIPLO": "3",
        "QUATRO": "4",
        "CINCO": "5",
        "SEIS": "6",
        "SETE": "7",
        "OITO": "8",
        "NOVE": "9",
        "DEZ": "10",
        "VINTE": "20",
        "TRINTA": "30",
        "QUARENTA": "40",
        "CINQUENTA": "50",
        "SESSENTA": "60",
        "SETENTA": "70",
        "OITENTA": "80",
        "NOVENTA": "90",
        "CEM": "100",
    }


def clean_oab(sr: Union[str, pd.Series]) -> pd.Series:
    """Clean OAB (Brazilian bar association) registration numbers.

    Args:
        sr: OAB number string or Series of strings.

    Returns:
        Series of cleaned OAB numbers in ``"N/UF"`` format.
    """
    if isinstance(sr, str):
        sr = pd.Series([sr])
    n = (
        pd.to_numeric(sr.str.replace("[^0-9]", "", regex=True), errors="coerce")
        .astype(str)
        .str.replace("\.0", "", regex=True)
    )
    states = "|".join(get_estado_mapping().values())
    state = sr.str.extract("({})".format(states), expand=False)
    ab = sr.str.extract("\d(a|b|A|B)", expand=False).str.upper().fillna("")
    cleaned = n + ab + "/" + state
    return cleaned


def clean_cpf(cpf: pd.Series, as_str: bool = False) -> pd.Series:
    """Clean CPF (Brazilian individual taxpayer ID) numbers.

    Returns nullable ``Int64`` by default — the workspace standard for
    CPF storage (see ``research/rules/data_conventions.md``). Non-numeric
    values, and the politica raw-data sentinels ``-1`` / ``-4`` / ``0``,
    are normalized to ``pd.NA``.

    Args:
        cpf: Series of CPF strings or numbers.
        as_str: If True, return zero-padded 11-character strings (with
            ``pd.NA`` for missing). Rarely needed — the canonical join
            type is ``Int64``.

    Returns:
        ``Int64`` series (or ``string`` series if ``as_str=True``).
    """
    numeric = pd.to_numeric(cpf, errors="coerce")
    # Treat politica's <=0 sentinels as missing. Real CPFs are strictly positive.
    numeric = numeric.where(numeric > 0)
    int_series = numeric.astype("Int64")
    if as_str:
        return int_series.astype("string").str.zfill(11)
    return int_series


def extract_number(
    sr: pd.Series,
    cardinal: bool = True,
    ordinal: bool = True,
    numeric: bool = True,
    decimal_sep: str = ",",
) -> pd.Series:
    """Extract numeric values from text, supporting digits and Portuguese number words.

    Args:
        sr: Series of text strings.
        cardinal: If True, recognize Portuguese cardinal number words.
        ordinal: If True, recognize Portuguese ordinal number words.
        numeric: If True, recognize digit sequences.
        decimal_sep: Decimal separator character (``","`` for Brazilian format).

    Returns:
        Numeric series of extracted values.
    """
    if decimal_sep == ",":
        # 250.100.00 -> 250.100,00 (fixing OCR error)
        sr = sr.str.replace('\.([0-9]{2})$', r',\1', regex=True)
        sr = sr.str.replace('\.([0-9]{4,5})$', r',\1', regex=True)
    sr = clean_text(sr, drop=f"^A-Za-z0-9{decimal_sep} ", upper=True)
    # Does not extract zero for now
    mapping = {}
    if cardinal:
        mapping = _get_cardinal_numbers()
    if ordinal:
        mapping = {**mapping, **_get_ordinal_numbers()}
    mapping = {
        r'\b{}\b'.format(k): v
        for k, v in mapping.items()
    }
    ones = {k: v for k, v in mapping.items() if v % 10 != 0}
    tens = {k: v for k, v in mapping.items() if v % 10 == 0 and v % 100 != 0}
    hundreds = {k: v for k, v in mapping.items() if v % 100 == 0}
    number = pd.Series(index=sr.index, dtype=float)
    if numeric:
        regex = f'([0-9]+({decimal_sep}[0-9]+)?)'
        number = (
            sr
            .str.extract(regex)[0]
            .str.replace(decimal_sep, ".", regex=True)
        )
        too_large = number.str.len() > 20
        if sum(too_large) > 0:
            print("Truncating too large numbers:")
            print(number.loc[too_large])
            number = number.str[0:21]
        number = pd.to_numeric(number)
    if len(mapping) > 0:
        # map_regex returns an object Series; coerce each element to a
        # plain numeric scalar (anything else -> NaN) before summing, so
        # the fill is dtype-safe under pandas 2.x strict assignment.
        def _scalar_num(s: pd.Series) -> pd.Series:
            return pd.to_numeric(
                s.map(lambda x: x if isinstance(x, (int, float))
                      and not isinstance(x, bool) else None),
                errors="coerce",
            )
        word_sum = (
            _scalar_num(map_regex(sr, hundreds, keep_unmatched=False)).fillna(0)
            + _scalar_num(map_regex(sr, tens, keep_unmatched=False)).fillna(0)
            + _scalar_num(map_regex(sr, ones, keep_unmatched=False)).fillna(0)
        )
        number = pd.to_numeric(number, errors="coerce").fillna(word_sum)
    number = pd.to_numeric(number, errors="coerce")
    return number.mask(number == 0)

def get_ordinal_number_regex(flags: str = '(?i)(?s)') -> str:
    """Return regex pattern matching Portuguese ordinal numbers.

    Args:
        flags: Regex flags prefix string.

    Returns:
        Regex pattern string.
    """
    tens = [
        'D[EÉ]CIM',
        'VIG[EÉ]SIM',
        'TRIG[EÉ]SIM',
        'QUADRAG[EÉ]SIM',
    ]
    ones = [
        'PRIMEIR',
        'SEGUND',
        'TERCEIR',
        'QUART',
        'QUINT',
        'SEXT',
        'SETIM',
        'OITAV',
        'NON',
        'D[EÉ]CIM',
    ]
    regex = r'{}\b(({})[AO]\s+)?({})[AOªº]\b'.format(
        flags,
        '|'.join(tens),
        '|'.join(ones)
    )
    return regex


def get_cardinal_number_regex(flags: str = '(?i)(?s)') -> str:
    """Return regex pattern matching Portuguese cardinal numbers and digits.

    Args:
        flags: Regex flags prefix string.

    Returns:
        Regex pattern string.
    """
    numbers = _get_cardinal_numbers().keys()
    regex = r'{}([0-9][0-9.,]*|\b(?:{})\b)'.format(
        flags,
        '|'.join(numbers)
    )
    return regex


def _get_ordinal_numbers() -> Dict[str, int]:
    """Return mapping from Portuguese ordinal number patterns to integers."""
    return {
        'PRIMEIR[AO]': 1,
        'SEGUND[AO]': 2,
        'TERCEIR[AO]': 3,
        'QUART[AO]': 4,
        'QUINT[AO]': 5,
        'SEXT[AO]': 6,
        'SETIM[AO]': 7,
        'OITAV[AO]': 8,
        'NON[AO]': 9,
        'DECIM[AO]': 10,
        'VIGESIM[AO]': 20,
        'TRIGESIM[AO]': 30,
        'QUADRAGESIM[AO]': 40,
    }


def _get_cardinal_numbers() -> Dict[str, int]:
    """Return mapping from Portuguese cardinal number patterns to integers."""
    return {
        'UMA?': 1,
        'DOIS': 2,
        'DUAS': 2,
        'TRES': 3,
        'QUATRO': 4,
        'CINCO': 5,
        'SEIS': 6,
        'SETE': 7,
        'OITO': 8,
        'NOVE': 9,
        'DEZ': 10,
        'ONZE': 11,
        'DOZE': 12,
        'TREZE': 13,
        'CATORZE': 14,
        'QUINZE': 15,
        'DEZ[AE]SSEIS': 16,
        'DEZ[AE]SSETE': 17,
        'DEZOITO': 18,
        'DEZ[AE]NOVE': 19,
        'VINTE': 20,
        'TRINTA': 30,
        'QUARENTA': 40,
        'CINQUENTA': 50,
        'SESSENTA': 60,
        'SETENTA': 70,
        'OITENTA': 80,
        'NOVENTA': 90,
        'CEM': 100,
        'CENTO': 100,
        'DUZENT[OA]S': 200,
        'TREZENT[OA]S': 300,
        'QUATROCENT[OA]S': 400,
        'QUINHENT[OA]S': 500,
        'SEISCENT[OA]S': 600,
        'SETECENT[OA]S': 700,
        'OITOCENT[OA]S': 800,
        'NOVECENT[OA]S': 900,
        'MIL': 1000,
    }
