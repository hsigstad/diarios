"""Core text cleaning and data utility functions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np
import glob
from unidecode import unidecode
import os
import re
from copy import copy
import warnings
warnings.filterwarnings("ignore", "This pattern has match groups")

__all__ = [
    "clean_text",
    "remove_links",
    "clean_text_columns",
    "clean_diario_text",
    "get_data",
    "get_data_file",
    "get_estado_mapping",
    "map_regex",
    "remove_regexes",
    "extract_series",
    "extractall_series",
    "split_series",
    "title",
    "transform",
    "generate_id",
    "move_columns_first",
    "read_csv",
    "extract_from_list",
    "add_leads_and_lags",
]


def clean_text(
    text: Union[str, pd.Series],
    drop: Optional[str] = "^A-Za-z0-9 ",
    replace_character: str = "",
    lower: bool = False,
    upper: bool = True,
    accents: bool = False,
    links: bool = False,
    newline: bool = False,
    pagebreak: bool = False,
    cr: bool = False,
    multiple_spaces: bool = False,
    strip: bool = True,
) -> Union[str, pd.Series]:
    """Clean text by removing accents, special characters, and normalizing whitespace.

    Boolean parameters with default False mean the feature is *removed* by default.
    Set to True to *preserve* that feature.

    Args:
        text: Input text string or Series.
        drop: Character class to remove (regex bracket content), or None to keep all.
        replace_character: Replacement for dropped characters.
        lower: If True, convert to lowercase.
        upper: If True, convert to uppercase.
        accents: If True, preserve accented characters.
        links: If True, preserve links.
        newline: If True, preserve newlines.
        pagebreak: If True, preserve page break markers.
        cr: If True, preserve carriage returns.
        multiple_spaces: If True, preserve multiple consecutive spaces.
        strip: If True, strip leading/trailing whitespace.

    Returns:
        Cleaned text string or Series.
    """
    is_string = isinstance(text, str)
    if is_string:
        text = pd.Series([text])
    text = text.fillna("").astype(str)
    if not links:
        text = remove_links(text)
    if not cr:
        text = text.str.replace(r"\r", "\n", regex=True)
    if not newline:
        text = text.str.replace("\n", " ", regex=True)
    if not pagebreak:
        text = text.str.replace("==>.*?<==", "", regex=True)
    if not accents:
        text = text.apply(unidecode)
    if lower:
        text = text.str.lower()
    if upper:
        text = text.str.upper()
    if drop:
        text = text.str.replace("[{}]".format(drop), replace_character, regex=True)
    if not multiple_spaces:
        text = text.str.replace("  +", " ", regex=True)
    if strip:
        text = text.str.strip()
    if is_string:
        return text[0]
    else:
        return text


def remove_links(text: pd.Series) -> pd.Series:
    """Remove markdown-style links from text, keeping the link label."""
    return text.str.replace(r"\[(.*?)\]", r"\1", regex=True).str.replace(r"(?s)\(http.*?\)", r"", regex=True)


def clean_text_columns(
    df: pd.DataFrame, exclude: List[str] = [], drop: str = "^A-Z0-9 ", **kwargs: Any
) -> pd.DataFrame:
    """Apply ``clean_text`` to all object-type columns in a DataFrame.

    Args:
        df: Input DataFrame.
        exclude: Column names to skip.
        drop: Character class to remove.
        **kwargs: Additional keyword arguments passed to ``clean_text``.

    Returns:
        DataFrame with cleaned text columns.
    """
    for col in df.select_dtypes(include="object").columns:
        if col not in exclude:
            df[col] = clean_text(df[col], drop=drop, **kwargs)
    return df


def clean_diario_text(text: pd.Series) -> pd.Series:
    """Clean diario text preserving case and special characters."""
    return clean_text(
        text,
        upper=False,
        lower=False,
        drop=None,
        accents=True,
        links=False,
        newline=True,
    )


def get_data(datafile: str) -> pd.DataFrame:
    """Read a CSV from the package data directory.

    Args:
        datafile: Filename within the ``data/`` directory.

    Returns:
        DataFrame loaded from the CSV file.
    """
    infile = get_data_file(datafile)
    return pd.read_csv(infile)


def get_data_file(datafile: str) -> str:
    """Return the absolute path to a file in the package data directory."""
    pkg_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(pkg_dir, "data", datafile)


def get_estado_mapping() -> Dict[str, str]:
    """Return mapping from full state names (uppercase, no accents) to abbreviations."""
    return {
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAPA": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARA": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPIRITO SANTO": "ES",
        "GOIAS": "GO",
        "MARANHAO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARA": "PA",
        "PARAIBA": "PB",
        "PARANA": "PR",
        "PERNAMBUCO": "PE",
        "PIAUI": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDONIA": "RO",
        "RORAIMA": "RR",
        "SAO PAULO": "SP",
        "SANTA CATARINA": "SC",
        "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }


def map_regex(
    series: Union[str, pd.Series, np.ndarray],
    mapping: Dict[str, Any],
    keep_unmatched: bool = True,
    flags: int = 0,
) -> Union[str, pd.Series]:
    """Map values using regex pattern matching.

    Args:
        series: String, numpy array, or pandas Series to map.
        mapping: Dict with regex patterns as keys and replacement values.
        keep_unmatched: If True, keep original value when no pattern matches.
        flags: ``re`` module flags for pattern matching.

    Returns:
        Series or string with values of first matching regex in dict.
    """
    if series is np.NaN:
        return np.NaN
    if isinstance(series, str):
        for key, val in mapping.items():
            if re.search(key, series):
                return val
        if keep_unmatched:
            return series
        else:
            return np.NaN
    if isinstance(series, np.ndarray):
        series = pd.Series(series)
    if sum(series.isnull()) == len(series):
        print("Empty Series")
        return series
    ix = series.index
    series = series.reset_index(drop=True)
    mapped = pd.Series(index=series.index, dtype=object)
    for key, val in mapping.items():
        mapped.loc[series.str.contains(key, flags=flags, regex=True) & mapped.isnull()] = val
    if keep_unmatched:
        mapped.loc[mapped.isnull()] = series
    mapped.index = ix
    return mapped


def remove_regexes(texts: pd.Series, regex_list: List[str], flags: str = "(?s)") -> pd.Series:
    """Remove all patterns in regex_list from texts.

    Args:
        texts: Series of strings.
        regex_list: List of regex patterns to remove.
        flags: Regex flags prefix string.

    Returns:
        Series with matched patterns removed.
    """
    for regex in regex_list:
        regex = r"{}{}".format(flags, regex)
        texts = texts.str.replace(regex, "", regex=True)
    return texts


def extract_series(text: pd.Series, regex: Union[str, pd.Series]) -> pd.DataFrame:
    """Extract named capture groups from text using per-row regexes.

    Args:
        text: Series of text strings.
        regex: Single regex or Series of regexes (one per row).

    Returns:
        DataFrame of named capture groups for regex matches.
    """
    df = pd.DataFrame({"text": text, "regex": regex})
    return df.apply(
        lambda row: _search_row(row.regex, row.text), axis=1, result_type="expand"
    )


def _search_row(regex: str, text: str) -> Dict[str, str]:
    """Apply a single regex search and return the group dict."""
    try:
        return re.search(regex, text).groupdict()
    except AttributeError:
        return dict()
    except TypeError:
        return dict()


def extractall_series(
    text: pd.Series, regex: Union[str, pd.Series], level_name: str = 'match'
) -> pd.DataFrame:
    """Extract all matches of named capture groups using per-row regexes.

    Args:
        text: Series of text strings.
        regex: Single regex or Series of regexes (one per row).
        level_name: Name for the match-level index.

    Returns:
        DataFrame of named capture groups for all regex matches.
    """
    df = pd.DataFrame({"text": text, "regex": regex}, index=text.index)
    out = df.apply(lambda row: _searchall_row(row.regex, row.text), axis=1)
    out = out.apply(lambda x: pd.Series(x, dtype=object)).stack()
    out = out.apply(lambda x: pd.Series(x, dtype=object))
    out.index = out.index.set_names(level_name, level=-1)
    return out


def _searchall_row(regex: str, text: str) -> List[Dict[str, str]]:
    """Apply finditer and return list of group dicts."""
    try:
        return [a.groupdict() for a in re.finditer(regex, text)]
    except AttributeError:
        return []
    except TypeError:
        return []


def split_series(text: pd.Series, regex: str,
                 text_pos: str = "right",
                 drop_end: bool = False,
                 level_name: str = "group",
                 text_name: Optional[str] = None) -> pd.DataFrame:
    """Split text on regex, preserving named capture groups.

    Args:
        text: Series of text strings.
        regex: Regex to split on with named capture group(s).
        text_pos: Keep text to the ``'left'`` or ``'right'`` of split.
        drop_end: Whether to drop trailing empty segments.
        level_name: Name for the new index level.
        text_name: Column name for the text portion; defaults to
            the Series name.

    Returns:
        DataFrame with columns for capture groups and the text to the
        left or right of the split. Splits on first regex match.
    """
    if isinstance(regex, pd.Series):
        raise TypeError("Not implemented for series of regexes")
        text_df = pd.DataFrame({
            'text': text,
            'regex': regex
        })
        df = text_df.apply(lambda row: re.split(row.regex, row.text), 1)
        # Not finished
    df = text.str.split(regex, expand=True).stack()
    df.index = df.index.set_names("match", level=-1)
    df = df.reset_index(level="match")
    regex = re.compile(regex)
    group = df.match % (regex.groups + 1)
    group_names = {v: k for k, v in regex.groupindex.items()}
    df["variable"] = group.replace(group_names).replace({0: "text"})
    shift = {"left": 0, "right": 1}
    df[level_name] = (df.match - shift[text_pos]) // (regex.groups + 1)
    df = df.set_index(["variable", level_name], append=True)
    out = df.unstack("variable")[0]
    if drop_end:
        end = out.isnull().sum(axis=1) == len(out.columns) - 1
        out = out.loc[~end]
    out.columns.name = None
    if not text_name:
        text_name = text.name
    out = out.rename(columns={'text': text_name})
    return out


def title(sr: pd.Series) -> pd.Series:
    """Convert series to title case, lowering Portuguese prepositions."""
    sr = sr.str.title()
    tolower = {"De": "de", "Da": "da", "Do": "do", "Das": "das", "Dos": "dos", "E": "e"}
    for key, val in tolower.items():
        sr = sr.str.replace(r"\b{}\b".format(key), val, regex=True)
    return sr


def transform(
    x: Union[Any, pd.Series, pd.DataFrame, list],
    from_var: Union[str, List[str]],
    to_var: str,
    keep_unmatched: bool = False,
    infile: Optional[str] = None,
    dropna: bool = True,
) -> Union[Any, pd.Series]:
    """Look up values from a reference CSV by mapping ``from_var`` to ``to_var``.

    Args:
        x: Input value(s) to transform (scalar, list, Series, or DataFrame).
        from_var: Column name(s) in the lookup table to join on.
        to_var: Column name in the lookup table to return.
        keep_unmatched: If True, keep original values when no match found.
        infile: Path to CSV file; if None, inferred from ``from_var``.
        dropna: If True, drop rows with NaN in the join key.

    Returns:
        Transformed value(s).
    """
    from_var = _transform_clean_from_var(from_var)
    x = _transform_clean_x(x, from_var)
    df = _transform_get_df(infile, from_var, dropna)
    if type(x) not in [pd.Series, pd.DataFrame]:
        return df.loc[x, to_var]
    df = x.join(df, on=from_var, how="left")
    if keep_unmatched:
        if isinstance(x, pd.DataFrame) and len(x.columns) > 1:
            raise ValueError("keep_unmatched not supported for dataframes")
        df[to_var] = df[to_var].fillna(df[from_var])
    return df[to_var]


def _transform_clean_x(
    x: Union[Any, list, pd.Series, pd.DataFrame], from_var: Union[str, List[str]]
) -> Union[Any, pd.DataFrame]:
    """Coerce input to DataFrame for transform lookup."""
    if isinstance(x, list):
        x = pd.Series(x)
    if isinstance(x, pd.DataFrame):
        x = x.copy()
        x.columns = from_var
    if isinstance(x, pd.Series):
        x = x.copy()
        x = x.to_frame(name=from_var)
    return x


def _transform_clean_from_var(from_var: Union[str, List[str]]) -> Union[str, List[str]]:
    """Unwrap single-element list to plain string."""
    if isinstance(from_var, list) and len(from_var) == 1:
        from_var = from_var[0]
    return from_var


def _transform_dropna(df: pd.DataFrame, from_var: Union[str, List[str]]) -> pd.DataFrame:
    """Drop rows where the join key column(s) are null."""
    if isinstance(from_var, list):
        for v in from_var:
            df = df[df[v].notnull()]
    else:
        df = df[df[from_var].notnull()]
    return df


def _transform_get_df(
    infile: Optional[str], from_var: Union[str, List[str]], dropna: bool
) -> pd.DataFrame:
    """Load and prepare the lookup DataFrame for transform."""
    if infile is not None:
        df = pd.read_csv(infile)
    else:
        infile = "{}.csv".format(from_var.replace("_id", ""))
        df = get_data(infile)
    if dropna:
        df = _transform_dropna(df, from_var)
    if len(df) > len(df.drop_duplicates(from_var)):
        raise ValueError("Observations not unique by {} in {}".format(from_var, infile))
    df = df.set_index(from_var)
    return df


def generate_id(
    df: Union[pd.Series, pd.DataFrame],
    by: Optional[Union[str, List[str]]] = None,
    suffix: Optional[int] = None,
    suffix_length: int = 2,
) -> pd.Series:
    """Generate integer IDs from categorical values.

    Args:
        df: Series or DataFrame to generate IDs from.
        by: Column name(s) to use when ``df`` is a DataFrame.
        suffix: Optional max two-digit number to append to each ID.
        suffix_length: Number of digits reserved for the suffix.

    Returns:
        Series of integer IDs.
    """
    if isinstance(df, pd.DataFrame):
        df = df.loc[:, by].astype(str)
        if isinstance(by, list) and len(by) > 1:
            df = df.apply(lambda x: "_".join(x), axis=1)
    ids = (df.astype("category").cat.codes) + 1
    if suffix:
        ids = ids.apply(lambda x: x * 10 ** suffix_length + suffix)
    return ids


def move_columns_first(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Reorder DataFrame columns, moving specified columns to the front.

    Args:
        df: Input DataFrame.
        cols: Column names to move to the beginning.

    Returns:
        DataFrame with reordered columns.
    """
    for col in list(reversed(cols)):
        if col in df.columns:
            c = list(df)
            c.insert(0, c.pop(c.index(col)))
            df = df.loc[:, c]
    return df


def read_csv(regex: str) -> pd.DataFrame:
    """Read and concatenate all CSV files matching a glob pattern.

    Args:
        regex: Glob pattern for CSV file paths.

    Returns:
        Concatenated DataFrame from all matching files.
    """
    infiles = glob.glob(regex)
    return pd.concat(map(pd.read_csv, infiles), sort=True)


def extract_from_list(series: pd.Series, regex_list: List[str]) -> pd.Series:
    """Extract first matching regex from a prioritized list.

    Args:
        series: Series of strings to search.
        regex_list: List of regex patterns, tried in order.

    Returns:
        Series with the first match found for each row.
    """
    extracted = pd.Series(index=series.index)
    for regex in regex_list:
        regex = "({})".format(regex)
        extracted.loc[extracted.isnull()] = series.str.extract(regex)[0]
    return extracted


def add_leads_and_lags(
    df: pd.DataFrame,
    variables: List[str],
    ivar: str,
    tvar: str,
    leads_and_lags: List[int],
) -> pd.DataFrame:
    """Add lead and lag columns for specified variables to a panel DataFrame.

    Args:
        df: Panel DataFrame.
        variables: Column names to create leads/lags for.
        ivar: Individual/entity identifier column name.
        tvar: Time variable column name.
        leads_and_lags: List of lead/lag offsets (positive = lag, negative = lead).

    Returns:
        DataFrame with additional lead/lag columns.
    """
    for l in leads_and_lags:
        df2 = df.copy().loc[:, variables + [ivar, tvar]].drop_duplicates()
        df2[tvar] -= l
        df = pd.merge(df, df2, on=[ivar, tvar], suffixes=["", l], how="left")
    return df
