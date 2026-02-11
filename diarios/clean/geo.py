"""Geographic data functions and court classes."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np
import re
from copy import copy

from diarios.clean.text import clean_text, get_data, get_estado_mapping, title, transform
from diarios.clean.numbers import extract_info_from_case_numbers


class TRT:
    """Represents a Brazilian Regional Labor Court (TRT)."""

    def __init__(self, n: Union[int, str]) -> None:
        """Initialize TRT with its number.

        Args:
            n: TRT number or string like ``'TRT5'``.
        """
        if isinstance(n, str):
            if re.match("TRT", n):
                n = int(n[3])
        self.n = n
        self.name = "TRT{}".format(n)
        self.estados = get_trt_estados(self.name)


class TRF:
    """Represents a Brazilian Regional Federal Court (TRF)."""

    def __init__(self, n: Union[int, str]) -> None:
        """Initialize TRF with its number.

        Args:
            n: TRF number or string like ``'TRF1'``.
        """
        if isinstance(n, str):
            if re.match("TRF", n):
                n = int(n[3])
        self.n = n
        self.name = "TRF{}".format(n)
        self.estados = get_trf_estados(self.name)


def get_trt_estados(trt: str) -> List[str]:
    """Return list of state abbreviations for a given TRT.

    Args:
        trt: TRT name, e.g. ``"TRT1"``.

    Returns:
        List of two-letter state codes.
    """
    df = get_data("trt_estado.csv")
    return df.loc[df.trt == trt, "estado"].tolist()


def get_trf_estados(trf: str) -> List[str]:
    """Return list of state abbreviations for a given TRF.

    Args:
        trf: TRF name, e.g. ``"TRF1"``.

    Returns:
        List of two-letter state codes.
    """
    mapping = get_trf_estados_mapping()
    return mapping[trf]


def get_trf_estados_mapping() -> Dict[str, List[str]]:
    """Return mapping from TRF name to list of state abbreviations."""
    return {
        "TRF1": [
            "AC",
            "AM",
            "AP",
            "BA",
            "DF",
            "GO",
            "MA",
            "MG",
            "MT",
            "PA",
            "PI",
            "RO",
            "RR",
            "TO",
        ],
        "TRF2": [
            "ES",
            "RJ",
        ],
        "TRF3": [
            "MS",
            "SP",
        ],
        "TRF4": [
            "PR",
            "RS",
            "SC",
        ],
        "TRF5": [
            "AL",
            "CE",
            "PB",
            "PE",
            "RN",
            "SE",
        ],
    }


def clean_estado(estado: Union[str, pd.Series]) -> pd.Series:
    """Clean and map state names to two-letter abbreviations.

    Args:
        estado: State names (full or abbreviated).

    Returns:
        Series of two-letter state codes.
    """
    estado = clean_text(estado)
    mapping = get_estado_mapping()
    ufs = mapping.values()
    uf_mapping = dict(zip(ufs, ufs))
    mapping = {**mapping, **uf_mapping}
    return estado.map(mapping)


def get_capital(estado: Union[str, pd.Series]) -> Union[str, pd.Series]:
    """Return the capital city name for a given state abbreviation.

    Args:
        estado: Two-letter state code or Series of codes.

    Returns:
        Capital city name(s) in uppercase without accents.
    """
    mapping = {
        "AC": "RIO BRANCO",
        "AL": "MACEIO",
        "AP": "MACAPA",
        "AM": "MANAUS",
        "BA": "SALVADOR",
        "CE": "FORTALEZA",
        "ES": "VITORIA",
        "GO": "GOIANIA",
        "MA": "SAO LUIS",
        "MT": "CUIABA",
        "MS": "CAMPO GRANDE",
        "MG": "BELO HORIZONTE",
        "PA": "BELEM",
        "PB": "JOAO PESSOA",
        "PR": "CURITIBA",
        "PE": "RECIFE",
        "PI": "TERESINA",
        "RJ": "RIO DE JANEIRO",
        "RN": "NATAL",
        "RS": "PORTO ALEGRE",
        "RO": "PORTO VELHO",
        "RR": "BOA VISTA",
        "SC": "FLORIANOPOLIS",
        "SP": "SAO PAULO",
        "SE": "ARACAJU",
        "TO": "PALMAS",
        "DF": "BRASILIA",
    }
    if isinstance(estado, str):
        return mapping[estado]
    else:
        return estado.map(mapping)


def extract_municipio(
    text: Union[str, pd.Series],
    estado: Union[str, pd.Series],
    add: Optional[np.ndarray] = None,
) -> Union[str, pd.Series]:
    """Extract municipality name from text using state-specific regex.

    Args:
        text: Text to search for municipality names.
        estado: State abbreviation(s) to narrow the search.
        add: Optional additional municipality entries to include.

    Returns:
        Extracted and cleaned municipality name(s).
    """
    regex = get_municipio_regex(estado, add=add)
    if isinstance(text, str):
        try:
            municipio = re.search(regex, text).group(1)
        except (AttributeError, TypeError):
            municipio = ''
    else:
        municipio = text.str.extract(regex, expand=False)
    municipio = clean_municipio(municipio, estado)
    return municipio


def clean_municipio(
    municipio: Union[str, pd.Series], estado: Union[str, pd.Series]
) -> Union[str, pd.Series]:
    """Clean and correct municipality names using known corrections.

    Args:
        municipio: Municipality name(s) to clean.
        estado: Corresponding state abbreviation(s).

    Returns:
        Corrected municipality name(s).
    """
    if isinstance(municipio, str):
        correct = _clean_municipio_series(pd.Series([municipio]), estado)
        return correct[0]
    else:
        correct = _clean_municipio_series(municipio, estado)
        return correct


def _clean_municipio_series(municipio: pd.Series, estado: Union[str, pd.Series]) -> pd.Series:
    """Apply municipality corrections from CSV lookup tables."""
    municipio = clean_text(municipio, drop="^A-Z\- ")
    df = pd.DataFrame({"wrong": municipio, "estado": estado, "index": municipio.index})
    corr1 = get_data("municipio_correction_tse.csv")
    corr2 = get_data("municipio_correction_manual.csv")
    corr = pd.concat([corr1, corr2], sort=True).drop_duplicates()
    df = pd.merge(df, corr, on=["wrong", "estado"], validate="m:1", how="left")
    df.loc[df.correct.isnull(), "correct"] = df.wrong
    df.index = df["index"]
    return df.correct


def get_municipio_id(municipio: pd.Series, estado: pd.Series) -> pd.Series:
    """Return municipality IDs by joining on municipality name and state.

    Args:
        municipio: Municipality names.
        estado: State abbreviations.

    Returns:
        Series of municipality IDs.
    """
    df = pd.DataFrame(
        {"municipio": municipio, "estado": estado, "index": municipio.index}
    )
    ids = get_data("municipio_id.csv").dropna()
    ids = pd.merge(df, ids, on=["municipio", "estado"], validate="m:1", how="left")
    ids.index = ids["index"]
    return ids.municipio_id


def get_municipio_regex(
    estados: Optional[Union[str, List[str]]] = None,
    add: Optional[np.ndarray] = None,
) -> str:
    """Build a regex matching municipality names for specified states.

    Args:
        estados: State abbreviation(s) to include, or None for all.
        add: Optional array of additional ``[estado, municipio]`` entries.

    Returns:
        Regex pattern with word boundaries matching municipality names.
    """
    mun = get_data("municipio.csv")
    corr1 = get_data("municipio_correction_tse.csv")
    corr2 = get_data("municipio_correction_manual.csv")
    ar = np.concatenate(
        [
            mun.loc[:, ("estado", "municipio")].values,
            mun.loc[:, ("estado", "municipio_accents")].values,
            corr1.loc[:, ("estado", "wrong")].values,
            corr2.loc[:, ("estado", "wrong")].values,
        ]
    )
    if add is not None:
        ar = np.concatenate([ar, add])
    df = (
        pd.DataFrame(ar, columns=["estado", "municipio"])
        .drop_duplicates()
        .query("estado.notnull()")
    )
    df["municipio"] = title(df.municipio)
    df2 = copy(df)
    df2["municipio"] = df2.municipio.str.upper()
    df = pd.concat([df, df2])
    df3 = copy(df)
    df3["municipio"] = df3.municipio.str.replace("'", "´", regex=False)
    df = pd.concat([df, df3]).drop_duplicates()
    if estados:
        if not isinstance(estados, list):
            estados = [estados]
        df = df.loc[df.estado.isin(estados)]
    regex = r"\b({})\b".format("|".join(df.municipio.values))
    regex = regex.replace(" ", r"\s+")
    return regex


def clean_comarca(comarca: pd.Series) -> pd.Series:
    """Clean comarca names by removing the 'comarca de' prefix."""
    comarca = clean_text(comarca)
    comarca = comarca.str.replace("comarca de", "", regex=False).str.strip()
    return comarca


def clean_vara(vara: pd.Series) -> pd.Series:
    """Clean vara (court division) text, keeping only alphanumeric and spaces."""
    vara = clean_text(vara, drop="^a-z0-9 ")
    return vara


def get_foro_id(numbers: pd.Series) -> pd.Series:
    """Return foro (court branch) IDs extracted from case numbers."""
    return get_foro_info(numbers).loc[:, "id"]

def get_foro(numbers: pd.Series) -> pd.Series:
    """Return foro (court branch) names extracted from case numbers."""
    return get_foro_info(numbers).loc[:, "foro"]


def get_comarca_id(
    number: Optional[pd.Series] = None,
    comarca: Optional[pd.Series] = None,
    tribunal: Optional[pd.Series] = None,
) -> pd.Series:
    """Return comarca IDs from case numbers or comarca/tribunal pair.

    Args:
        number: Case number series (used if provided).
        comarca: Comarca name series (used with ``tribunal``).
        tribunal: Tribunal name series (used with ``comarca``).

    Returns:
        Series of comarca IDs.

    Raises:
        Exception: If neither ``number`` nor both ``comarca`` and ``tribunal`` are provided.
    """
    if number is not None:
        comarca_id = get_foro_info(number).loc[:, "municipio_id"]
    elif comarca is not None and tribunal is not None:
        df = pd.DataFrame(
            {"comarca": comarca, "tribunal": tribunal, "index": comarca.index}
        )
        comarca = get_data("comarca.csv")
        df = df.merge(comarca, on=["tribunal", "comarca"], how="left", validate="m:1")
        df.index = df["index"]
        comarca_id = df["comarca_id"]
    else:
        raise Exception("Either number or comarca and" " tribunal must be specified")
    return comarca_id


def get_comarca(numbers: pd.Series) -> pd.Series:
    """Return comarca names derived from case numbers."""
    ids = get_foro_info(numbers).loc[:, "comarca_id"].to_frame()
    comarca = get_data("comarca.csv").set_index("id")
    df = ids.join(comarca, on="comarca_id", how="left")
    return df["comarca"]


def get_foro_info(numbers: pd.Series) -> pd.DataFrame:
    """Return full foro information by joining case numbers with the foro table.

    Args:
        numbers: Series of CNJ case number strings.

    Returns:
        DataFrame with foro details (id, name, comarca, municipality, etc.).
    """
    foro = get_data("foro.csv")
    foro['code_j'] = transform(foro.tribunal, 'tribunal', 'code_j')
    foro['code_tr'] = transform(foro.tribunal, 'tribunal', 'code_tr')
    index_name = numbers.index.name
    if not index_name:
        index_name = "index"
    foro_info = (
        extract_info_from_case_numbers(numbers, types=["CNJ"])
        .reset_index()
        .merge(
            foro,
            on=["code_j", "code_tr", "oooo"],
            how="left",
        )
    )
    foro_info.index = foro_info[index_name]
    return foro_info


def get_caderno_id(diario: pd.Series, caderno: pd.Series) -> pd.Series:
    """Return caderno (supplement) IDs from diario and caderno series.

    Args:
        diario: Series of diario identifiers.
        caderno: Series of caderno names.

    Returns:
        Series of caderno IDs.
    """
    ids = get_data("caderno.csv").set_index(["diario", "caderno"])
    df = pd.DataFrame({"diario": diario, "caderno": caderno}, index=caderno.index)
    df2 = df.join(ids, on=["diario", "caderno"])
    return df2["caderno_id"]
