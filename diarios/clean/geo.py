"""Geographic data functions and court classes."""

from __future__ import annotations

from typing import Dict, List, Optional, Union

import pandas as pd
import numpy as np
import re
from copy import copy

from diarios.clean.text import clean_text, get_data, get_data_file, get_estado_mapping, title, transform
from diarios.clean.numbers import extract_info_from_case_numbers

__all__ = [
    "TRT",
    "TRF",
    "get_trt_estados",
    "get_trf_estados",
    "get_trf_estados_mapping",
    "clean_estado",
    "get_capital",
    "extract_municipio",
    "clean_municipio",
    "get_municipio_id",
    "get_municipio_regex",
    "clean_comarca",
    "clean_vara",
    "get_foro_id",
    "get_foro",
    "get_comarca_id",
    "get_subsecao_id",
    "get_comarca",
    "get_foro_info",
    "get_caderno_id",
]


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


def get_municipio_id(
    municipio: pd.Series, estado: pd.Series, code: str = "tse"
) -> pd.Series:
    """Return municipality IDs by joining on municipality name and state.

    Args:
        municipio: Municipality names.
        estado: State abbreviations.
        code: ID system to return. ``"tse"`` (default) returns TSE
            ``municipio_id``; ``"ibge7"`` or ``"ibge6"`` converts via
            the municipio reference table.

    Returns:
        Series of municipality IDs.
    """
    df = pd.DataFrame(
        {"municipio": municipio, "estado": estado, "index": municipio.index}
    )
    ids = get_data("municipio_id.csv").dropna()
    ids = pd.merge(df, ids, on=["municipio", "estado"], validate="m:1", how="left")
    ids.index = ids["index"]
    if code == "tse":
        return ids.municipio_id
    return transform(ids.municipio_id, "municipio_id", code)


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
    comarca = comarca.str.replace("COMARCA DE", "", regex=False).str.strip()
    return comarca


def clean_vara(vara: pd.Series) -> pd.Series:
    """Clean vara (court division) text, keeping only alphanumeric and spaces."""
    vara = clean_text(vara, drop="^a-z0-9 ")
    return vara


def get_foro_id(numbers: pd.Series) -> pd.Series:
    """Return foro (court branch) IDs extracted from case numbers."""
    return get_foro_info(numbers).loc[:, "foro"]

def get_foro(numbers: pd.Series) -> pd.Series:
    """Return foro (court branch) names extracted from case numbers."""
    return get_foro_info(numbers).loc[:, "foro"]


# The municipio_year__comarca / __subsecao panels are frozen at the 2018 snapshot
# vintage, and that cross-section is value-identical to the legacy single-value
# comarca_id/subsecao_id columns that used to live in municipio.csv. So an
# unspecified-year lookup (year=DEFAULT_JURISDICTION_YEAR) reproduces the old
# frozen behaviour exactly, while callers with a real event year opt into the
# time-varying answer by passing it.
DEFAULT_JURISDICTION_YEAR = 2018


def _seat_from_case(
    number: Optional[pd.Series], foro: Optional[pd.Series]
) -> pd.Series:
    """The seat município of a case's foro, from a CNJ number or a foro id.

    Works for both state (code_j=8) and federal (code_j=4) foros — foro.csv
    carries all five TRFs, whose seat município is the subseção seat. So this is
    the shared case route for get_comarca_id (state) and get_subsecao_id (federal).
    """
    if number is not None:
        return get_foro_info(number).loc[:, "municipio_id"]
    return transform(foro, "foro", "municipio_id")


def _jurisdiction_asof(
    municipio_id: pd.Series, year: int, panel_file: str, id_col: str
) -> pd.Series:
    """As-of lookup against a municipio_year panel (``id_col | municipio_id | year``).

    Returns, per município, the ``id_col`` from the latest dated layer whose year
    is <= ``year`` (NaN if the requested year precedes every layer). Index and
    order follow ``municipio_id``.
    """
    panel = get_data(panel_file)
    eligible = panel[panel["year"] <= year]
    if eligible.empty:
        return pd.Series(pd.NA, index=municipio_id.index, name=id_col)
    seat = (
        eligible.sort_values("year")
        .drop_duplicates("municipio_id", keep="last")
        .set_index("municipio_id")[id_col]
    )
    seat.index = seat.index.astype("float64")
    # coerce the lookup key to float so Int64/object municipio_id still matches
    # the float-keyed panel (mirrors what transform()'s join does)
    keys = pd.to_numeric(municipio_id, errors="coerce").astype("float64")
    out = keys.map(seat)
    out.index = municipio_id.index
    out.name = id_col
    return out


def get_comarca_id(
    number: Optional[pd.Series] = None,
    comarca: Optional[pd.Series] = None,
    tribunal: Optional[pd.Series] = None,
    *,
    num_cnj: Optional[pd.Series] = None,
    foro: Optional[pd.Series] = None,
    municipio_id: Optional[pd.Series] = None,
    ibge7: Optional[pd.Series] = None,
    year: Optional[int] = None,
) -> pd.Series:
    """Resolve comarca_id (= the comarca-seat município_id) by one of three routes.

    Exactly one primary key must be given; new keys are keyword-only.

    Case route — decode the court location from the case itself (``year`` rejected):
        ``number=`` / ``num_cnj=``  CNJ case-number series (``num_cnj`` aliases ``number``)
        ``foro=``                   foro-id series
      Returns the município where the case's foro sits.

    Name route — look up by comarca + tribunal name (``year`` rejected):
        ``comarca=`` with ``tribunal=``

    Geo route — the time-varying município→comarca jurisdiction (the panel):
        ``municipio_id=`` or ``ibge7=`` , optional ``year=`` (default
        ``DEFAULT_JURISDICTION_YEAR`` = 2018). Returns the comarca the município
        belonged to as of ``year`` (the latest dated layer with year <= ``year``).

    Args:
        number/num_cnj: CNJ case-number series (case route).
        comarca, tribunal: comarca + tribunal name series (name route).
        foro: foro-id series (case route).
        municipio_id, ibge7: município key series (geo route).
        year: jurisdiction year; geo route only, defaults to 2018.

    Returns:
        Series of comarca IDs, aligned to the input.

    Raises:
        ValueError: on zero or multiple primary keys, a missing ``tribunal``, or
            ``year`` supplied to a non-geo route.
    """
    number = number if number is not None else num_cnj
    routes = {
        "number": number is not None,
        "foro": foro is not None,
        "comarca": comarca is not None,
        "municipio_id": municipio_id is not None,
        "ibge7": ibge7 is not None,
    }
    active = [k for k, on in routes.items() if on]
    if len(active) != 1:
        raise ValueError(
            "get_comarca_id: specify exactly one of number/num_cnj, foro, "
            f"comarca(+tribunal), municipio_id, ibge7 (got {active or 'none'})."
        )
    route = active[0]
    if year is not None and route not in ("municipio_id", "ibge7"):
        raise ValueError(
            "get_comarca_id: `year` applies only to the municipio_id/ibge7 (geo) route."
        )

    if route in ("number", "foro"):
        return _seat_from_case(number, foro)
    if route == "comarca":
        if tribunal is None:
            raise ValueError("get_comarca_id: `comarca` requires `tribunal`.")
        df = pd.DataFrame(
            {"comarca": comarca, "tribunal": tribunal, "index": comarca.index}
        )
        cdf = get_data("comarca.csv")
        df = df.merge(cdf, on=["tribunal", "comarca"], how="left", validate="m:1")
        df.index = df["index"]
        return df["comarca_id"]
    # geo route
    mid = municipio_id if municipio_id is not None else transform(
        ibge7, "ibge7", "municipio_id", infile=get_data_file("municipio.csv"))
    if not isinstance(mid, pd.Series):
        mid = pd.Series(mid)
    y = DEFAULT_JURISDICTION_YEAR if year is None else int(year)
    return _jurisdiction_asof(mid, y, "municipio_year__comarca.csv", "comarca_id")


def get_subsecao_id(
    *,
    number: Optional[pd.Series] = None,
    num_cnj: Optional[pd.Series] = None,
    foro: Optional[pd.Series] = None,
    municipio_id: Optional[pd.Series] = None,
    ibge7: Optional[pd.Series] = None,
    year: Optional[int] = None,
) -> pd.Series:
    """Resolve subsecao_id (= the subseção-seat município_id). Federal twin of
    get_comarca_id; exactly one primary key, all keyword-only.

    Case route — decode the subseção from the (federal) case itself (``year``
    rejected): ``number=`` / ``num_cnj=`` CNJ series, or ``foro=`` foro-id series.
    foro.csv carries all five TRFs, so a federal case's foro seat is its subseção
    seat.

    Geo route — the time-varying município→subseção jurisdiction:
        ``municipio_id=`` or ``ibge7=`` , optional ``year=`` (default 2018), as-of
        the latest dated layer with year <= ``year``, from the
        municipio_year__subsecao panel.

    Raises:
        ValueError: on zero or multiple primary keys, or ``year`` on the case route.
    """
    number = number if number is not None else num_cnj
    routes = {
        "number": number is not None,
        "foro": foro is not None,
        "municipio_id": municipio_id is not None,
        "ibge7": ibge7 is not None,
    }
    active = [k for k, on in routes.items() if on]
    if len(active) != 1:
        raise ValueError(
            "get_subsecao_id: specify exactly one of number/num_cnj, foro, "
            f"municipio_id, ibge7 (got {active or 'none'})."
        )
    route = active[0]
    if year is not None and route not in ("municipio_id", "ibge7"):
        raise ValueError(
            "get_subsecao_id: `year` applies only to the municipio_id/ibge7 (geo) route."
        )
    if route in ("number", "foro"):
        return _seat_from_case(number, foro)
    mid = municipio_id if municipio_id is not None else transform(
        ibge7, "ibge7", "municipio_id", infile=get_data_file("municipio.csv"))
    if not isinstance(mid, pd.Series):
        mid = pd.Series(mid)
    y = DEFAULT_JURISDICTION_YEAR if year is None else int(year)
    return _jurisdiction_asof(mid, y, "municipio_year__subsecao.csv", "subsecao_id")


def get_comarca(numbers: pd.Series) -> pd.Series:
    """Return comarca names derived from case numbers."""
    ids = get_foro_info(numbers).loc[:, "municipio_id"].to_frame()
    comarca = get_data("comarca.csv").set_index("comarca_id")
    df = ids.join(comarca, on="municipio_id", how="left")
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
