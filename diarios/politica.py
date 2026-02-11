"""Political data utilities for elections and party coalitions."""

from __future__ import annotations

from typing import Union

import pandas as pd
import numpy as np
from .clean import get_data


def split_coalition(coalition: pd.Series, name: str = "party") -> pd.Series:
    """Split coalition strings into individual party names.

    Args:
        coalition: Series of coalition strings separated by `` / ``.
        name: Name for the resulting Series.

    Returns:
        Series with one row per party, preserving the original index.
    """
    return (
        coalition.str.split(" / ", expand=True)
        .stack()
        .rename(name)
        .droplevel(-1)
        .str.strip()
    )


def get_district(df: pd.DataFrame) -> Union[pd.Series, np.ndarray]:
    """Determine the electoral district for each candidate.

    Municipal offices use ``municipio_id``; state/federal offices use
    ``estado``.

    Args:
        df: DataFrame with ``office``, ``municipio_id``, and ``estado`` columns.

    Returns:
        Array of district identifiers.
    """
    district = np.where(
        df.office.isin(["PREFEITO", "VEREADOR"]), df.municipio_id, df.estado
    )
    return district


def get_office_type(office: pd.Series) -> pd.Series:
    """Classify offices as proportional representation or majority.

    Args:
        office: Series of office names (e.g., ``"VEREADOR"``, ``"GOVERNADOR"``).

    Returns:
        Series with values ``"pr"`` or ``"majority"``.
    """
    mapping = {
        "VEREADOR": "pr",
        "DEPUTADO ESTADUAL": "pr",
        "DEPUTADO FEDERAL": "pr",
        "PREFEITO": "majority",
        "GOVERNADOR": "majority",
        "PRESIDENTE": "majority",
        "SENADOR": "majority",
    }
    return office.map(mapping)


def get_election_date(year: pd.Series, rnd: int = 1) -> pd.Series:
    """Look up election dates for given years and rounds.

    Args:
        year: Series of election years.
        rnd: Election round (1 or 2).

    Returns:
        Series of election dates as datetime.
    """
    dates = get_data("eleicao.csv")
    dates["electiondate"] = pd.to_datetime(dates.electiondate)
    dt = pd.DataFrame({"year": year, "round": rnd, "index": year.index})
    dt = pd.merge(dt, dates, on=["year", "round"], how="left")
    dt.index = dt["index"]
    return dt.electiondate


def calculate_name_log_likelihood(names: pd.Series) -> pd.DataFrame:
    """Compute log-likelihood scores for candidate names.

    Each token in a name is looked up in a reference table of name
    frequencies. Tokens not in the list receive twice the minimum
    log-likelihood.

    Args:
        names: Series with unique index containing full names.

    Returns:
        DataFrame with a ``ll`` column of summed log-likelihoods per name.

    Raises:
        ValueError: If the index is not unique.
    """
    if len(names) > len(names.index.drop_duplicates()):
        raise ValueError("Must have unique index")
    name_ll = get_data("name_ll.csv")
    likelihood_if_not_in_list = 2 * min(name_ll["ll"])
    names = names.str.split(expand=True)
    names["index"] = names.index
    names = (
        pd.melt(names, id_vars="index", value_name="name")
        .query("name.notnull()")
        .loc[:, ("index", "name")]
        .merge(name_ll, on="name", how="left")
        .fillna(likelihood_if_not_in_list)
    )
    ll = pd.pivot_table(names, index="index", values=["ll"], aggfunc="sum")
    return ll
