"""Generate tribunal.csv by enriching tribunal_manual.csv with diary date ranges."""

import os
from typing import Optional

import pandas as pd
from diarios.misc import get_user_config

os.chdir('/home/henrik/Dropbox/brazil/diarios')
os.chdir('/home/henrik/Dropbox/brazil/diarios/diarios')


def main() -> pd.DataFrame:
    """Read tribunal_manual.csv and add diary start/end dates.

    Returns:
        Enriched tribunal DataFrame.
    """
    df = pd.read_csv(
        'data/tribunal_manual.csv'
    )
    df['diario_start'] = get_diario_start(
        df.tribunal
    )
    df['diario_end'] = get_diario_end(
        df.tribunal
    )
    df.to_csv('data/tribunal.csv')
    return df


def get_diario_start(tribunal: pd.Series) -> pd.Series:
    """Get the earliest diary date for each tribunal.

    Args:
        tribunal: Series of tribunal names.

    Returns:
        Series of start date strings.
    """
    return tribunal.apply(
        lambda x: get_date(x, 0)
    )


def get_diario_end(tribunal: pd.Series) -> pd.Series:
    """Get the latest diary date for each tribunal.

    Args:
        tribunal: Series of tribunal names.

    Returns:
        Series of end date strings.
    """
    return tribunal.apply(
        lambda x: get_date(x, -1)
    )


def get_date(diario: str, loc: int) -> str:
    """Find the first or last date in a diary's directory tree.

    Args:
        diario: Tribunal/diary name.
        loc: 0 for earliest date, -1 for latest date.

    Returns:
        Date string in ``'YYYY-MM-DD'`` format, or empty string if not found.
    """
    indir = get_user_config(
        'external_local_directory'
    )
    indir = os.path.join(
        indir, 'diarios', diario
    )
    if not os.path.isdir(indir):
        return ''
    year = get_loc(indir, loc)
    day = None
    i = 0
    while not day:
        if loc == 0:
            loc2 = loc + i
        else:
            loc2 = loc - i
        month = get_loc(
            os.path.join(indir, year),
            loc2
        )
        day = get_loc(
            os.path.join(indir, year, month),
            loc
        )
        i += 1
    return '{}-{}-{}'.format(year, month, day)


def get_loc(indir: str, loc: int) -> Optional[str]:
    """Get a sorted directory name at a given position.

    Args:
        indir: Directory to list.
        loc: Index into the sorted subdirectory list.

    Returns:
        Subdirectory name, or None if no subdirectories exist.
    """
    lst = [
        x for x in
        os.listdir(indir)
        if os.path.isdir(os.path.join(indir, x))
    ]
    if len(lst) == 0:
        return None
    else:
        return lst[loc]


df = main()
