"""Generate foro.csv by combining foro data with tribunal and municipality info."""

from glob import glob

import numpy as np
import pandas as pd
import path
from diarios.clean import transform

# TODO: Add trabalhista (use varas/vara_year_TRT)

def read_csv(infile: str) -> pd.DataFrame:
    """Read a single foro CSV file with latin1 encoding.

    Args:
        infile: Path to the CSV file.

    Returns:
        DataFrame from the file.
    """
    return pd.read_csv(infile, encoding='latin1')


def add_foro_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Join foro_raw.csv to add foro names.

    Args:
        df: Foro DataFrame with ibge7 and oooo columns.

    Returns:
        DataFrame with foro_name column added.
    """
    foro_raw = (
        pd.read_csv('diarios/data/foro_raw.csv')
        .loc[:, ['ibge7', 'foro', 'oooo']]
        .drop_duplicates() # Should not be necessary
        .drop_duplicates(['ibge7', 'oooo'], keep=False) # Should drop Belo Horizonte only
        .rename(columns={'foro': 'foro_name'})
    )
    df = df.merge(foro_raw, on=['ibge7', 'oooo'], how='left', validate='m:1')
    return df


if __name__ == '__main__':
    infiles = glob('{}/foro/foro*.csv'.format(path.db_dir))
    df = pd.concat(map(read_csv, infiles))
    df = df.loc[:, ('foro', 'ibge7', 'idnom_forum')]
    df['oooo'] = df.foro % 10000
    df['code_j'] = np.floor(df.foro/1000000)
    df['code_tr'] = np.floor(df.foro/10000) % 100
    df['municipio_id'] = transform(df.ibge7, 'ibge7', 'municipio_id', infile='diarios/data/municipio.csv')
    tribunal = pd.read_csv('diarios/data/tribunal.csv').loc[:, ['code_j', 'code_tr', 'tribunal']]
    df = df.merge(tribunal, on=['code_j', 'code_tr'], validate='m:1', how='left')
    df = df.drop(columns=['code_j', 'code_tr'])
    df = add_foro_raw(df)
    df.loc[df.foro_name.isnull(), 'foro_name'] = df.idnom_forum
    df = df.drop(columns='idnom_forum')
    df.to_csv('diarios/data/foro.csv', index=False)
