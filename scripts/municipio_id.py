"""Generate municipio_id.csv and municipio_correction_tse.csv from TSE election data."""

import os
import re
from glob import glob
from typing import List, Tuple

import pandas as pd
import path
from diarios.clean import clean_text


def clean_municipio() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build municipality ID mapping and corrections from TSE data.

    Returns:
        Tuple of (municipio DataFrame, corrections DataFrame).
    """
    mun = get_mun()
    corr = get_corrections(mun)
    mun = (mun.sort_values('n').drop_duplicates(
        'municipio_id',
        keep='last').sort_values('municipio_id').drop(columns='n'))
    mun = mun.loc[mun.municipio_id.notnull()]
    mun.to_csv('diarios/data/municipio_id.csv', index=False)
    corr.to_csv('diarios/data/municipio_correction_tse.csv', index=False)
    return mun, corr


def get_mun() -> pd.DataFrame:
    """Read and combine municipality data from TSE election files.

    Returns:
        DataFrame with estado, municipio_id, and municipio columns.
    """
    indir = os.path.join(path.local_data_dir, 'TSE')
    infiles1 = glob('{}/*/*/votacao_candidato_munzona*'.format(indir))
    mun1 = pd.concat(map(get_municipio_id, infiles1), sort=True)
    infiles2 = glob('{}/*/*/consulta_cand*.csv'.format(indir))
    mun2 = pd.concat(map(get_municipio_nasc_id, infiles2), sort=True)
    mun = pd.concat([mun1, mun2], sort=True)
    mun['municipio_id'] = pd.to_numeric(mun.municipio_id, errors='coerce')
    mun['municipio'] = clean_text(mun.municipio, drop='^A-Z\\- ')
    cols = ['estado', 'municipio_id', 'municipio']
    mun = (mun.groupby(cols).agg('sum').reset_index())
    mun = mun.loc[mun.municipio_id > 0]
    mun = mun.dropna()
    missing = pd.DataFrame({'municipio_id': [-1, -3]})
    mun = pd.concat([mun, missing], sort=True)
    return mun


def get_municipio_id(infile: str) -> pd.DataFrame:
    """Extract municipality IDs from a votacao_candidato_munzona file.

    Args:
        infile: Path to the TSE election CSV file.

    Returns:
        DataFrame with estado, municipio_id, and municipio columns.
    """
    year = re.search('[0-9]{4}', infile).group(0)
    if year in ['2018', '2020']:
        cols = [10, 13, 14]
    else:
        cols = [5, 7, 8]
    return get_ids(infile, cols)


def get_municipio_nasc_id(infile: str) -> pd.DataFrame:
    """Extract municipality IDs from a consulta_cand file.

    Args:
        infile: Path to the TSE candidate CSV file.

    Returns:
        DataFrame with estado, municipio_id, and municipio columns.
    """
    year = re.search('[0-9]{4}', infile).group(0)
    if year in ['2014']:
        cols = [39, 40, 41]
    elif year in ['2016', '2018', '2020']:
        cols = [35, 36, 37]
    else:
        cols = [37, 38, 39]
    return get_ids(infile, cols)


def get_ids(infile: str, cols: List[int]) -> pd.DataFrame:
    """Read a TSE CSV and extract municipality ID columns.

    Args:
        infile: Path to the CSV file.
        cols: Column indices for estado, municipio_id, and municipio.

    Returns:
        DataFrame with estado, municipio_id, municipio, and count columns.
    """
    ids = pd.read_csv(infile,
                      header=None,
                      encoding='latin1',
                      sep=';',
                      usecols=cols)
    cols = ['estado', 'municipio_id', 'municipio']
    ids.columns = cols
    ids = (ids.groupby(cols).size().reset_index(name='n'))
    return ids


def get_corrections(mun: pd.DataFrame) -> pd.DataFrame:
    """Identify municipality name corrections from TSE data.

    Args:
        mun: Municipality DataFrame with n (count) column.

    Returns:
        DataFrame with estado, wrong, and correct columns.
    """
    mun.loc[mun.estado.isin(['BR', 'VT', 'ZZ']), 'n'] = 0
    mun = mun.sort_values('n')
    mun['correct'] = (
        mun.groupby('municipio_id')['municipio'].transform('last'))
    corr = mun.loc[(mun.municipio != mun.correct)
                   & ~mun.estado.isin(['BR', 'VT', 'ZZ']),
                   ('estado', 'municipio', 'correct')].dropna()
    corr.columns = ['estado', 'wrong', 'correct']
    return corr.drop_duplicates()


mun, corr = clean_municipio()
print(mun.sample(5))
