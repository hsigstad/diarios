"""Generate comarca.csv from judicial district data and comarca info."""

import os
from typing import Tuple

import pandas as pd
import path
from diarios.clean import clean_municipio, clean_text, get_municipio_id, transform


def main() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build foro and comarca DataFrames and write comarca.csv.

    Returns:
        Tuple of (foro DataFrame, comarca DataFrame).
    """
    foro_comarca = get_foro_comarca()
    foro_comarca = clean_foro_comarca(foro_comarca)
    foro = (foro_comarca.loc[:, ('foro_id', 'tribunal', 'oooo', 'estado',
                                 'estado_id', 'comarca_id')])
    comarca = (foro_comarca.loc[:, ('comarca_id', 'comarca', 'tribunal',
                                    'estado_id', 'estado',
                                    'n_municipios')].drop_duplicates(
                                        subset='comarca_id'))
    comarca = add_comarca_info(comarca)
    comarca = comarca.sort_values('comarca_id')
    #foro.to_csv('diarios/data/foro.csv', index=False) # Use foro.py
    comarca.to_csv('diarios/data/comarca.csv', index=False)
    return foro, comarca


def get_foro_comarca() -> pd.DataFrame:
    """Read and concatenate all comarca CSV files from the database directory.

    Returns:
        Combined DataFrame of all comarca data.
    """
    indir = os.path.join(path.db_dir, 'comarcas')
    infiles = [os.path.join(indir, f) for f in os.listdir(indir)]
    return pd.concat(map(pd.read_csv, infiles), sort=True)


def clean_foro_comarca(foro: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the raw foro/comarca data.

    Args:
        foro: Raw foro/comarca DataFrame.

    Returns:
        Cleaned DataFrame with IDs and municipality counts.
    """
    foro = foro.reset_index(drop=True)
    foro['comarca'] = clean_municipio(foro.comarca, foro.muni_state)
    foro['estado_id'] = transform(foro['muni_state'], 'estado', 'estado_id')
    foro.loc[foro['muni_state'].isnull(), 'estado_id'] = foro['state_codetj']
    foro['tribunal_id'] = foro['estado_id']
    foro['estado'] = transform(foro.estado_id, 'estado_id', 'estado')
    foro['tribunal'] = transform(foro.tribunal_id, 'tribunal_id', 'tribunal')
    n_municipios = (foro.drop_duplicates(
        subset=['comarca', 'muni_name', 'estado_id']).groupby(
            ['comarca', 'estado_id']).size().reset_index(name='n_municipios'))
    foro = pd.merge(foro,
                    n_municipios,
                    on=['comarca', 'estado_id'],
                    how='left')
    foro = (foro.rename(columns={
        'comarca_codetj': 'oooo'
    }).loc[:,
           ('oooo', 'comarca', 'tribunal_id', 'tribunal', 'estado',
            'estado_id', 'n_municipios'
            )].query('estado_id.notnull() & oooo.notnull()').drop_duplicates(
                subset=['oooo', 'estado_id']).sort_values(
                    ['tribunal_id', 'oooo']).reset_index(drop=True))
    foro['comarca_id'] = get_municipio_id(foro.comarca, foro.estado)
    miss = foro.query('comarca_id.isnull()')
    if len(miss) > 0:
        print(miss)
        raise Exception('Unknown municipalities', miss[['estado', 'comarca']])
    foro['foro_id'] = foro.index + 1
    return foro


def add_comarca_info(comarca: pd.DataFrame) -> pd.DataFrame:
    """Join comarca with entrancia and juizes info.

    Args:
        comarca: Comarca DataFrame to enrich.

    Returns:
        DataFrame with entrancia and juizes columns added.
    """
    info_file = os.path.join(
        path.db_dir,
        'comarcas/comarca_info.csv',
    )
    info = pd.read_csv(info_file,
                       usecols=['state', 'name', 'entrancia',
                                'juizes']).rename(columns={'name': 'comarca'})
    info['estado_id'] = transform(info['state'], 'estado', 'estado_id')
    info['comarca'] = clean_text(info['comarca'])
    comarca = (comarca.merge(
        info, on=['comarca', 'estado_id'], how='left').drop(
            columns=['state']).drop_duplicates(subset='comarca_id'))
    return comarca


foro, comarca = main()
