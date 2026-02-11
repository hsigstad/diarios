"""Generate municipio.csv by combining TSE, IBGE, and comarca data."""

import os
from typing import List, Tuple

import pandas as pd
import path
from diarios.clean import (
    clean_estado,
    clean_municipio,
    clean_text,
    get_data,
    get_municipio_id,
    transform,
)


def main() -> pd.DataFrame:
    """Build the municipio table and write municipio.csv.

    Returns:
        Complete municipality DataFrame.
    """
    mun = pd.read_csv('diarios/data/municipio_id.csv')
    mun_comarca = get_municipio_comarca()
    mun_comarca = clean_municipio_comarca(mun_comarca)
    mun_ibge = get_mun_ibge()
    mun = pd.merge(mun, mun_ibge, on='municipio_id', how='left')
    mun = pd.merge(mun, mun_comarca, on='ibge6', how='left')
    # NB: 277 where comarca_id and
    # comarca_id2 disagree.
    # Using comarca_id for those cases.
    mun = impute_comarca_id(mun)
    mun['estado'] = transform(mun.estado_id, 'estado_id', 'estado')
    mun = add_subsecao_id(mun)
    mun = (mun.loc[:, ('municipio_id', 'municipio', 'municipio_accents',
                       'ibge7', 'ibge6', 'estado', 'estado_id', 'comarca_id',
                       'subsecao_id')].query('municipio_id.notnull()').
           drop_duplicates('municipio_id').sort_values('municipio_id'))
    mun.to_csv('diarios/data/municipio.csv', index=False)
    return mun


def get_mun_ibge() -> pd.DataFrame:
    """Read IBGE municipality codes and judicial district mapping.

    Returns:
        DataFrame with municipio_id, ibge6, ibge7, and comarca_id2 columns.
    """
    infile = os.path.join(
        path.db_dir,
        'municipios',
        'clean',
        'municipios_all_codes.csv',
    )
    cols = {
        'id_TSE': 'municipio_id',
        'id_munic_6': 'ibge6',
        'id_munic_7': 'ibge7',
        'id_judicial_district': 'comarca7'
    }
    df = pd.read_csv(infile, usecols=cols.keys()).rename(columns=cols)
    cid = (df.loc[:, ('ibge7', 'municipio_id')].rename(columns={
        'municipio_id': 'comarca_id2',
        'ibge7': 'comarca7'
    }))
    df = df.merge(cid, on='comarca7', how='left')
    df = df.drop(columns='comarca7')
    return df


def get_municipio_comarca() -> pd.DataFrame:
    """Read and concatenate comarca CSV files.

    Returns:
        Combined comarca DataFrame.
    """
    indir = os.path.join(path.db_dir, 'comarcas')
    infiles = [os.path.join(indir, f) for f in os.listdir(indir)]
    return pd.concat(map(pd.read_csv, infiles), sort=True)


def clean_municipio_comarca(municipio: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw municipio/comarca mapping.

    Args:
        municipio: Raw DataFrame with comarca and muni_code columns.

    Returns:
        Cleaned DataFrame with ibge6 and comarca_id columns.
    """
    municipio['comarca'] = clean_text(municipio['comarca'], drop='^A-Z\\- ')
    municipio['ibge6'] = municipio['muni_code'] // 10
    municipio['estado_id'] = transform(municipio.muni_state, 'estado',
                                       'estado_id')
    municipio = add_comarca_id(municipio)
    municipio['muni_name'] = municipio['muni_name'].str.upper()
    municipio = (municipio.loc[:, ('estado_id', 'muni_name',
                                   'ibge6', 'comarca_id')].rename(
                                       columns={
                                           'muni_name': 'municipio_accents'
                                       }).drop_duplicates().sort_values(
                                           ['estado_id']))
    return municipio


def add_comarca_id(municipio: pd.DataFrame) -> pd.DataFrame:
    """Merge comarca_id from the comarca reference table.

    Args:
        municipio: DataFrame to enrich.

    Returns:
        DataFrame with comarca_id column added.
    """
    comarca = get_data('comarca.csv')
    comarca = (comarca.loc[:, ('comarca_id', 'comarca', 'estado_id')])
    return pd.merge(municipio,
                    comarca,
                    on=['comarca', 'estado_id'],
                    how='left')


def impute_comarca_id(mun: pd.DataFrame) -> pd.DataFrame:
    """Fill missing comarca_id from the IBGE-based comarca_id2.

    Args:
        mun: Municipality DataFrame with comarca_id and comarca_id2.

    Returns:
        DataFrame with imputed comarca_id and comarca_id2 dropped.
    """
    mun.loc[mun.comarca_id.isnull(), 'comarca_id'] = mun.comarca_id2
    mun = mun.drop(columns='comarca_id2')
    return mun


def add_subsecao_id(municipio: pd.DataFrame) -> pd.DataFrame:
    """Add federal judiciary subsecao_id to the municipality table.

    Args:
        municipio: Municipality DataFrame.

    Returns:
        DataFrame with subsecao_id column added.
    """
    df = get_subsecao()
    df = clean_subsecao(df)
    return pd.merge(municipio,
                    df,
                    on=['municipio_id'],
                    validate='m:1',
                    how='left')


def get_subsecao() -> pd.DataFrame:
    """Read the federal judiciary organization Excel file.

    Returns:
        Raw subsecao DataFrame.
    """
    infile = os.path.join(
        path.db_dir,
        'subsecoes',
        'Organização Justiça Federal.xlsx',
    )
    return pd.read_excel(
        infile,
        engine='openpyxl',
    )


def clean_subsecao(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the subsecao data and map municipalities to subsecao IDs.

    Args:
        df: Raw subsecao DataFrame from Excel.

    Returns:
        DataFrame with municipio_id and subsecao_id columns.
    """
    cols = {
        'Região TRF': 'tribunal',
        'Seção  Judiciária': 'secao',
        'Subseção Judiciária': 'subsecao',
        'Município sede': 'sede',
        'Jurisdição': 'municipio'
    }
    df = (df.rename(columns=cols).loc[:, cols.values()])
    df['sede'] = df.sede.str.replace('Altamira\\( exceto.*', 'Altamira', regex=True)
    sede = df.drop(columns='municipio').drop_duplicates()
    sede['municipio'] = sede.sede
    df = pd.concat([df, sede])
    df['estado'] = clean_estado(df.secao)
    df['subsecao'] = clean_municipio(df.sede, df.estado)
    df['subsecao_id'] = get_municipio_id(df.subsecao, df.estado)
    df['municipio'] = clean_municipio(df.municipio, df.estado)
    df['municipio_id'] = get_municipio_id(df.municipio, df.estado)
    df = df.loc[df.subsecao_id.notnull()]
    df = df.drop_duplicates()
    for row in get_wrong_rows():
        df = df.loc[~((df.estado == row[0]) & (df.municipio == row[1]) &
                      (df.subsecao == row[2]))]
    df = df.drop_duplicates('municipio_id')  #NB!!!
    return df.loc[:, ('municipio_id', 'subsecao_id')]


def get_wrong_rows() -> List[Tuple[str, str, str]]:
    """Return rows to exclude due to jurisdiction edge cases.

    Returns:
        List of (estado, municipio, subsecao) tuples to drop.
    """
    return [
        ('AC', 'cruzeiro do sul', 'rio branco'),
        ('ES', 'fundao', 'vitoria'),
        ('ES', 'serra', 'vitoria'),
        ('RJ', 'belford roxo', 'sao joao de meriti'),
        ('RJ', 'duque de caxias', 'sao joao de meriti'),
        ('RJ', 'japeri', 'sao joao de meriti'),
        ('RJ', 'queimados', 'sao joao de meriti'),
        ('MG', 'santa vitoria', 'uberlandia')
    ]


if __name__ == '__main__':
    df = main()
    print(df.sample().iloc[0])
