import pandas as pd
import os
import glob
import re
import sys
sys.path.append('/home/henrik/Dropbox/brazil/diarios')
from diarios.misc import get_user_config
from diarios.clean import clean_text


def clean_municipio():
    mun = get_mun()
    corr = get_corrections(mun)
    mun = (
        mun
        .sort_values('n')
        .drop_duplicates('municipio_id', keep='last')
        .sort_values('municipio_id')
        .drop(columns='n')
    )
    mun = mun.loc[
        mun.municipio_id.notnull()
    ]
    mun.to_csv(
        'data/municipio_id.csv',
        index=False
    )
    corr.to_csv(
        'data/municipio_correction_tse.csv',
        index=False
    )    
    return mun, corr


def get_mun():
    indir = os.path.join(
        get_user_config('external_dropbox_directory'),
        'elections'
    )
    mun1 = pd.concat(
        map(
            get_municipio_id,
            glob.glob(
                '{}/*/*/votacao_candidato_munzona*'
                .format(indir)
            )
        ),
        sort=True
    )
    mun2 = pd.concat(
        map(
            get_municipio_nasc_id,
            glob.glob(
                '{}/*/*/consulta_cand*'
                .format(indir))
        ),
        sort=True
    )
    mun = pd.concat(
        [mun1, mun2],
        sort=True
    )
    mun['municipio_id'] = pd.to_numeric(
        mun.municipio_id, errors='coerce'
    )
    mun['municipio'] = clean_text(
        mun.municipio
    )
    cols = [
        'estado',
        'municipio_id',
        'municipio'
    ]
    mun = (
        mun.groupby(cols)
        .agg('sum')
        .reset_index()
    )
    mun = mun.loc[mun.municipio_id>0]
    mun = mun.dropna()
    missing = pd.DataFrame({
        'municipio_id': [-1, -3]
    })
    mun = pd.concat(
        [mun, missing],
        sort=True
    )
    return mun


def get_municipio_id(infile):
    year = re.search(
        '[0-9]{4}', infile
    ).group(0)
    if year == '2018':
        cols = [10, 13, 14]
    else:
        cols = [5, 7, 8]
    return get_ids(infile, cols)


def get_municipio_nasc_id(infile):
    year = re.search(
        '[0-9]{4}', infile
    ).group(0)
    if year in ['2014', '2016']:
        cols = [5, 40, 41]
    elif year == '2018':
        cols = [5, 36, 37]
    else:
        cols = [5, 38, 39]
    return get_ids(infile, cols)


def get_ids(infile, cols):        
    ids = pd.read_csv(
        infile,
        header=None,
        encoding='latin1',
        sep=';',
        usecols=cols
    )
    cols = [
        'estado',
        'municipio_id',
        'municipio'
    ]
    ids.columns = cols
    ids = (
        ids.groupby(cols)
        .size()
        .reset_index(name='n')
    )
    return ids


def get_corrections(mun):
    mun.loc[
        mun.estado.isin(['BR', 'VT', 'ZZ']),
        'n'
    ] = 0
    mun = mun.sort_values('n')
    mun['correct'] = (
        mun.groupby('municipio_id')
        ['municipio']
        .transform('last')
    )
    corr = mun.loc[
        (mun.municipio!=mun.correct) &
        ~mun.estado.isin(['BR', 'VT', 'ZZ']),
        ('estado', 'municipio', 'correct')
    ].dropna()
    corr.columns = ['estado', 'wrong', 'correct']
    return corr.drop_duplicates()


mun, corr = clean_municipio()
print(mun.sample(5))
