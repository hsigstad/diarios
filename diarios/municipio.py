import pandas as pd
import sys
import os
import glob
import re
from unidecode import unidecode
import sys
sys.path.append('/home/henrik/Dropbox/brazil/diarios')
from diarios.misc import get_user_config
from diarios.clean import clean_text
from diarios.clean import transform
from diarios.clean import get_data
from diarios.clean import clean_municipio
from diarios.clean import clean_estado
from diarios.clean import get_municipio_id


def main():
    mun = pd.read_csv('data/municipio_id.csv')
    mun_comarca = get_municipio_comarca()
    mun_comarca = clean_municipio_comarca(mun_comarca)
    mun_ibge = get_mun_ibge()
    mun = pd.merge(
        mun,
        mun_ibge,
        on='municipio_id',
        how='left'
    )
    mun = pd.merge(
        mun,
        mun_comarca,
        on='ibge6',
        how='left'
    )
    mun['estado'] = transform(
        mun.estado_id,
        'estado_id', 'estado'
    )
    mun = add_subsecao_id(mun)
    mun = (
        mun.loc[:, (
            'municipio_id', 'municipio', 'ibge7',
            'ibge6', 'estado',
            'estado_id', 'comarca_id',
            'subsecao_id'
        )]
        .query('municipio_id.notnull()')
        .drop_duplicates('municipio_id')
        .sort_values('municipio_id')
    )
    mun.to_csv('data/municipio.csv', index=False)
    outfile = os.path.join(
        get_user_config('external_dropbox_directory'),
        'municipios',
        'municipio.csv'
    )
    mun.to_csv(outfile, index=False)    
    return mun


def get_mun_ibge():
    infile = os.path.join(
        get_user_config('external_dropbox_directory'),
        'municipios',
        'codes_tse_ibge.csv'        
    )
    return (
        pd.read_csv(infile)
        .rename(columns={'tse': 'municipio_id'})
    )

    
def get_municipio_comarca():
    indir = os.path.join(
        get_user_config('external_dropbox_directory'),
        'comarcas'
    )
    infiles = [
        os.path.join(indir, f) for f in os.listdir(indir)
    ]
    return pd.concat(
        map(pd.read_csv, infiles),
        sort=True
    )


def clean_municipio_comarca(municipio):
    municipio['comarca'] = clean_text(municipio['comarca'])
    municipio['ibge6'] = municipio['muni_code']//10
    municipio['estado_id'] = transform(
        municipio.muni_state,
        'estado', 'estado_id'
    )
    municipio = add_comarca_id(municipio)  
    municipio = (
        municipio
        .loc[:, ('estado_id',
                 'muni_code', 'ibge6',
                 'comarca_id')]        
        .rename(columns={
            'muni_code': 'ibge7'
        })     
        .drop_duplicates()
        .sort_values(['estado_id'])
    )
    return municipio


def add_comarca_id(municipio):
    comarca = get_data('comarca.csv')
    comarca = (
        comarca
        .loc[:, ('comarca_id',
                 'comarca', 'estado_id')]
    )
    return pd.merge(
        municipio, comarca,
        on=['comarca', 'estado_id'],
        how='left'
    )


def add_subsecao_id(municipio):
    df = get_subsecao()
    df = clean_subsecao(df)
    return pd.merge(
        municipio, df,
        on=['municipio_id'],
        validate='m:1',
        how='left'
    )    



def clean_subsecao(df):
    cols = {
        'Região TRF': 'tribunal',
        'Seção  Judiciária': 'secao',
        'Subseção Judiciária': 'subsecao',
        'Município sede': 'sede',
        'Jurisdição': 'municipio'
    }
    df = (
        df.rename(columns=cols)
        .loc[:, cols.values()]
    )
    sede = df.drop(columns='municipio').drop_duplicates()
    sede['municipio'] = sede.sede
    df = pd.concat([df, sede])
    df['estado'] = clean_estado(df.secao)
    df['subsecao'] = clean_municipio(df.sede, df.estado)
    df['subsecao_id'] = get_municipio_id(df.subsecao, df.estado)
    df['municipio'] = clean_municipio(df.municipio, df.estado)
    df['municipio_id'] = get_municipio_id(df.municipio, df.estado)
    df = df.loc[df.subsecao_id.notnull()]
    df = df.drop_duplicates('municipio_id') #NB!!!
    return df.loc[:, ('municipio_id', 'subsecao_id')]


def get_subsecao():
    infile = os.path.join(
        get_user_config('external_dropbox_directory'),
        'subsecoes',
        'Organização Justiça Federal.xlsx'
    )
    return pd.read_excel(infile)

mun = main()

print(mun.sample(5).iloc[0])
