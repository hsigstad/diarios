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
            'municipio_id', 'municipio',
            'municipio_accents',
            'ibge7',
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
    municipio['comarca'] = clean_text(
        municipio['comarca'],
        drop='^a-z\- '        
    )
    municipio['ibge6'] = municipio['muni_code']//10
    municipio['estado_id'] = transform(
        municipio.muni_state,
        'estado', 'estado_id'
    )
    municipio = add_comarca_id(municipio)  
    municipio = (
        municipio
        .loc[:, ('estado_id', 'muni_name',
                 'muni_code', 'ibge6',
                 'comarca_id')]        
        .rename(columns={
            'muni_code': 'ibge7',
            'muni_name': 'municipio_accents'
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


def get_subsecao():
    infile = os.path.join(
        get_user_config('external_dropbox_directory'),
        'subsecoes',
        'Organização Justiça Federal.xlsx'
    )
    return pd.read_excel(infile)


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
    df['sede'] = df.sede.str.replace(
        'Altamira\( exceto.*', 'Altamira'
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
    df = df.drop_duplicates()
    for row in get_wrong_rows():
        df = df.loc[~(
            (df.estado == row[0]) &
            (df.municipio == row[1]) &
            (df.subsecao == row[2])
        )]
    df = df.drop_duplicates('municipio_id') #NB!!!
    return df.loc[:, ('municipio_id', 'subsecao_id')]
    

def get_wrong_rows():
    return [
        ['AC', 'cruzeiro do sul', 'rio branco'],
        ['ES', 'fundao', 'vitoria'],
        ['ES', 'serra', 'vitoria'],
        # §1º. As Varas Federais Criminais da sede (art. 36) alcançam também os municípios de
        # Serra e Fundão no âmbito de suas competências em razão da matéria.
        # §2º. As Varas Federais de Execução Fiscal da sede (art. 35) alcançam também os
        # municípios de Serra e Fundão no âmbito de sua competência em razão da matéria.
        # §3º. As Varas Federais Cíveis da sede com competência para conhecer matéria
        # tributária (art. 34, inciso I) alcançam também os municípios de Serra e Fundão, no
        # âmbito de sua competência.
        # Art. 15. A Subseção de Serra, composta por uma Vara Federal de competência cível,
        # incluindo Juizado Especial Federal Adjunto, alcança a extensão territorial dos
        # municípios de Serra e Fundão, observado o disposto no artigo anterior. 
        ['RJ', 'belford roxo', 'sao joao de meriti'],
        ['RJ', 'duque de caxias', 'sao joao de meriti'],
        ['RJ', 'japeri' ,'sao joao de meriti'],
        ['RJ', 'queimados', 'sao joao de meriti'],
        # Municípios de Belfort Roxo, Queimados, Japeri e Duque de
        # Caxias: A subseção de Duque de Caxias alcança esses
        # municípios, sendo competente para o processamento e
        # julgamento das causas afetas às Varas Federais e aos
        # Juizados Especiais Federais, com exceção das causas
        # criminais, cuja competência é atribuída às 3ª e 4ª Varas
        # Federais de São João de Meriti.
        ['MG', 'santa vitoria', 'uberlandia']
    ]

mun = main()
print(mun.sample().iloc[0])
