import pandas as pd
import os
import sys
sys.path.append('/home/henrik/Dropbox/brazil/diarios')
from diarios.misc import get_user_config
from diarios.clean import clean_municipio
from diarios.clean import clean_estado
from diarios.clean import get_municipio_id

def main():
    df = get_subsecao()
    df = clean(df)
    df.to_csv('data/subsecao.csv', index=False)
    return df


def get_subsecao():
    infile = os.path.join(
        get_user_config('external_dropbox_directory'),
        'subsecoes',
        'Organização Justiça Federal.xlsx'
    )
    return pd.read_excel(infile)


def clean(df):
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
    return df

df = main()

df['estado'] = clean_estado(df.secao)
df['sede'] = clean_municipio(df.sede, df.estado)
df['sede_id'] = get_municipio_id(df.sede, df.estado)
df['municipio'] = clean_municipio(df.municipio, df.estado)
df['municipio_id'] = get_municipio_id(df.municipio, df.estado)

print(df.sample().iloc[0])
