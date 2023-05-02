import path
import pandas as pd
import numpy as np
import os
from diarios.clean import transform
from glob import glob

# TODO: Add trabalhista (use varas/vara_year_TRT)

infiles = glob('{}/foro/foro*.csv'.format(path.db_dir))

def read_csv(infile):
    return pd.read_csv(infile, encoding='latin1')

df = pd.concat(map(read_csv, infiles))
df = df.loc[:, ('foro', 'ibge7', 'idnom_forum')]
df = df.rename(columns={'idnom_forum': 'foro_name'})
df['oooo'] = df.foro % 10000
df['code_j'] = np.floor(df.foro/1000000)
df['code_tr'] = np.floor(df.foro/10000) % 100
df['municipio_id'] = transform(df.ibge7, 'ibge7', 'municipio_id', infile='diarios/data/municipio.csv')
tribunal = pd.read_csv('diarios/data/tribunal.csv').loc[:, ['code_j', 'code_tr', 'tribunal']]
df = df.merge(tribunal, on=['code_j', 'code_tr'], validate='m:1', how='left')
df = df.drop(columns=['code_j', 'code_tr'])
df.to_csv('diarios/data/foro.csv', index=False)
