"""Generate diario.csv by joining caderno data with tribunal metadata."""

import pandas as pd

diario = (
    pd.read_csv('data/caderno.csv')
    .loc[:, ('diario')]
    .drop_duplicates()
    .to_frame()
    .sort_values('diario')
    .reset_index(drop=True)
)
diario['tribunal'] = (
    diario['diario'].str.replace('DJ', 'TJ')
)
diario.loc[diario.tribunal == 'MP-MG', 'tribunal'] = ''
tribunal = (
    pd.read_csv('data/tribunal.csv')
    .loc[:, ('tribunal', 'id', 'estado', 'estado_id')]
    .rename(columns={'id': 'tribunal_id'})
)
diario = (
    diario
    .merge(tribunal, on='tribunal', how='left')
    .sort_values(['tribunal_id', 'diario'])
    .reset_index(drop=True)
)
diario.index = diario.index + 1
diario.index.name = 'diario_id'
diario.to_csv('data/diario.csv')
