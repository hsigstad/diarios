import pandas as pd
import clean
import os
import misc
import importlib
importlib.reload(clean)

# This is the code to generate data/foro.csv and data/comarca.csv
# Should not be used, unless we want to regenerate those files
# Need user-config.yaml with directory of Dropbox data folder

def main():
    foro_comarca = get_foro_comarca()
    foro_comarca = clean_foro_comarca(foro_comarca)
    foro = (
        foro_comarca
        .loc[:, ('foro_id', 'tribunal_id', 'oooo',
                 'estado_id', 'comarca_id')]
    )
    comarca = (
        foro_comarca
        .loc[:, ('comarca_id', 'comarca', 'tribunal_id',
                 'estado_id', 'n_municipios')]
        .drop_duplicates(subset='comarca_id')
    )
    comarca = add_comarca_info(comarca)
    comarca = comarca.sort_values('comarca_id')
    foro.to_csv('data/foro.csv', index=False)
    comarca.to_csv('data/comarca.csv', index=False)
    return foro, comarca


def get_foro_comarca():
    indir = os.path.join(
        misc.get_user_config('external_dropbox_directory'),
        'comarcas'
    )
    infiles = [
        os.path.join(indir, f) for f in os.listdir(indir)
    ]
    return pd.concat(
        map(pd.read_csv, infiles), sort=True
    )


def clean_foro_comarca(foro):
    foro = foro.reset_index(drop=True)
    foro['comarca'] = clean.clean_text(foro['comarca'])
    foro['estado_id'] = clean.transform(
        foro['muni_state'],
        'estado', 'estado_id'
    )
    foro.loc[
        foro['muni_state'].isnull(), 'estado_id'
    ] = foro['state_codetj']
    foro['tribunal_id'] = foro['estado_id']
    n_municipios = (
        foro
        .drop_duplicates(subset=['comarca', 'muni_name', 'estado_id'])
        .groupby(['comarca', 'estado_id'])
        .size()
        .reset_index(name='n_municipios')
    )
    foro = pd.merge(
        foro,
        n_municipios,
        on=['comarca', 'estado_id'],
        how='left'
    )
    foro = (foro
        .rename(columns={'comarca_codetj': 'oooo'})             
        .loc[:, ('oooo', 'comarca', 'tribunal_id', 'estado_id', 'n_municipios')]
        .query('estado_id.notnull() & oooo.notnull()')
        .drop_duplicates(subset=['oooo', 'estado_id'])                 
        .sort_values(['tribunal_id', 'oooo'])
        .reset_index(drop=True)
    )
    foro['comarca_id'] = (
        (foro['estado_id'].astype('str') + foro['comarca'])
        .astype('category')
        .cat.codes + 1
    )
    foro['foro_id'] = foro.index + 1
    return foro
 

def add_comarca_info(comarca):
    info_file = os.path.join(
        misc.get_user_config('external_dropbox_directory'),
        'comarcas/comarca_info.csv',
    )
    info = pd.read_csv(
        info_file,
        usecols= ['state', 'name', 'entrancia', 'juizes']
    ).rename(columns={'name': 'comarca'})
    info['estado_id'] = clean.transform(
        info['state'],
        'estado', 'estado_id'
    )
    info['comarca'] = clean.clean_text(info['comarca'])
    comarca = (
        comarca
        .merge(info, on=['comarca', 'estado_id'], how='left')
        .drop(columns=['state'])
        .drop_duplicates(subset='comarca_id')
    )
    return comarca


foro, comarca = main()
