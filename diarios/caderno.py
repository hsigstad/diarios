import pandas as pd
import os
import glob
import re
import importlib
os.chdir('/home/henrik/Dropbox/brazil/diarios')
import diarios
importlib.reload(diarios)
os.chdir('/home/henrik/Dropbox/brazil/diarios/diarios')


def main():
    indir = diarios.get_user_config(
        'external_local_directory'
    )
    diario_names = os.listdir(
        '{}/diarios'.format(indir)
    )
    func = lambda x: get_cadernos(
        x, indir
    )
    caderno = pd.concat(
        map(func, diario_names),
        sort=True
    )
    caderno = (
        caderno
        .sort_values(['diario', 'caderno'])
        .reset_index(drop=True)
    )
    caderno.index.name = 'caderno_id'
    caderno.index = caderno.index + 1
    caderno.to_csv(
        'data/caderno.csv'
    )
    return caderno


def get_cadernos(diario, indir):
    print(diario)
    if diario == 'MP-SP':
        return pd.DataFrame()
    indir = (
        '{}/diarios/{}'
        .format(indir, diario)
    )
    cadernos = set()
    for _, _, files in os.walk(indir):
        cadernos = cadernos.union(files)
    cadernos = [
        c[:-3] for c in cadernos
        if '.md' in c
    ]
    return pd.DataFrame({
        'diario': [diario]*len(cadernos),
        'caderno': list(cadernos)
    })


caderno = main()
