"""Generate caderno.csv from local diary data directories."""

import os

import pandas as pd
import path


def main() -> pd.DataFrame:
    """Scan diary directories and write caderno.csv.

    Returns:
        DataFrame of cadernos indexed by caderno_id.
    """
    indir = path.local_data_dir
    diario_names = os.listdir('{}/diarios'.format(indir))
    func = lambda x: get_cadernos(x, indir)
    caderno = pd.concat(map(func, diario_names), sort=True)
    caderno = (caderno.sort_values(['diario',
                                    'caderno']).reset_index(drop=True))
    caderno.index.name = 'caderno_id'
    caderno.index = caderno.index + 1
    caderno.to_csv('diarios/data/caderno.csv')
    return caderno


def get_cadernos(diario: str, indir: str) -> pd.DataFrame:
    """Extract caderno names from a single diary directory.

    Args:
        diario: Name of the diary directory.
        indir: Root data directory path.

    Returns:
        DataFrame with diario and caderno columns.
    """
    print(diario)
    if diario == 'MP-SP':
        return pd.DataFrame()
    indir = ('{}/diarios/{}'.format(indir, diario))
    cadernos = set()
    for _, _, files in os.walk(indir):
        cadernos = cadernos.union(files)
    cadernos = [c[:-3] for c in cadernos if '.md' in c]
    return pd.DataFrame({
        'diario': [diario] * len(cadernos),
        'caderno': list(cadernos)
    })


caderno = main()
