"""Generate assunto.org by building a tree of legal subject categories from CNJ data."""

import os
from typing import Any, Dict, List

import pandas as pd
from diarios.clean import clean_text



def read(infile: str) -> pd.DataFrame:
    """Read a CSV from the tabelas_unificadas dump directory.

    Args:
        infile: Filename relative to the dump/CSV directory.

    Returns:
        DataFrame from the CSV file.
    """
    indir = os.environ["EXTERNAL_DROPBOX_DIR"]
    infile = os.path.join(
        indir, 'tabelas_unificadas',
        'dump', 'CSV', infile
    )
    return pd.read_csv(infile)


def create_tree(row: pd.Series, assunto: pd.DataFrame, item: pd.DataFrame) -> Dict[str, Any]:
    """Recursively build a tree node for a legal subject category.

    Args:
        row: A row from the itens DataFrame with cod_item and nome columns.
        assunto: DataFrame of assunto records.
        item: DataFrame of item records filtered to type "A".

    Returns:
        Dict with name, dispositivo, artigo, glossario, and children keys.
    """
    children = item.query(
        'cod_item_pai=={}'
        .format(row.cod_item)
    )
    info = assunto.query(
        'cod_assunto=={}'
        .format(row.cod_item)
    )
    if len(info) == 1:
        info = info.iloc[0]
    else:
        info = {
            'dispositivo_legal': '',
            'artigo': '',
            'glossario': ''
        }
    return {
        'name': row.nome,
        'dispositivo': info['dispositivo_legal'],
        'artigo': info['artigo'],
        'glossario': info['glossario'],
        'children': [
            create_tree(r, assunto, item)
            for _, r in
            children.iterrows()
        ]
    }


def to_org(tree: Dict[str, Any], level: int = 1) -> str:
    """Convert a subject tree node to Org-mode formatted text.

    Args:
        tree: Tree node dict with name, dispositivo, artigo, glossario, children.
        level: Current heading depth for Org-mode stars.

    Returns:
        Org-mode formatted string for this node and its children.
    """
    children = [
        to_org(child, level=level+1)
        for child in tree['children']
    ]
    children = '\n\n'.join(children)
    info = ''
    for v in ['dispositivo', 'artigo', 'glossario']:
        if isinstance(tree[v], str):
            info = '{}\n- {}'.format(
                info, tree[v]
            )
    txt = '{} {}\n{}\n{}\n'.format(
        '*'*level,
        tree['name'],
        info,
        children
    )
    return txt


if __name__ == '__main__':
    assunto = read('assuntos.csv')
    item = (
        read('itens.csv')
        .query('tipo_item=="A"')
    )

    roots = item.query('cod_item_pai.isnull()')
    tree = [
        create_tree(root, assunto, item)
        for _, root in
        roots.iterrows()
    ]

    org = '\n'.join([
        to_org(node)
        for node in tree
    ])

    outfile = 'data/assunto.org'

    with open(outfile, 'w') as f:
        f.write(org)
