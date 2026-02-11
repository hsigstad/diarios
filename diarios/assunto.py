import pandas as pd
import numpy as np
import os
os.chdir('/home/henrik/Dropbox/brazil/diarios')
from diarios.misc import get_user_config
from diarios.clean import clean_text
os.chdir('/home/henrik/Dropbox/brazil/diarios/diarios')


def read(infile):
    indir = get_user_config(
        'external_dropbox_directory'
    )
    infile = os.path.join(
        indir, 'tabelas_unificadas',
        'dump', 'CSV', infile
    )
    return pd.read_csv(infile)


assunto = read('assuntos.csv')
item = (
    read('itens.csv')
    .query('tipo_item=="A"')
)


def create_tree(row):
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
            create_tree(r)
            for _, r in
            children.iterrows()
        ]
    }


roots = item.query('cod_item_pai.isnull()')
tree = [
    create_tree(root)
    for _, root in
    roots.iterrows()
]


outfile = 'assunto.org'
    
def to_org(tree, level=1):
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


org = '\n'.join([
    to_org(node)
    for node in tree
])


outfile = 'data/assunto.org'

with open(outfile, 'w') as f:
    f.write(org)
    
    
    
