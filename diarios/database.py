import pandas as pd
from time import time
import sqlite3


def query(database, sql):
    conn = sqlite3.connect(database)
    return pd.read_sql(sql, conn)


def insert(database,
           table,
           files,
           columns,
           primary=None,
           chunksize=1000,
           index=False,
           fts5=False,
           read_csv=pd.read_csv,
           **kwargs):
    conn = sqlite3.connect(database)
    if fts5:
        conn.execute("DROP TABLE IF EXISTS {};".format(table))
        conn.execute("CREATE VIRTUAL TABLE {} USING FTS5 ({});".format(
            table, ','.join(columns)))
        if_exists = 'append'
    else:
        if_exists = 'replace'
    for infile in files:
        print(infile)
        df = pd.read_csv(infile)
        for c in columns:
            if c not in df.columns:
                df[c] = pd.NA
        df = df.loc[:, columns]
        t0 = time()
        df.to_sql(table, conn, if_exists=if_exists, index=index, **kwargs)
        print('Duration:', round(time() - t0))
        if_exists = 'append'


def create_index(database, table, columns, name, unique=False):
    if unique:
        unique = 'UNIQUE'
    else:
        unique = ''
    conn = sqlite3.connect(database)
    cols = ', '.join(columns)
    sql = ('CREATE {0} INDEX {1} '
           'ON {2} ({3})'.format(unique, name, table, cols))
    conn.execute(sql)
