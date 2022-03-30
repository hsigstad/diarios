import pandas as pd
from time import time
import sqlite3
import os
from re import sub
from diarios.misc import get_user_config
from sqlalchemy import create_engine


def query(database, sql, flavor='sqlite3', echo=True):
    if type(database) == str:
        conn = connect(database, flavor, echo=echo)
    if type(database) == list:
        conn = sqlite3.connect(database[0])
        c = conn.cursor()
        for d in database[1:]:
            name = sub('\..*', '', os.path.basename(d))
            c.execute("ATTACH '{}' AS {}".format(d, name))
    return pd.read_sql(sql, conn)

    return pd.read_csv(infile)


def insert(database,
           table,
           files,
           columns=None,
           primary=None,
           echo=False,
           index=False,
           fts5=False,
           flavor='sqlite3',
           chunksize=100000,
           dtype_csv=None,
           **kwargs):
    conn = connect(database, flavor, echo=echo)
    if_exists = 'replace'
    if fts5:
        conn.execute("DROP TABLE IF EXISTS {};".format(table))
        conn.execute("CREATE VIRTUAL TABLE {} USING FTS5 ({});".format(
            table, ','.join(columns)))
        if_exists = 'append'
    for infile in files:
        print(infile)
        df = pd.read_csv(infile, dtype=dtype_csv, lineterminator='\n')
        for c in columns:
            if c not in df.columns:
                df[c] = pd.NA
        df = df.loc[:, columns]
        t0 = time()
        df.to_sql(table, conn, if_exists=if_exists, index=index, **kwargs)
        print('Duration:', round(time() - t0))
        if_exists = 'append'


def create_index(
    database,
    table,
    columns,
    name,
    unique=False,
    flavor='sqlite3',
    fulltext=False,
):
    if unique:
        pre = 'UNIQUE'
    elif fulltext:
        pre = 'FULLTEXT'
    else:
        pre = ''
    conn = connect(database, flavor)
    cols = ', '.join(columns)
    sql = 'DROP INDEX {} ON {}'.format(name, table)
    try:
        conn.execute(sql)
    except:
        pass
    sql = 'CREATE {} INDEX {} ON {} ({})'.format(pre, name, table, cols)
    conn.execute(sql)


def connect(database, flavor, **kwargs):
    if flavor == 'sqlite3':
        conn = sqlite3.connect(database)
    if flavor == 'mysql':
        conn = get_db_engine(database, **kwargs)
    return conn


def get_db_engine(database, echo=True):
    user = get_user_config('mysql_user')
    pw = get_user_config('mysql_pw')
    host = get_user_config('mysql_host')
    engine = create_engine('mysql+mysqlconnector://{0}:'
                           '{1}@{2}/{3}?charset=utf8'.format(
                               user, pw, host, database),
                           echo=echo)
    return engine
