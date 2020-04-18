import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from diarios.misc import get_user_config


def query(database, sql, echo=True):
    conn = get_db_engine(
        database,
        echo=echo
    ).connect()
    return pd.read_sql(sql, conn)


def get_db_engine(database, echo=True):
    user = get_user_config('mysql_user')
    pw = get_user_config('mysql_pw')
    host = get_user_config('mysql_host')
    engine = create_engine(
        'mysql+mysqlconnector://{0}:'
        '{1}@{2}/{3}?charset=utf8'
        .format(user, pw, host, database),
        echo=echo
    )
    return engine


def insert(database, table, files, outdir='build/insert'):
    engine = get_db_engine(database, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    engine.execute('SET FOREIGN_KEY_CHECKS=0')
    _truncate_too_long_strings(engine)
    engine.execute('TRUNCATE TABLE {}'.format(table.__tablename__))    
    for infile in files:
        print(infile)
        data = _get_data(infile)
        chunksize = 1000
        nchunks = len(data) // chunksize + 1
        for chunk in range(0, nchunks):
            print('{} of {}'.format(chunk, nchunks))
            session.bulk_insert_mappings(
                table,
                data[chunk*chunksize:(chunk+1)*chunksize]
            )
    session.commit()
    engine.execute('SET FOREIGN_KEY_CHECKS=1')    
    outfile = '{}/{}.txt'.format(
        outdir, table.__tablename__
    )
    with open(outfile, 'a+') as f:
        f.write('Table built!')


def _truncate_too_long_strings(engine):
    engine.execute('SET SESSION sql_mode=""')


def _get_data(infile):
    data = pd.read_csv(infile)
    data = _convert_int_to_float(data)
    if 'id' in data.columns:
        data = data.drop_duplicates(subset='id')
    data = data.where((pd.notnull(data)), None)
    return data.to_dict('records')


def _convert_int_to_float(df):
    for col in df.columns:
        if df[col].dtype == np.int64:
            df[col] = df[col].astype(float)
    return df

    
def head(table, n=5):
    return pd.read_sql('SELECT * FROM {0} LIMIT {1}'.format(table, n), session.bind)


def create_index(
        database, table,
        columns, name,
        index_type=''
    ):
    '''
    Args:
       index_type: 'FULLTEXT' or ''
    '''
    conn = get_db_engine(database).connect()
    cols = ', '.join(columns)
    sql = (
        'CREATE {0} INDEX {1}\n'
        'ON {2} ({3})'
        .format(
            index_type, name, table, cols
        )
    )
    conn.execute(sql)


def order_by(
    database,
    table,
    columns
):
    conn = get_db_engine(database).connect()
    cols = ', '.join(columns)
    sql = (
        'ALTER TABLE {0} ORDER BY {1}'
        .format(table, cols)
    )
    conn.execute(sql)
        

