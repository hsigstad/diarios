import pandas as pd
import os
import glob
import re
os.chdir('/home/henrik/Dropbox/brazil/diarios')
from diarios.misc import get_user_config
os.chdir('/home/henrik/Dropbox/brazil/diarios/diarios')


def main():
    df = pd.read_csv(
        'data/tribunal_manual.csv'
    )
    df['diario_start'] = get_diario_start(
        df.tribunal
    )
    df['diario_end'] = get_diario_end(
        df.tribunal
    )
    df.to_csv('data/tribunal.csv')
    return df

    
def get_diario_start(tribunal):
    return tribunal.apply(
        lambda x: get_date(x, 0)
    )


def get_diario_end(tribunal):
    return tribunal.apply(
        lambda x: get_date(x, -1)
    )


def get_date(diario, loc):
    indir = get_user_config(
        'external_local_directory'
    )
    indir = os.path.join(
        indir, 'diarios', diario
    )
    if not os.path.isdir(indir):
        return ''
    year = get_loc(indir, loc)    
    day = None
    i = 0
    while not day:
        if loc == 0:
            loc2 = loc + i
        else:
            loc2 = loc - i
        month = get_loc(
            os.path.join(indir, year),
            loc2
        )
        day = get_loc(
            os.path.join(indir, year, month),
            loc
        )
        i += 1
    return '{}-{}-{}'.format(year,month,day)


def get_loc(indir, loc):    
    lst = [
        x for x in
        os.listdir(indir)
        if os.path.isdir(os.path.join(indir,x))
    ]
    if len(lst) == 0:
        return None
    else:
        return lst[loc]


df = main()
