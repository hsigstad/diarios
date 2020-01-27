import pandas as pd
import numpy as np
import random
from .politica import get_district
from .politica import get_office_type
from copy import copy

random.seed(42)

def is_close(
        df, only_two=False
    ):
    cols = [
        'year', 'office',
        'round', 'votes',
        'margin', 'municipio_id',
        'estado'
    ]
    if not all(c in df.columns for c in cols):
        raise Exception(
            'df needs cols:', cols
        )
    df_copy = copy(df)
    df_copy['group'] = _get_group(df_copy)
    df_copy = _rank_candidates(df_copy)
    df_copy = _center_rank(df_copy)
    df_copy['close'] = _is_close(
        df_copy, only_two
    )
    df_copy = _drop_duplicates(df_copy)
    if only_two:
        df_copy = _balance_close(
            df_copy, 'close', n=2
        )
    else:
        df_copy = _balance_close(
            df_copy, 'close'
        )
    return df_copy.close

def _get_group(df):
    # df must contain:
    # office, year, round,
    # municipio_id, estado
    office_type = get_office_type(
        df.office
    )
    df['temp'] = np.where(
        office_type == 'pr',
        df.coalition,
        ''
    )
    df['district'] = get_district(df)
    group = df.groupby([
        'year', 'district',
        'office', 'round', 'temp'
    ]).ngroup()
    df.drop(
        columns=['temp', 'district'],
        inplace=True
    )
    return group

def _rank_candidates(df):
    # to make sure to randomly
    # drop one in the case of ties
    # (need to be used with method='first'
    # which preserves order of ties):
    df = df.sample(frac=1)
    # to rank elected higher when equal
    # number of votes:
    df['votes_elected'] = (
        df.votes + df.electeddummy
    )
    df['rank'] = (
        df
        .groupby('group')
        ['votes_elected']
        .rank(ascending=True,
              method='first')
    )
    df = df.drop('votes_elected', 1)
    return df


def _center_rank(df):
    elected = df.electeddummy==1
    df.loc[elected, 'erank'] = df['rank']
    min_elected_rank = (
        df.groupby('group')
        ['erank']
        .transform('min')
    )
    df['crank'] = (
        df['rank'] - min_elected_rank
    )
    df.loc[elected, 'crank'] += 1
    df = df.drop('erank', 1)
    return df


def _is_close(df, only_two=False):
    max_rank = (
        df.groupby('group')
        ['crank'].transform('max')
    )
    min_rank = (
        df.groupby('group')
        ['crank'].transform('min')
    )
    close = (
        (df.crank <= -min_rank) &
        (df.crank >= -max_rank)
    )*1
    if only_two:
        close = (
            (close == 1) &
            df.crank.isin([-1,1])
        )*1
    return close


def _balance_close(df, var, n=None):
    df['eclose'] = df[var]*df.electeddummy
    df['nclose'] = df[var]*(1-df.electeddummy)
    out = (
        df.groupby('group')
        [['eclose', 'nclose']]
        .transform('sum')
    )
    df['emargin'] = (
        df.margin*
        df.electeddummy*df[var]
    )
    df['nmargin'] = (
        df.margin*
        (1-df.electeddummy)*df[var]
    )
    emargin = (
        df.groupby('group')
        ['emargin']
        .transform('min')
    )
    nmargin = (
        df.groupby('group')
        ['nmargin']
        .transform('max')
    )
    df = df.drop(columns=[
        'eclose', 'nclose',
        'emargin', 'nmargin'
    ])
    cond = (
        ((out.eclose != out.nclose) |
        (emargin != -nmargin)) &
        (df[var] == 1)
    )
    print(
        sum(cond), var,
        'not balanced, setting to 0'
    )
    if sum(cond) > 0:
        print('Example 1:')
        print(df.loc[cond].sample().iloc[0])
        print('Example 2:')
        print(df.loc[cond].sample().iloc[0])        
    df.loc[cond, var] = 0
    if n:
        nclose = (
            df.groupby('group')
            [var].transform('sum')
        )
        notn = (
            (nclose != n) &
            (nclose != 0)
        )
        print(
            sum(notn), var,
            'not', n, 'setting to 0'
        )
        if sum(notn) > 0:
            print('Example 1:')
            print(df.loc[notn].sample().iloc[0])
            print('Example 2:')
            print(df.loc[notn].sample().iloc[0])        
        df.loc[notn, var] = 0
    return df


def _drop_duplicates(df):
    # Not sure why the same candidate
    # sometimes appears more than one time
    # running for the same office
    cols = [
        'cpf', 'year',
        'municipio_id',
        'office', 'round'
    ]
    duplicated = df.duplicated(cols)
    print(
        sum(duplicated),
        "duplicates.",
        'Dropping 1'
    )
    return df.drop_duplicates(cols)
