import pandas as pd
import numpy as np
from .clean import get_data


def split_coalition(coalition, name='party'):
    return (coalition.str.split(
        ' / ', expand=True).stack().rename(name).droplevel(-1).str.strip())


def get_district(df):
    district = np.where(df.office.isin(['prefeito', 'vereador']),
                        df.municipio_id, df.estado)
    return district


def get_office_type(office):
    mapping = {
        'vereador': 'pr',
        'deputado estadual': 'pr',
        'deputado federal': 'pr',
        'prefeito': 'majority',
        'governador': 'majority',
        'presidente': 'majority',
        'senador': 'majority'
    }
    return office.map(mapping)


def get_election_date(year, rnd=1):
    dates = get_data('eleicao.csv')
    dates['electiondate'] = pd.to_datetime(dates.electiondate)
    dt = pd.DataFrame({'year': year, 'round': rnd, 'index': year.index})
    dt = pd.merge(dt, dates, on=['year', 'round'], how='left')
    dt.index = dt['index']
    return dt.electiondate


def calculate_name_log_likelihood(names):
    name_ll = get_data('name_ll.csv')
    likelihood_if_not_in_list = 2 * min(name_ll['ll'])
    names = names.str.split(expand=True)
    names['index'] = names.index
    names = (pd.melt(names, id_vars='index',
                     value_name='name').query('name.notnull()').loc[:, (
                         'index', 'name')].merge(
                             name_ll, on='name',
                             how='left').fillna(likelihood_if_not_in_list))
    ll = pd.pivot_table(names, index='index', values=['ll'], aggfunc='sum')
    return ll
