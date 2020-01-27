import pandas as pd
import numpy as np
from .clean import get_data

def split_coalition(coalition, name='party'):
    return (
        coalition
        .str.split(' / ', expand=True)
        .stack()
        .rename(name)
        .droplevel(-1)
        .str.strip()
    )


def get_district(df):
    district = np.where(
        df.office.isin([
            'prefeito',
            'vereador'
        ]),
        df.municipio_id,
        df.estado
    )
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
    dt = pd.DataFrame({
        'year': year,
        'round': rnd,
        'index': year.index
    })
    dt = pd.merge(
        dt, dates,
        on=['year', 'round'],
        how='left'
    )
    dt.index = dt['index']
    return dt.electiondate
        
        
    
