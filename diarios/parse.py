import pandas as pd
import numpy as np
import re
import diarios.clean as clean
from sqlalchemy import Column, String

class DiarioVar:
    def __init__(
        self, name,
        table, regex,
        cleaner,
        keyword=False
    ):
        self.name = name
        self.table = table
        self.regex = regex
        self.cleaner = cleaner
        self.keyword = keyword
    def __repr__(self):
       return 'DiarioVar({})'.format(name)


number = DiarioVar(
    name='number',
    table='proc',
    regex='[0-9.\-]{20,30}',
    cleaner=clean.clean_number
)

classe = DiarioVar(
    name='classe',
    table='proc',
    regex='AÇÃO.{5,50}?(?=\*|-)',
    cleaner=clean.clean_classe
)



class Parser:
    def __init__(
        self,
        columns = [number, classe],
        parte = 'AUTOR:|RÉU:',
        split_parte_on = ',|-',
        id_suffix=None
    ):
        self.parte = parte        
        self.columns = self._update_regexes(columns)
        self.split_parte_on = split_parte_on
        self.id_suffix = id_suffix


        
    def parse(self, df):
        df['text'] = _clean_text(df['text'])
        regex_df = self._add_columns(df)
        proc = self._get_proc(regex_df)
        parte = self._get_parte(df['text'], regex_df)
        mov = self._get_mov(regex_df)
        return proc, parte, mov


    def _add_columns(self, df):
        regexes = self._get_regexes()
        df2 = extract_regexes(
            df['text'], regexes
        )
        df2 = df2.transform({
            c.name: c.cleaner
            for c in self.columns
        })
        df = (
            df
            .join(df2)
            .query('number.notnull()')
        )
        df['proc_id'] = clean.generate_id(
            df['number'],
            suffix=self.id_suffix
        )
        return df


    def _get_regexes(self):
        return {
            c.name: c.regex
            for c in self.columns
        }
    

    def _get_parte(self, text, regex_df):
        df = extract_keywords(
            text, self.parte
        )
        df = split_col(
            df, 'name',
            split_on=self.split_parte_on
        )
        df = split_col(
            df, 'lastname',
            split_on=self.split_parte_on
        )
        df = df.transform({
            'name': clean.clean_parte,
            'lastname': clean.clean_last_parte,            
            'key': clean.clean_text
        })
        df['name'] = np.where(
            df['name'] == '',
            df['lastname'], df['name']
        )
        df = (
            df
            .drop(columns='lastname')
            .query('name != ""')
        )
        df = df.loc[
            (df.name.str.len() > 10) |
            (df.name == 'mp')
        ]    
        parte = (
            df
            .join(regex_df.loc[:, ('proc_id', 'number')])
            .drop_duplicates()
            .rename(columns={'name': 'parte'})
        )
        parte['tipo_parte'] = clean.clean_tipo_parte(
            parte['key']
        )
        parte['tipo_parte_id'] = clean.transform(
            parte['tipo_parte'],
            'tipo_parte', 'tipo_parte_id'
        )
        return parte
            

    def _get_keywords(self):
        regex = [
            c.regex for c in self.keyword_cols
        ]
        if type(self.parte_regex) == str:
            regex += [self.parte_regex]
        else:
            regex += self.parte_regex
        return regex

    def _get_proc(self, df):
        cols1 = ['proc_id', 'diario']
        cols2 = [
            c.name for c in self.columns
            if c.table == 'proc'
        ]
        proc = (
            df.loc[:, cols1 + cols2]
            .drop_duplicates('proc_id')
        )
        proc['tribunal_id'] = clean.transform(
            proc['diario'],
            'diario', 'tribunal_id'
        )
        proc['filingyear'] = clean.get_filing_year(
            proc['number']
        )
        proc['comarca_id'] = clean.get_comarca_id(
            proc['number']
        )
        return proc


    def _get_mov(self, df):
        cols1 = [
            'diario', 'proc_id', 'date',
            'caderno', 'line', 'text'
        ]
        cols2 = [
            c.name for c in self.columns
            if c.table == 'mov'
        ]    
        mov = df.loc[:, cols1 + cols2]
        mov['caderno_id'] = clean.get_caderno_id(
            mov['diario'], mov['caderno']
        )
        return mov
    
    def _update_regexes(self, keyword_cols):
        any_keyword = '|'.join([
            c.regex for c in keyword_cols
            if c.keyword
        ])
        any_keyword = '{}|{}'.format(any_keyword, self.parte)
        func = lambda x: _update(x, any_keyword)
        return [c for c in map(func, keyword_cols)]


def split_col(df, name_col, split_on=',|-'):
    df = df.reset_index()
    df.index.name = 'temp'
    names = (
        df[name_col]
        .fillna('')
        .str.split(split_on, expand=True)
        .stack()
    )
    names.name = name_col        
    df = df.drop(columns=name_col)
    return df.join(names).set_index('index')
    

def _clean_text(text):
    return clean.clean_text(
        text,
        lower=False,
        drop=None,
        accents=True,
        links=False,
        newline=False
    )
    

def parse_diario_extract(
        infile, nchar=None
    ):
    with open(infile, 'r') as f:
        text = f.read()
    if nchar:
        text = text[:nchar]
    diario = re.match('.*?/', text).group(0)
    df = (
        pd.Series(text.split(diario))
        .str.replace(';', '')                
        .str.replace(r'([0-9]{4}/[0-9]{2}/[0-9]{2})/', r'\1;', n=3)
        .str.replace(r'\.md', r';', n=1)
        .str.replace(r'(-|:)([0-9]+)(-|:)', r'\2;', n=1)
        .str.split(';', expand=True)
    )
    df.columns = ['date', 'caderno', 'line', 'text']
    df['diario'] = diario[:-1]
    df['date'] = df['date'].str.replace('/', '-')
    return df.query('line.notnull()')


def extract_regexes(text, regexes):
    if type(regexes) == dict:
        regexes = [
            '(?P<{}>{})'.format(k, v)
            for k, v in regexes.items()
        ]
    return pd.concat(
        map(text.str.extract, regexes),
        axis=1
    )


def extract_keywords(text, keywords):
    regex = get_keyword_regex(keywords)
    df = text.str.extractall(regex)
    df.index = df.index.droplevel(1)
    return df
        

def get_keyword_regex(
        keyword,
        max_name_length=100,
        last_name_length=50
    ):
    '''
    Args:
       keywords: list of keywords or regex
    '''
    if type(keyword) == list:
        keyword = '|'.join(keyword)
    name = '.{{0,{0}}}?(?={1})'.format(
        max_name_length, keyword
    )
    last_name = '.{{{0}}}'.format(
        last_name_length
    )
    #The ?s makes sure . includes newline    
    regex = (
        '(?s)(?P<key>{0})'
        '((?P<name>{1})|(?P<lastname>{2}))'
    ).format(keyword, name, last_name)
    return regex

    
def _update(col, any_keyword):
    if col.keyword:
        col.regex = '(?<={}).*?(?={})'.format(
            col.regex, any_keyword
        )
    return col
        




