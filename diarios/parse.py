import pandas as pd
import numpy as np
import re
import diarios.clean as clean


class Parser:
    def __init__(
        self,
        number = '[0-9.\-]{20,30}',
        classe = 'AÇÃO.{5,50}?(?=\*|-)',
        keywords = ['AUTOR:', 'RÉU:'],
        split_parte_on = ',|-',
        id_suffix=None
    ):
        self.number = number
        self.classe = classe
        self.keywords = keywords
        self.split_parte_on=split_parte_on
        self.id_suffix = id_suffix

        
    def parse(self, df):
        df['text'] = _clean_text(df['text'])
        regex_df = self._add_regex_columns(df)
        kw_df = self._get_keyword_df(df['text'])
        proc = _get_proc(regex_df)
        parte = _get_parte(regex_df, kw_df)
        mov = _get_mov(regex_df)
        return proc, parte, mov


    def _add_regex_columns(self, df):
        regexes = self._get_regexes()
        df2 = extract_regexes(
            df['text'], regexes
        )
        df2 = df2.transform({
            'number': clean.clean_number,
            'classe': clean.clean_classe
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
            'number': self.number,
            'classe': self.classe
        }
    

    def _get_keyword_df(self, text):
        df = extract_keywords(text, self.keywords)
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
        return (
            df
            .drop(columns='lastname')
            .query('name != ""')
        )


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
    

def _get_proc(df):
    proc = (
        df.loc[:, ('diario', 'proc_id', 'number', 'classe')]
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

    
def _get_mov(df):
    mov = df.loc[:, (
        'diario', 'proc_id', 'number',
        'date', 'caderno', 'line', 'text'
    )]
    mov['caderno_id'] = clean.get_caderno_id(
        mov['diario'], mov['caderno']
    )
    return mov

    
def _get_parte(regex_df, kw_df):
    kw_df = kw_df.loc[
        (kw_df.name.str.len() > 10) |
        (kw_df.name == 'mp')
    ]    
    parte = (
        kw_df
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


def get_keyword_regex(
        keywords,
        max_name_length=100,
        last_name_length=50
    ):
    key = '|'.join(keywords)
    name = '.{{0,{0}}}?(?={1})'.format(
        max_name_length, key
    )
    last_name = '.{{{0}}}'.format(
        last_name_length
    )
    #The ?s makes sure . includes newline    
    regex = (
        '(?s)(?P<key>{0})'
        '((?P<name>{1})|(?P<lastname>{2}))'
    ).format(key, name, last_name)
    return regex


def extract_keywords(text, keywords):
    regex = get_keyword_regex(keywords)
    df = text.str.extractall(regex)
    df.index = df.index.droplevel(1)
    return df



