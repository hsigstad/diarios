import pandas as pd
import numpy as np
import re
import diarios.clean as clean
from sqlalchemy import Column, String


class DiarioVar:
    def __init__(
        self, name, regex,
        table='proc',
        cleaner=clean.clean_text
    ):
        self.name = name
        self.table = table
        self.regex = regex
        self.cleaner = cleaner
        
    def __repr__(self):
       return 'DiarioVar({})'.format(name)


class Parser:
    '''Class to parse diarios extracts'''
    
    def __init__(
        self,
        columns=[DiarioVar('number', '[0-9.\-]{20,30}')],
        parte='AUTOR:|RÉU:',
        split_parte_on=',|-',
        split_text_on=None,        
        id_suffix=None,
        text_cleaner=clean.clean_diario_text,
        parte_cleaner=clean.clean_parte,
        last_parte_cleaner=clean.clean_last_parte,
        parte_key_cleaner=clean.clean_parte_key,
        tipo_parte_cleaner=clean.clean_tipo_parte     
    ):
        self.parte = parte
        self.columns = columns
        self.split_parte_on = split_parte_on
        self.split_text_on = split_text_on        
        self.id_suffix = id_suffix
        self.text_cleaner = text_cleaner
        self.parte_cleaner = parte_cleaner
        self.last_parte_cleaner = last_parte_cleaner
        self.parte_key_cleaner = parte_key_cleaner
        self.tipo_parte_cleaner = tipo_parte_cleaner
        
    def parse(self, df):
        df = self._split_text(df)        
        df.text = self.text_cleaner(df.text)
        df = self._add_cols(df)
        proc = self._get_proc(df)
        parte = self._get_parte(df)
        mov = self._get_mov(df)
        return proc, parte, mov
    
    def _split_text(self, df):
        if self.split_text_on:
            df = split_col(
                df, 'text',
                split_on=self.split_text_on
            ).reset_index()
        return df

    def _add_cols(self, df):
        df = df.join(
            self._extract_cols(df.text)
        )
        df = df.query('number.notnull()')
        df['proc_id'] = clean.generate_id(
            df.number,
            suffix=self.id_suffix
        )
        return df       

    def _extract_cols(self, text):
        regexes = {
            c.name: c.regex
            for c in self.columns
        }
        cleaners = {
            c.name: c.cleaner
            for c in self.columns
        }
        return extract_regexes(
            text, regexes
        ).transform(cleaners)

    def _get_parte(self, df):
        proc_id = df['proc_id']
        df = extract_keywords(
            df['text'], self.parte
        )
        df = self._split_parte(df)
        df['parte'] = self._clean_parte_name(df)
        df['key'] = self.parte_key_cleaner(df.key) 
        df['tipo_parte'] = self.tipo_parte_cleaner(df.key)
        df['tipo_parte_id'] = clean.transform(
            df.tipo_parte, 'tipo_parte', 'tipo_parte_id'
        )
        df = self._drop_partes(df)
        df = df.join(proc_id)
        return df.loc[:, (
            'proc_id', 'parte',
            'key', 'tipo_parte_id'
        )]

    def _split_parte(self, df):
        df = split_col(
            df, 'name',
            split_on=self.split_parte_on
        )
        df = split_col(
            df, 'lastname',
            split_on=self.split_parte_on
        )
        return df
    
    def _clean_parte_name(self, df):    
        df = df.transform({
            'name': self.parte_cleaner, 
            'lastname': self.last_parte_cleaner
        })
        return np.where(
            df['name'] == '',
            df['lastname'], df['name']
        )

    def _drop_partes(self, df):
        df = df.query('parte != ""')
        df = df.loc[
            (df.parte.str.len() > 10) |
            (df.parte == 'mp')
        ]
        return df    
    
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


number = DiarioVar(
    name='number',
    regex='[0-9.\-]{20,30}',
    cleaner=clean.clean_number
)

classe = DiarioVar(
    name='classe',
    regex='AÇÃO.{5,50}?(?=\*|-)',
    cleaner=clean.clean_classe
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
