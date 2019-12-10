import pandas as pd
import numpy as np
import re
import diarios.clean as clean


class CaseParser:
    '''Class to parse court cases'''
    
    def __init__(
        self,
        regexes=[
            '(?P<number>[0-9.\-]{19,30})'
        ],
        regexes_before_split=None,
        cleaners = {
            'classe': clean.clean_classe
        },
        parte='AUTOR:|RÉU:',
        split_parte_on=',|-|;',
        split_text_on=None,        
        id_suffix=None,
        clean_text=clean.clean_diario_text,
        clean_parte=clean.clean_parte,
        clean_number=clean.clean_number,
        clean_last_parte=lambda x: x,
        clean_parte_key=clean.clean_parte_key,
        clean_tipo_parte=clean.clean_tipo_parte,
        max_name_length=100,
        last_name_length=50,
        clean_proc=lambda x: x,
        clean_mov=lambda x: x,
        df_proc_cols=[],
        df_mov_cols=[],
        drop_if_no_number=True,
        parte_levels=['proc_id']
    ):
        self.parte = parte
        self.regexes_before_split = regexes_before_split
        self.regexes = regexes
        self.cleaners = cleaners
        self.split_parte_on = split_parte_on
        self.split_text_on = split_text_on        
        self.id_suffix = id_suffix
        self.clean_text = clean_text
        self.clean_number = clean_number
        self.clean_parte = clean_parte
        self.clean_last_parte = clean_last_parte
        self.clean_parte_key = clean_parte_key
        self.clean_tipo_parte = clean_tipo_parte
        self.max_name_length = max_name_length
        self.last_name_length = last_name_length
        self.clean_proc = clean_proc
        self.clean_mov = clean_mov
        self.df_proc_cols = df_proc_cols
        self.df_mov_cols = df_mov_cols       
        self.drop_if_no_number = drop_if_no_number
        self.parte_levels = parte_levels
        
    def parse(self, df):
        df = self._add_cols_before_split(df)
        df = self._split_text(df)
        df.text = self.clean_text(df.text)
        df = self._add_cols(df)
        df.loc[:, self.cleaners.keys()] = (
            df.transform(self.cleaners)
        )
        if len(df) == 0:
            return
        proc = self._get_proc(df)
        parte = self._get_parte(df)
        mov = self._get_mov(df)
        return proc, parte, mov

    def _add_cols_before_split(self, df):
        if self.regexes_before_split:
            cols = extract_regexes(
                df.text,
                self.regexes_before_split
            )
            df = df.join(cols)
        return df
                
    def _split_text(self, df):
        if self.split_text_on:
            df = split_col(
                df, 'text',
                split_on=self.split_text_on
            ).reset_index()
        return df

    def _add_cols(self, df):
        cols = extract_regexes(
            df.text, self.regexes
        )
        df = df.join(cols)
        if self.drop_if_no_number:
            df = df.query('number.notnull()')
        df['number'] = self.clean_number(
            df.number
        )
        df['proc_id'] = clean.generate_id(
            df.number,
            suffix=self.id_suffix
        )
        df['mov_id'] = df.index
        df['mov_id'] = clean.generate_id(
            df.mov_id,
            suffix=self.id_suffix
        )
        return df       

    def _get_parte(self, df):
        df_id = df.loc[:, self.parte_levels]
        df = extract_keywords(
            df['text'], self.parte,
            max_name_length=self.max_name_length,
            last_name_length=self.last_name_length
        )
        df['lastname'] = self.clean_last_parte(
            df.lastname
        )
        df = self._split_parte(df)
        df['parte'] = self._clean_parte_name(df)
        df['key'] = self.clean_parte_key(
            df.key
        ) 
        df['tipo_parte'] = self.clean_tipo_parte(df.key)
        df['tipo_parte_id'] = clean.transform(
            df.tipo_parte, 'tipo_parte', 'tipo_parte_id'
        )
        df = self._drop_partes(df)
        df = df.join(df_id)
        df = df.drop_duplicates([
            'parte',
            'tipo_parte_id'
        ] + self.parte_levels)
        return df.loc[:, [
            'parte', 'key',
            'tipo_parte_id'
        ] + self.parte_levels]

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
            'name': self.clean_parte, 
            'lastname': self.clean_parte
        })
        return np.where(
            df['name'] == '',
            df['lastname'], df['name']
        )

    def _drop_partes(self, df):
        df = df.query('parte != ""')
        df = df.loc[
            (df.parte.str.len() > 8) |
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
        cols1 = ['proc_id']
        proc = (
            df.loc[:, cols1 + self.df_proc_cols]
            .drop_duplicates('proc_id')
        )
        proc = proc.set_index('proc_id')
        proc = self.clean_proc(proc)            
        return proc

    def _get_mov(self, df):
        cols1 = ['mov_id', 'proc_id', 'text']
        mov = df.loc[:, cols1 + self.df_mov_cols]
        mov = self.clean_mov(mov)
        return mov

    
class DiarioParser(CaseParser):
    
    def __init__(
            self,
            number_types='CNJ',
            df_mov_cols=['tribunal', 'number',
                         'date', 'caderno', 'line'],
            df_proc_cols=['tribunal', 'number', 'classe'],            
            **kwargs
        ):
        super(DiarioParser, self).__init__(
            clean_proc=lambda x: clean_diario_proc(x, number_types),
            clean_mov=clean_diario_mov,
            df_mov_cols=df_mov_cols,
            df_proc_cols=df_proc_cols,
            **kwargs
        )


def clean_diario_proc(proc, number_types=['CNJ']):
    proc['tribunal_id'] = clean.transform(
        proc['tribunal'],
        'tribunal', 'tribunal_id'
    )
    proc['filingyear'] = clean.get_filing_year(
        proc.number,
        types=number_types
    )
    proc['comarca_id'] = clean.get_comarca_id(
        proc.number
    )
    return proc


def clean_diario_mov(mov):        
    mov['caderno_id'] = clean.get_caderno_id(
        mov['tribunal'], mov['caderno']
    )
    return mov
        

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
    tribunal = re.match('.*?/', text).group(0)
    df = (
        pd.Series(text.split(tribunal))
        .str.replace(';', '')                
        .str.replace(r'^([0-9]{4}/[0-9]{2}/[0-9]{2})/', r'\1;', n=3)
        .str.replace(r'\.md', r';', n=1)
        .str.replace(r'(-|:)([0-9]+)(-|:)', r'\2;', n=1)
        .str.split(';', expand=True)
    )
    df.columns = ['date', 'caderno', 'line', 'text']
    df['tribunal'] = tribunal[:-1]
    df['date'] = df['date'].str.replace('/', '-')
    return df.query('line.notnull()')


def extract_regexes(text, regexes, flags=0):
    func = lambda x: text.str.extract(x, flags=flags)
    return pd.concat(
        map(func, regexes),
        axis=1
    )


def extract_keywords(
        text, keywords,
        max_name_length=100,
        last_name_length=50
    ):
    regex = get_keyword_regex(
        keywords,
        max_name_length,
        last_name_length        
    )
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
    last_name = '.{{0,{0}}}'.format(
        last_name_length
    )
    #The ?s makes sure . includes newline    
    regex = (
        '(?s)(?P<key>{0})'
        '((?P<name>{1})|(?P<lastname>{2}))'
    ).format(keyword, name, last_name)
    return regex


def inspect(
        proc, parte, mov,
        tp='parte'
    ):
    mov = (
        mov
        .merge(proc.reset_index().loc[:, 'proc_id'], on='proc_id')
        .merge(parte.loc[:, 'proc_id'], on='proc_id')
        .drop_duplicates('proc_id')
    )
    ex = mov.sample().iloc[0]
    proc_id = ex.proc_id
    print(ex['text'])
    prt = (
        parte
        .query('proc_id == {}'.format(proc_id))
        .loc[:, ('parte', 'key')]
    )
    prc = (
        proc
        .query('proc_id == {}'.format(proc_id))
        .iloc[0]
    )
    if tp=='parte':
        print(prt)                
    if tp=='proc':
        print(prc)
    if tp=='mov':
        print(ex)
    if tp=='all':
        print(prt)
        print(ex)
        print(prc)
