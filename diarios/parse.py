import pandas as pd
import re

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
    return df.query('line.notnull()')

    # text = clean_text(
    #     text,
    #     lower=False,
    #     drop=None,
    #     accents=True,
    #     links=False,
    #     newline=False
    # )



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
        '(?P<name>{1}|{2})'
    ).format(key, name, last_name)
    return regex


def extract_keywords(text, keywords):
    regex = get_keyword_regex(keywords)
    df = text.str.extractall(regex)
    df.index = df.index.droplevel(1)
    return df



