import pandas as pd


def get_text(infile, split_regex, nchar=None):
    with open(infile, 'r') as f:
        text = f.read()
    if nchar:
        text = text[:nchar]
    text = pd.Series(
        text.split(split_regex)
    )
    return text


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
    key = "|".join(keywords)
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
    return text.str.extractall(regex)

