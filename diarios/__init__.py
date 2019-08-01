from .parse import *
from .extract import *
from .clean import *
import diarios.regexes


class Parser:
    def __init__(
        self,
        number = diarios.regexes.number,
        classe = diarios.regexes.classe,
        keywords = ['AUTOR:', 'RÉU:']
    ):
        self.number = number
        self.classe = classe
        self.keywords = keywords

        
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
            'number': clean_number,
            'classe': clean_classe
        })
        return (
            df
            .join(df2)
            .query('number.notnull()')
        )


    def _get_regexes(self):
        return {
            'number': self.number,
            'classe': self.classe
        }        
    

    def _get_keyword_df(self, text):
        df = extract_keywords(text, self.keywords)
        df = df.transform({
            'name': clean_name,
            'key': clean_text
        })
        return df.query('name.notnull()')


def _clean_text(text):
    return clean_text(
        text,
        lower=False,
        drop=None,
        accents=True,
        links=False,
        newline=False
    )
    

def _get_proc(df):
    proc = (
        df.loc[:, ('number', 'classe')]
        .drop_duplicates('number')
    )
    proc['filingyear'] = get_filing_year(proc['number'])
    proc['comarca'] = get_comarca(proc['number'])
    return proc

    
def _get_mov(df):
    mov = df.loc[:, (
        'number', 'date',
        'caderno', 'line',
        'text'
    )]
    return mov

    
def _get_parte(regex_df, kw_df):
    parte = (
        kw_df
        .join(regex_df.loc[:, ('number')])
        .drop_duplicates()
    )
    parte['tipo_parte'] = clean_tipo_parte(
        parte['key']
    )
    return parte
