import pandas as pd
import numpy as np
import glob
from unidecode import unidecode
import os



def clean_comarca(comarca):
    comarca = clean_text(comarca)
    comarca = comarca.str.replace(
        'comarca de', ''
    ).str.strip()
    return comarca


def clean_vara(vara):
    vara = clean_text(vara, drop='^a-z0-9 ')
    return vara


def clean_valor(valores):
    return (
        valores
        .str.replace('.', '')
        .str.replace(',', '.')
    )


def clean_date(dates):
    return (
        dates
        .fillna('')
        .astype(str)
        .str.replace('/', '-')
        .str.extract('([0-9]{4}-[0-9]{2}-[0-9]{2})')
    )


def clean_parte(partes):
    partes = (
        partes
        .str.replace('[^ ]+:.*', '')
    )
    partes = clean_text(partes)
    mp_regex = (
        'ministerio publico|'
        'justica publica'
    )
    partes.loc[
        partes.str.contains(mp_regex)
    ] = 'mp'
    partes = (
        partes
        .str.replace('(^| )dra? ', '')
        .str.replace('^(os?|as?|s) ', '')        
        .str.replace(' e outro.*', '')
        .str.strip()
    )
    return partes


def clean_last_parte(partes):
    partes = clean_parte(partes)
    words = [
        'visto', 'sentenca',
        'despach', 'decisao',
        'protocol', 'relat',
        'recebo', 'isto',
        'ante ', 'defiro',
        'etc', 'intim',
        'posto', 'dou ',
        'conforme ', 'sobre ',
        'com ', 'mainfest',
        'tratase', 'dispoe',
        'provimento', 'designa',
        'tendo ', 'pelo ', 'ese '
    ]
    regex = ' {}.*'.format(
        '|'.join(words)
    )
    partes = partes.str.replace(regex, '')
    return partes


def clean_line(lines):
    return pd.to_numeric(lines, errors='coerce')


def clean_classe(classes):
    classes = clean_text(classes)
    mapping = {
        'improb': 'ACIA',
        'popular': 'APop',
        'publica': 'ACP',
        'agravo de instrumento': 'AI',
        'apelacao': 'Ap',
        'procedimento ordinario': 'ProOrd',
        'procedimento sumario': 'ProSum'
    }
    return map_regex(classes, mapping)


def clean_tipo_parte(keywords):
    mapping = {
        'interessado': 'third party',        
        'autor|ente$|ante$|reqte|exeqte': 'plaintiff',
        'lit at|ativ': 'plaintiff',
        '^-$|^\*\*$': 'plaintiff',
        'promotor': 'plaintiff',
        'adv|dr|repr': 'lawyer', # has to be before defendant!
        'reu|^res?$|parte re|do$|da$': 'defendant',
        'reqd|exectd': 'defendant',
        '^x$': 'defendant',
        'passiv|lit pas': 'defendant',
        'paciente': 'paciente'
    }
    return map_regex(keywords, mapping)
    

def clean_decision(decisions, grau='1'):
    decisions = clean_text(decisions)
    mapping = {
        'julgo .{0,5}par.{0,10}procedente': 'parcialmente procedente',
        'julgo.{0,5} procedentes? ((parcialmente)|(em parte))': 'parcialmente procedente',  
        'julgo.{0,5} procedente': 'procedente',
        'julgo.{0,5} improcedente': 'improcedente',                
        'indefiro.{0,20}desbloqueio': 'indefiro desbloqueio',
        '^defiro.{0,20}desbloqueio': 'defiro desbloqueio',        
        'recebo.{0,20}( acao|inicial)': 'recebo inicial',
        '^defiro.{0,20}bloqueio': 'defiro bloqueio',
        '^defiro.{0,20}liminar': 'defiro liminar',        
        'indefiro.{0,20}bloqueio': 'indefiro bloqueio',
        'indefiro.{0,20}liminar': 'indefiro liminar',        
        'mantenho.{0,20}bloqueio': 'mantenho bloqueio',
        'embargos.{0,30}rejeit': 'rejeito embargos',
        'preliminar.{0,10}nao.{0,10}acholidas': 'indefiro liminar',
        '(indef|rejeit).{0,20}( acao|inicial)': 'rejeito inicial',
        'rejeito.{0,20}embargos': 'rejeito embargos',
        'homologo.{0,70}acordo': 'homologo acordo',
        'homologada.{0,10}transacao': 'homologo acordo',
        'homologo.{0,40}pedido de desistencia': 'homologo desistencia',
        'sem .{10,20} merito': 'extinto sem merito',
        'extint.{0,20}punibilidade': 'extinto punibilidade'
    }
    if grau == '2':
        mapping = {
            'deram': 'deram',
            'negar': 'negar',
            'denegar': 'denegar',
            'rejeit': 'rejeit',
            'nao conhecer': 'nao conhecer'
        }
    return map_regex(decisions, mapping)


def map_regex(series, mapping, keep_unmatched=True):
    series = series.reset_index(drop=True)
    mapped = pd.Series(index=series.index)
    for key, val in mapping.items():
        mapped.loc[
            series.str.contains(key) & mapped.isnull()
        ] = val
    if keep_unmatched:
        mapped.loc[mapped.isnull()] = series
    return mapped.values


def get_decision(texts, grau='1'):
    regex_list = [
        '(julgo .{0,20}procedentes?)( parcialmente )?( em parte.? )?',
        'recebo .{0,20}( acao|inicial)',
        'inicial .{0,20}recebida',
        'dou .{0,20}saneado',
        'preliminares.{0,20}acolhidas',
        'mantenho a decisao agravada',
        'recebo a apelacao',
        'declaro encerrada a instrucao processual',
        'mantenho .{0,20}bloqueios?',
        '(in)?defiro .{0,20}desbloqueio',
        'recebo os embargos.{0,40}rejeito',
        'declaro suspenso',
        'defiro .{0,20}bloqueio',
        'conheco .{0,10}embargos.{0,20}provimento',
        '(in)?defiro .{0,20}liminar',
        'rejeit(o|a) .{0,30}( acao|inicial|embargos)',
        'indef(erida|iro).{0,20}inicial',
        'homologo .{0,70}acordo',
        'homologada .{0,10}transacao',
        'homologo .{0,40}pedido de desistencia',
        'extingo .{10,30} sem .{10,20} merito',
        'extint.{0,20}punibilidade',
        'homologo .{0,10}desistencia.{0,20}testemunhas?'
    ]
    if grau == '2':
        regex_list = [
            ('(rejei|deram|negar|denegar|nao conhecer)'
             '.{0,40}(recurso|interno|agravo|embargos|ordem)'
            '(.{0,5}v\. ?u\.)?'),
            '(recurso.{0,20}provido)(.{0,5}v\. ?u\.)?'
        ]
    return extract_from_list(texts, regex_list)


def extract_from_list(series, regex_list):
    extracted = pd.Series(index=series.index)    
    for regex in regex_list:
        regex = '({})'.format(regex)
        extracted.loc[
            extracted.isnull()
        ] = series.str.extract(regex)[0]
    return extracted
    

def clean_number(numbers):
    numbers = (
        numbers
        .str.extract('([0-9].*[0-9])', expand=False)
        .str.replace(' ', '')
    )
    numbers = clean_cnj_number(numbers, errors='ignore')
    return numbers


def is_cnj_number(numbers):
    regex = get_number_regexes()['cnj']['regex']
    return numbers.str.match(regex)    


def clean_cnj_number(numbers, errors='coerce'):
    regex = get_number_regexes()['cnj']['regex']
    df = (
        numbers
        .fillna('')
        .str.extract(regex)
    )
    cleaned = (
        df[0] + '-' + df[2] + '.' + df[3] + '.' +
        df[5] + '.' + df[6] + '.' + df[7]
    )
    if errors == 'ignore':
        cleaned.loc[cleaned.isnull()] = numbers
    return cleaned


def get_number_regexes():
    return {
        'cnj': {
            'regex': (
                '([0-9]+)(\-|\.)?([0-9]{2})\.'
                '((20|19)[0-9]{2})\.'
                '([0-9])\.?([0-9]{2})\.([0-9]{4})'
            ),
            'names': {
                3: 'filingyear',
                5: 'code_j',
                6: 'code_tr',
                7: 'oooo'
            }
        },
        'mg': {
            'regex': '([0-9]{4})([0-9]{2})([0-9]{6}\-[0-9])',
            'names': {1: 'filingyear'}
        },
        'other': {
            'regex': '(^|[^0-9])((20[01]|19[89])[0-9])($|[^0-9])',
            'names': {1: 'filingyear'}
        },
        'no_match': {
            'regex': '()',
            'names': {0: 'filingyear'}
        }
    }


def get_tribunal(
        series,
        input_type='number',
        output='tribunal'
    ):
    '''
    Args:
       input_type: 'number' or 'diario'
       output: 'tribunal' or 'tribunal_id'
    '''
    if input_type == 'number':
        tribunal = (
            get_data('tribunal.csv')
            .set_index(['code_j', 'code_tr'])
        )
        info = extract_info_from_case_numbers(
            series, tp="cnj"
        )
        info = info.join(
            tribunal,
            on=['code_j', 'code_tr']
        )
        return info[output]
    if input_type == 'diario':
        diario = (
            get_data('diario.csv')
            .set_index('diario')
        )
        return (
            series
            .to_frame(name='diario')
            .join(diario, on='diario')
            .loc[:, (output)]
        )
        
def transform(x, from_var, to_var):
    infile = '{}.csv'.format(
        from_var.replace('_id', '')
    )
    df = get_data(infile).set_index(from_var)
    if type(x) == pd.Series:
        return (
            x.to_frame(name=from_var)
            .join(df, on=from_var)[to_var]
        )
    else:
        return df.loc[x, to_var]
    

def get_number_type(numbers):
    regexes = get_number_regexes()
    types = pd.Series(index=numbers.index)
    for key, val in regexes.items():
        types.loc[
            types.isnull() & numbers.str.contains(val['regex'])
        ] = key
    return types


def extract_info_from_case_numbers(numbers, tp=None):
    df = pd.DataFrame({
        'number': numbers,
        'type': get_number_type(numbers)
    })
    if tp:
        df['type'] = tp
    info = (
        df.groupby('type')
        .apply(extract_info_by_case_type)
    )
    info.index = info.index.droplevel(0)
    return info


def extract_info_by_case_type(df):
    regex = get_number_regexes()[df.name]
    info = (
        df['number'].str.extract(regex['regex'])
        .apply(pd.to_numeric, errors='coerce')
        .loc[:, regex['names'].keys()]        
        .rename(columns=regex['names'])
    )
    return info


def move_columns_first(df, cols):
    for col in list(reversed(cols)):
        if col in df.columns:
            c = list(df)
            c.insert(0, c.pop(c.index(col)))
            df = df.loc[:, c]
    return df


def get_decisao_id(decisoes):
    ids = get_data('decisao.csv')
    mapping = dict(zip(ids.decisao, ids.id))    
    return decisoes.map(mapping)


def get_tipo_parte_id(tipo_partes):
    ids = get_data('tipo_parte.csv')
    mapping = dict(zip(ids.tipo_parte, ids.id))
    return tipo_partes.map(mapping)


def get_foro_id(numbers):
    return get_foro_info(numbers).loc[:, 'id']


def get_comarca_id(numbers):
    return get_foro_info(numbers).loc[:, 'comarca_id']


def get_comarca(numbers):
    ids = (
        get_foro_info(numbers)
        .loc[:, 'comarca_id']
        .to_frame()
    )
    comarca = get_data('comarca.csv').set_index('id')
    df = ids.join(comarca, on='comarca_id', how='left')
    return df['comarca']


def get_foro_info(numbers):
    foro = get_data('foro.csv')
    foro_info = (
        extract_info_from_case_numbers(numbers, tp="cnj").reset_index()
        .merge(
            foro,
            left_on=['code_tr', 'oooo'],
            right_on=['estado_id', 'oooo'],
            how='left'
        )
    )
    foro_info.index = foro_info['index']
    return foro_info


def get_filing_year(numbers):
    filingyear = extract_info_from_case_numbers(
        numbers
    ).loc[:, 'filingyear']
    filingyear.loc[filingyear.between(0, 18)] = filingyear + 2000
    filingyear.loc[filingyear.between(80, 99)] = filingyear + 1900
    return filingyear


def read_csv(regex):
    infies = glob.glob(regex)
    return pd.concat(
        map(pd.read_csv, infiles),
        sort=True
    )


def get_caderno_id(diario, caderno):
    ids = (
        get_data('caderno.csv')
        .set_index(
            ['diario', 'caderno']
        )
    )
    df = pd.concat([diario, caderno], axis=1)
    df2 = df.join(ids, on=['diario', 'caderno'])
    return df2['caderno_id']
    

def clean_text(
    text,
    drop='^a-z0-9 ',
    lower=True,
    accents=False,
    links=False,
    newline=False,
    pagebreak=False
):
    text = text.fillna("").astype(str)
    if not links:
        text = remove_links(text)
    if not newline:
        text = text.str.replace('\n', ' ')
    if not pagebreak:
        text = text.str.replace('==>.*?<==', '')
    if lower:
        text = text.str.lower()
    if not accents:
        text = text.apply(unidecode)
    if drop:
        text = text.str.replace(
            '[{}]'.format(drop), ''
        )
    text = (
        text
        .str.replace("  +", " ")
        .str.strip()        
    )
    return text


def remove_links(text):
    return (
        text
        .str.replace(r'\[(.*?)\]', r'\1')
        .str.replace(r'(?s)\(http.*?\)', r'')
    )


def clean_text_columns(df, exclude=[], drop='^a-z0-9 '):
    for col in df.select_dtypes(include="object").columns:
        if col not in exclude:
            df[col] = clean_text(df[col], drop=drop)            
    return df


def get_data(datafile):
    infile = get_data_file(datafile)
    return pd.read_csv(infile)


def get_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(
        pkg_dir, "data", datafile
    )



def generate_id(df, suffix=None):
    '''
    Args:
       df: series or df
       suffix: Either None or a max two-digit
               number to be appended to id
    '''
    if type(df) == pd.DataFrame:
        df = df.loc[:, by].astype(str).apply(
            lambda x: '_'.join(x), axis=1
        )
    ids = (
        df
        .astype('category')
        .cat.codes
    ) + 1
    if suffix:
        ids = ids.apply(lambda x: x*100 + suffix)
    return ids


# def generate_number_id(numbers, tribunals):
#     numbers.name = 'number'
#     tribunals.name = 'tribunal'
#     df = pd.concat([numbers, tribunals], axis=1)
#     df['tribunal_number'] = get_tribunal(
#         df['number'], input_type='number'
#     )
#     df['id'] = np.where(
#         df['tribunal'] == df['tribunal_number'],
#         df['number'], df['tribunal'] + df['number']
#     )
#     return df['id']
