import pandas as pd
import glob
from unidecode import unidecode
import os


def clean_parte(partes):
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
        .str.replace('^(os?|as?|s) ', '')
        .str[:255]
        .str.strip()
    )
    return partes


def clean_valor(valores):
    return (
        valores
        .str.replace('.', '')
        .str.replace(',', '.')
    )


def clean_date(dates):
    dates = dates.fillna("").astype(str)
    dates = (
        dates.str[-4:] + dates.str[3:5] +
        dates.str[0:2]
    )
    return pd.to_numeric(dates, errors='coerce')


def clean_classe(classes):
    classes = clean_text(classes)
    mapping = {
        'improb': 'ACIA',
        'popular': 'APop',
        'publica': 'ACP',
        'agravo de instrumento': 'AI',
        'apelacao': 'Ap'
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
                5: 'j',
                6: 'tr',
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
    ids = pd.read_csv(
        get_data_path('decisao.csv')
    )
    mapping = dict(zip(ids.name, ids.id))    
    return decisoes.map(mapping)


def get_tipo_parte_id(tipo_partes):
    ids = pd.read_csv(
        get_data_path('tipo_parte.csv')
    )
    mapping = dict(zip(ids.name, ids.id))
    return tipo_partes.map(mapping)


def get_foro_id(numbers):
    return get_foro_info(numbers).loc[:, 'id']


def get_comarca_id(numbers):
    return get_foro_info(numbers).loc[:, 'comarca_id']


def get_foro_info(numbers):
    foro = pd.read_csv(
        get_data_path('foro.csv')
    )
    foro_info = (
        extract_info_from_case_numbers(numbers, tp="cnj").reset_index()
        .merge(
            foro,
            left_on=['tr', 'oooo'],
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


def get_estado_id(estado):
    ids = pd.read_csv(
        get_data_path('estado.csv')
    )
    mapping = dict(zip(ids.name, ids.id))
    if type(estado) == pd.Series:
        return estado.map(mapping)
    else:
        return mapping[estado]


def clean_text(text, keep='a-z '):
    return (
        text
        .fillna("")
        .astype(str)                
        .str.lower()
        .apply(unidecode)
        .str.replace('[^{}]'.format(keep), '')
        .str.replace("  +", " ")                
        .str.strip()        
    )


def clean_text_columns(df, exclude=[], keep='a-z0-9 '):
    for col in df.select_dtypes(include="object").columns:
        if col not in exclude:
            df[col] = clean_text(df[col], keep=keep)            
    return df


def get_data_path(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(
        pkg_dir, "data", datafile
    )
