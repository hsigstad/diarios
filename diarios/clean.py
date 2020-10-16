import pandas as pd
import numpy as np
import glob
from unidecode import unidecode
import os
import re
from copy import copy
import warnings
from diarios.misc import get_user_config
warnings.filterwarnings('ignore', 'This pattern has match groups')


class TRF:
    def __init__(self, n):
        if type(n) == str:
            if re.match('TRF', n):
                n = int(n[3])
        self.n = n
        self.name = 'TRF{}'.format(n)
        self.estados = get_trf_estados(self.name)


def get_trf_estados(trf):
    mapping = get_trf_estados_mapping()
    return mapping[trf]


def get_trf_estados_mapping():
    return {
        'TRF1': [
            'AC', 'AM', 'AP', 'BA', 'DF', 'GO', 'MA', 'MG', 'MT', 'PA', 'PI',
            'RO', 'RR', 'TO'
        ],
        'TRF2': ['ES', 'RJ'],
        'TRF3': ['MS', 'SP'],
        'TRF4': ['PR', 'RS', 'SC'],
        'TRF5': ['AL', 'CE', 'PB', 'PE', 'RN', 'SE']
    }


def title(sr):
    sr = sr.str.title()
    tolower = {
        'De': 'de',
        'Da': 'da',
        'Do': 'do',
        'Das': 'das',
        'Dos': 'dos',
        'E': 'e'
    }
    for key, val in tolower.items():
        sr = sr.str.replace(r'\b{}\b'.format(key), val)
    return sr


def clean_estado(estado):
    estado = clean_text(estado)
    mapping = get_estado_mapping()
    ufs = mapping.values()
    lower_ufs = [uf.lower() for uf in ufs]
    uf_mapping = dict(zip(lower_ufs, ufs))
    mapping = {**mapping, **uf_mapping}
    return estado.map(mapping)


def get_estado_mapping():
    return {
        'acre': 'AC',
        'alagoas': 'AL',
        'amapa': 'AP',
        'amazonas': 'AM',
        'bahia': 'BA',
        'ceara': 'CE',
        'distrito federal': 'DF',
        'espirito santo': 'ES',
        'goias': 'GO',
        'maranhao': 'MA',
        'mato grosso': 'MT',
        'mato grosso do sul': 'MS',
        'minas gerais': 'MG',
        'para': 'PA',
        'paraiba': 'PB',
        'parana': 'PR',
        'pernambuco': 'PE',
        'piaui': 'PI',
        'rio de janeiro': 'RJ',
        'rio grande do norte': 'RN',
        'rio grande do sul': 'RS',
        'rondonia': 'RO',
        'roraima': 'RR',
        'sao paolo': 'SP',
        'santa catarina': 'SC',
        'sergipe': 'SE',
        'tocantins': 'TO'
    }


def get_municipio_regex(estados=None):
    mun = get_data('municipio.csv')
    corr1 = get_data('municipio_correction_tse.csv')
    corr2 = get_data('municipio_correction_manual.csv')
    ar = np.concatenate([
        mun.loc[:, ('estado', 'municipio')].values,
        mun.loc[:, ('estado', 'municipio_accents')].values,
        corr1.loc[:, ('estado', 'wrong')].values,
        corr2.loc[:, ('estado', 'wrong')].values
    ])
    df = (pd.DataFrame(ar,
                       columns=['estado', 'municipio'
                                ]).drop_duplicates().query('estado.notnull()'))
    df['municipio'] = title(df.municipio)
    df2 = copy(df)
    df2['municipio'] = df2.municipio.str.upper()
    df = pd.concat([df, df2])
    df3 = copy(df)
    df3['municipio'] = df3.municipio.str.replace("'", "´")
    df = pd.concat([df, df3]).drop_duplicates()
    if estados:
        if not type(estados) == list:
            estados = [estados]
        df = df.loc[df.estado.isin(estados)]
    regex = r'\b({})\b'.format('|'.join(df.municipio.values))
    return regex


def clean_municipio(municipio, estado):
    municipio = clean_text(municipio, drop='^a-z\- ')
    df = pd.DataFrame({
        'wrong': municipio,
        'estado': estado,
        'index': municipio.index
    })
    corr1 = get_data('municipio_correction_tse.csv')
    corr2 = get_data('municipio_correction_manual.csv')
    corr = pd.concat([corr1, corr2], sort=True).drop_duplicates()
    df = pd.merge(df, corr, on=['wrong', 'estado'], validate='m:1', how='left')
    df.loc[df.correct.isnull(), 'correct'] = df.wrong
    df.index = df['index']
    return df.correct


def get_municipio_id(municipio, estado):
    df = pd.DataFrame({
        'municipio': municipio,
        'estado': estado,
        'index': municipio.index
    })
    ids = get_data('municipio_id.csv').dropna()
    ids = pd.merge(df,
                   ids,
                   on=['municipio', 'estado'],
                   validate='m:1',
                   how='left')
    ids.index = ids['index']
    return ids.municipio_id


def clean_comarca(comarca):
    comarca = clean_text(comarca)
    comarca = comarca.str.replace('comarca de', '').str.strip()
    return comarca


def clean_vara(vara):
    vara = clean_text(vara, drop='^a-z0-9 ')
    return vara


def clean_valor(valores):
    return (valores.str.replace('.', '').str.replace(',', '.'))


def clean_date(dates):
    return (dates.fillna('').astype(str).str.replace(
        '/', '-').str.extract('([0-9]{4}-[0-9]{2}-[0-9]{2})'))


def clean_parte(
        partes,
        remove='[^ ]+:.*',
        remove_after=['(^| )dra? ', '^(os?|as?|s) ', ' e outro.*', ' e$'],
        mapping={
            'ministerio publico': 'mp',
            'justica publica': 'mp'
        },
        **kwargs):
    if type(remove) == list:
        remove = '|'.join(remove)
    if type(remove_after) == list:
        remove_after = '|'.join(remove_after)
    partes = partes.str.replace(remove, '')
    partes = clean_text(partes, **kwargs)
    partes = map_regex(partes, mapping)
    partes = partes.str.replace(remove_after, '')
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


def clean_parte_key(keywords):
    return (clean_text(keywords).str.replace(' a?o?s?$', '').str.strip())


def clean_tipo_parte(keywords):
    mapping = {
        'interessado': 'third party',
        'adv|dr|repr': 'lawyer',  # has to be before defd and plaintiff        
        'autor|ente$|ante$|reqte|exeqte': 'plaintiff',
        'lit at|ativ': 'plaintiff',
        '^-$|^\*\*$': 'plaintiff',
        'promotor': 'plaintiff',
        'reu|^res?$|parte re|do$|da$': 'defendant',
        'requerid': 'defendant',
        'requerent': 'plaintiff',
        'reqd|exectd': 'defendant',
        '^x$': 'defendant',
        'passiv|lit pa?s|litispa': 'defendant',
        'paciente': 'paciente'
    }
    return map_regex(keywords, mapping)


def get_procedencia(
    texts,
    regex=('(?s)((julgo\s.{0,20}procedentes?)'
           '(\sparcialmente\s)?(\sem\sparte.?\s)?)'),
    mapping={
        'julg.{0,5}par.{0,10}procedente': 'parcialmente procedente',
        'julg.{0,5} procedentes? ((parcialmente)|(em parte))':
        'parcialmente procedente',
        'julg.{0,5} procedente': 'procedente',
        'julg.{0,5} improcedente': 'improcedente'
    }):
    decision = texts.str.extract(regex, flags=re.IGNORECASE)[0]
    decision = clean_text(decision)
    return map_regex(decision, mapping)


def get_plaintiffwins(decision, parcial=1):
    return decision.map(get_plaintiffwins_mapping(parcial=parcial))


def get_plaintiffwins_mapping(parcial=1):
    return {
        'improcedente': 0,
        'parcialmente procedente': parcial,
        'procedente': 1,
        'recebo inicial': 1,
        'rejeito inicial': 0,
        'defiro liminar': 1,
        'indefiro liminar': 0,
        'defiro desbloqueio': 0,
        'indefiro desbloqueio': 1,
        'defiro bloqueio': 1,
        'indefiro bloqueio': 0,
        'mantenho bloqueio': 1,
        'rejeito embargos': 1,
        'preliminar não acholida': 1,
        'extinto sem merito': 0,
        'extinto punibilidade': 0,
        'deram': 1,
        'negar': 0,
        'denegar': 0,
        'rejeit': 0,
        'nao conhecer': 0
    }


def clean_decision(decisions, grau='1'):
    decisions = clean_text(decisions)
    mapping = {
        'julgo .{0,5}par.{0,10}procedente': 'parcialmente procedente',
        'julgo.{0,5} procedentes? ((parcialmente)|(em parte))':
        'parcialmente procedente',
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
    if type(series) == np.ndarray:
        series = pd.Series(series)
    ix = series.index
    series = series.reset_index(drop=True)
    mapped = pd.Series(index=series.index)
    for key, val in mapping.items():
        mapped.loc[series.str.contains(key) & mapped.isnull()] = val
    if keep_unmatched:
        mapped.loc[mapped.isnull()] = series
    mapped.index = ix
    return mapped


def remove_regexes(texts, regex_list, flags='(?s)'):
    for regex in regex_list:
        regex = r'{}{}'.format(flags, regex)
        texts = texts.str.replace(regex, '')
    return texts


def get_decision(texts, grau='1'):
    regex_list = [
        '(julgo .{0,20}procedentes?)( parcialmente )?( em parte.? )?',
        'recebo .{0,20}( acao|inicial)', 'inicial .{0,20}recebida',
        'dou .{0,20}saneado', 'preliminares.{0,20}acolhidas',
        'mantenho a decisao agravada', 'recebo a apelacao',
        'declaro encerrada a instrucao processual',
        'mantenho .{0,20}bloqueios?', '(in)?defiro .{0,20}desbloqueio',
        'recebo os embargos.{0,40}rejeito', 'declaro suspenso',
        'defiro .{0,20}bloqueio', 'conheco .{0,10}embargos.{0,20}provimento',
        '(in)?defiro .{0,20}liminar',
        'rejeit(o|a) .{0,30}( acao|inicial|embargos)',
        'indef(erida|iro).{0,20}inicial', 'homologo .{0,70}acordo',
        'homologada .{0,10}transacao', 'homologo .{0,40}pedido de desistencia',
        'extingo .{10,30} sem .{10,20} merito', 'extint.{0,20}punibilidade',
        'homologo .{0,10}desistencia.{0,20}testemunhas?'
    ]
    if grau == '2':
        regex_list = [('(rejei|deram|negar|denegar|nao conhecer)'
                       '.{0,40}(recurso|interno|agravo|embargos|ordem)'
                       '(.{0,5}v\. ?u\.)?'),
                      '(recurso.{0,20}provido)(.{0,5}v\. ?u\.)?']
    return extract_from_list(texts, regex_list)


def extract_from_list(series, regex_list):
    extracted = pd.Series(index=series.index)
    for regex in regex_list:
        regex = '({})'.format(regex)
        extracted.loc[extracted.isnull()] = series.str.extract(regex)[0]
    return extracted


def clean_number(numbers):
    numbers = (numbers.str.extract('([0-9].*[0-9])',
                                   expand=False).str.replace(' ', ''))
    numbers = clean_cnj_number(numbers, errors='ignore')
    return numbers


def is_cnj_number(numbers):
    regex = get_number_regex('CNJ')
    return numbers.str.match(regex)


def clean_cnj_number(numbers, errors='coerce'):
    cleaned = numbers.fillna('').str.replace(
        '([0-9]+)(\-|\.)?([0-9]{2})\.'
        '((20|19)[0-9]{2})\.'
        '([0-9])\.?'
        '([0-9]{2})\.'
        '([0-9]{4})', r'0000000\1-\3.\4.\6.\7.\8').str[-25:]
    if errors == 'ignore':
        cleaned.loc[cleaned.isnull()] = numbers
    return cleaned


def get_number_regex(tp='CNJ'):
    regexes = get_number_regexes()
    return regexes[tp]


def get_number_regexes():
    return {
        'CNJ': ('([0-9]+)(\-|\.)?([0-9]{2})\.'
                '(?P<filingyear>(20|19)[0-9]{2})\.'
                '(?P<code_j>[0-9])\.?'
                '(?P<code_tr>[0-9]{2})\.'
                '(?P<oooo>[0-9]{4})'),
        'TJAL': ('[0-9]+\.'
                 '(?P<filingyear>[0-9]{2})'
                 '\.[0-9]+-[0-9]'),
        'TJAM': ('[0-9]+\.'
                 '(?P<filingyear>[0-9]{2})'
                 '\.[0-9]+-[0-9]'),
        'TJCE': ('[0-9]+\.'
                 '(?P<filingyear>(199|200|201)[0-9])'
                 '\.[0-9]{4}\.[0-9]{3}'),
        'TJGO': ('(?P<filingyear>(199|200|201)[0-9])'
                 '[0-9]{7,10}'),
        'TJMA': ('[0-9]{4,10}'
                 '(?P<filingyear>(199|200|201)[0-9])'
                 '[0-9]{7}'),
        'TJMA_2': ('[0-9]+-[0-9]{2}\.'
                   '(?P<filingyear>(199|200|201)[0-9])'
                   '\.[0-9]{2}\.[0-9]{4}'),
        'TJMG': ('[0-9]{4}'
                 '(?P<filingyear>[0-9]{2})'
                 '[0-9]{6}\-[0-9]'),
        'TJMS': ('[0-9]+\.'
                 '(?P<filingyear>[0-9]{2})'
                 '\.[0-9]{6}-[0-9]'),
        'TJPA': ('[0-9]{9}'
                 '(?P<filingyear>(199|200|201)[0-9])'
                 '[0-9]{7}'),
        'TJPB': ('[0-9]{3}'
                 '(?P<filingyear>(199|200|201)[0-9])'
                 '[0-9]{6}-[0-9]'),
        'TJPR': ('[0-9]{1,6}/'
                 '(?P<filingyear>[0-9]{4})'),
        'TJSC': ('[0-9]+\.'
                 '(?P<filingyear>[0-9]{2})'
                 '\.[0-9]{6}-[0-9]'),
        'TJSE': ('(?P<filingyear>(199|200|201)[0-9])'
                 '[0-9]{7}'),
        'TJSP': ('[0-9]+\.[0-9]{2}\.'
                 '(?P<filingyear>(199|200|201)[0-9])'
                 '\.[0-9]{6}-[0-9]'),  # 625.01.1996.002168-3            
        'TJTO': ('(?P<filingyear>(199|200|201)[0-9])'
                 '\.[0-9]{4}\.[0-9]{4}(-|–)[0-9]'),
        'TRF4': ('[0-9]{1,4}\.'
                 '(?P<filingyear>[0-9]{4})'
                 '\.[0-9]\.?[0-9]{2}\.[0-9]{4}'),
        'TRF4_2': ('(?P<filingyear>[0-9]{4})'
                   '\.[0-9]{2}\.[0-9]{2}\.[0-9]{6}')
    }


def get_verificador_cnj(n, remainder):
    '''
    Args:
       n: NNNNNN
       remainder: YYYY.J.TT.FFFF

    NB: Cannot be vectorized since floats are imprecise
    '''
    base = '{}{}00'.format(n, remainder)
    try:
        return 98 - (int(base) % 97)
    except ValueError:
        print('Value Error')

def convert_ncnj_tjms(df, col):
    df = df[df.tribunal.str.contains('TJMS')]
    df2 = df
    df = df[col].str.split(r'\.|\-', expand=True)
    df.columns = df.columns.map(str)
    df['remainder'] = '20' + df['1'] + '812' + '0' + df['0']
    df = df.rename(columns={'2': 'n'})
    df['dd'] = df.apply(lambda x: get_verificador_cnj(x.n, x.remainder), axis=1)
    df['dd'] = df['dd'].astype(str)
    df['dd'] = df['dd'].apply(lambda x: x.zfill(2))
    df['n'] = df['n'].apply(lambda x: x.zfill(7))
    df['n_cnj'] = df['n'] + '-' + df['dd'] + '.20' + df['1'] + '.8.12.0' + df['0']
    df2 = pd.concat([df2, df['n_cnj']], axis=1)
    return df2


def convert_ncnj_tjsp(df, col):
    df = df[df.tribunal.str.contains('TJSP')]
    df2 = df
    df = df[col].str.split(r'\.|\-', expand=True)
    df.columns = df.columns.map(str)
    df['remainder'] = df['2'] + '826' + '0' + df['0']
    df = df.rename(columns={'3': 'n'})
    df['dd'] = df.apply(lambda x: get_verificador_cnj(x.n, x.remainder), axis=1)
    df['dd'] = df['dd'].astype(str)
    df['dd'] = df['dd'].apply(lambda x: x.zfill(2))
    df['n'] = df['n'].apply(lambda x: x.zfill(7))
    df['n_cnj'] = df['n'] + '-' + df['dd'] + '.' + df['2'] + '.8.26.0' + df['0']
    df2 = pd.concat([df2, df['n_cnj']], axis=1)
    return df2


def get_old_format(df, col):
    regex = r'[0-9][0-9][0-9][0-9][0-9][0-9][0-9]\-[0-9][0-9]\.[1-2][0-9][0-9][0-9]\.[0-9].[0-9][0-9]\.[0-9][0-9][0-9][0-9]'
    df['valid'] = df[col].str.contains(regex)
    df = df[df.valid.astype(str).str.contains('False')]
    return df


def get_tribunal(series, input_type='number', output='tribunal'):
    '''
    Args:
       input_type: 'number' or 'diario'
       output: 'tribunal' or 'tribunal_id'
    '''
    if input_type == 'number':
        tribunal = (get_data('tribunal.csv').set_index(['code_j', 'code_tr']))
        info = extract_info_from_case_numbers(series, types=['CNJ'])
        info = info.join(tribunal, on=['code_j', 'code_tr'])
        return info[output]
    if input_type == 'diario':
        diario = (get_data('diario.csv').set_index('diario'))
        return (series.to_frame(name='diario').join(diario,
                                                    on='diario').loc[:,
                                                                     (output)])


def transform(x, from_var, to_var):
    infile = '{}.csv'.format(from_var.replace('_id', ''))
    df = get_data(infile).set_index(from_var)
    if type(x) == pd.Series:
        return (x.to_frame(name=from_var).join(df, on=from_var)[to_var])
    else:
        return df.loc[x, to_var]


def extract_info_from_case_numbers(numbers, types=['CNJ']):
    regexes = map(get_number_regex, types)
    info = pd.DataFrame(index=numbers.index)
    for regex in regexes:
        df = numbers.str.extract(regex)
        new_cols = (set(df.columns) - set(info.columns))
        info = info.join(df.loc[:, new_cols])
        if info.isnull().any().any():
            info[info.isnull()] = df
    info = info.apply(pd.to_numeric, errors='coerce')
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


def get_comarca_id(number=None, comarca=None, tribunal=None):
    if number is not None:
        comarca_id = (get_foro_info(number).loc[:, 'comarca_id'])
    elif (comarca is not None and tribunal is not None):
        df = pd.DataFrame({
            'comarca': comarca,
            'tribunal': tribunal,
            'index': comarca.index
        })
        comarca = get_data('comarca.csv')
        df = df.merge(comarca,
                      on=['tribunal', 'comarca'],
                      how='left',
                      validate='m:1')
        df.index = df['index']
        comarca_id = df['comarca_id']
    else:
        raise Exception('Either number or comarca and'
                        ' tribunal must be specified')
    return comarca_id


def get_comarca(numbers):
    ids = (get_foro_info(numbers).loc[:, 'comarca_id'].to_frame())
    comarca = get_data('comarca.csv').set_index('id')
    df = ids.join(comarca, on='comarca_id', how='left')
    return df['comarca']


def get_foro_info(numbers):
    foro = get_data('foro.csv')
    index_name = numbers.index.name
    if not index_name:
        index_name = 'index'
    foro_info = (extract_info_from_case_numbers(
        numbers,
        types=['CNJ']).reset_index().merge(foro,
                                           left_on=['code_tr', 'oooo'],
                                           right_on=['estado_id', 'oooo'],
                                           how='left'))
    foro_info.index = foro_info[index_name]
    return foro_info


def get_filing_year(numbers, types=['CNJ']):
    filingyear = extract_info_from_case_numbers(numbers,
                                                types).loc[:, 'filingyear']
    filingyear.loc[filingyear.between(0, 18)] = filingyear + 2000
    filingyear.loc[filingyear.between(80, 99)] = filingyear + 1900
    return filingyear


def read_csv(regex):
    infiles = glob.glob(regex)
    return pd.concat(map(pd.read_csv, infiles), sort=True)


def get_caderno_id(diario, caderno):
    ids = (get_data('caderno.csv').set_index(['diario', 'caderno']))
    df = pd.DataFrame({
        'diario': diario,
        'caderno': caderno
    },
                      index=caderno.index)
    df2 = df.join(ids, on=['diario', 'caderno'])
    return df2['caderno_id']


def clean_diario_text(text):
    return clean_text(text,
                      lower=False,
                      drop=None,
                      accents=True,
                      links=False,
                      newline=True)


def clean_text(text,
               drop='^a-z0-9 ',
               replace_character='',
               lower=True,
               accents=False,
               links=False,
               newline=False,
               pagebreak=False,
               multiple_spaces=False,
               strip=True):
    text = text.fillna('').astype(str)
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
        text = text.str.replace('[{}]'.format(drop), replace_character)
    if not multiple_spaces:
        text = text.str.replace('  +', ' ')
    if strip:
        text = text.str.strip()
    return text


def remove_links(text):
    return (text.str.replace(r'\[(.*?)\]',
                             r'\1').str.replace(r'(?s)\(http.*?\)', r''))


def clean_text_columns(df, exclude=[], drop='^a-z0-9 '):
    for col in df.select_dtypes(include='object').columns:
        if col not in exclude:
            df[col] = clean_text(df[col], drop=drop)
    return df


def get_data(datafile):
    infile = get_data_file(datafile)
    return pd.read_csv(infile)


def get_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, 'data', datafile)


def generate_id(df, suffix=None):
    '''
    Args:
       df: series or df
       suffix: Either None or a max two-digit
               number to be appended to id
    '''
    if type(df) == pd.DataFrame:
        df = df.astype(str).apply(lambda x: '_'.join(x), axis=1)
    ids = (df.astype('category').cat.codes) + 1
    if suffix:
        ids = ids.apply(lambda x: x * 100 + suffix)
    return ids


def clean_reais(sr):
    return pd.to_numeric(sr.str.replace(',[0-9]{2}([^0-9].*|$)',
                                        '').str.replace('[^0-9]', ''),
                         errors='coerce')


def clean_integer(sr):
    mapping = get_integer_mapping()
    regex = list(mapping.keys()) + ['[0-9]+']
    regex = '({})'.format('|'.join(regex))
    sr = sr.str.extract(regex, expand=False)
    sr = map_regex(sr, mapping)
    return pd.to_numeric(sr, errors='coerce')


def get_integer_mapping():
    return {
        'uma?': '1',
        'dois': '2',
        'duas': '2',
        'duplo': '2',
        'tres': '3',
        'triplo': '3',
        'quatro': '4',
        'cinco': '5',
        'seis': '6',
        'sete': '7',
        'oito': '8',
        'nove': '9',
        'dez': '10',
        'vinte': '20',
        'trinta': '30',
        'quarenta': '40',
        'cinquenta': '50',
        'sessenta': '60',
        'setenta': '70',
        'oitenta': '80',
        'noventa': '90',
        'cem': '100'
    }


def add_leads_and_lags(df, variables, ivar, tvar, leads_and_lags):
    for l in leads_and_lags:
        df2 = df.copy().loc[:, variables + [ivar, tvar]].drop_duplicates()
        df2[tvar] -= l
        df = pd.merge(df, df2, on=[ivar, tvar], suffixes=['', l], how='left')
    return df


def clean_oab(sr):
    if type(sr) == str:
        sr = pd.Series([sr])
    n = pd.to_numeric(sr.str.replace('[^0-9]', ''),
                      errors='coerce').astype(str).str.replace('\.0', '')
    states = "|".join(get_estado_mapping().values())
    state = sr.str.extract("({})".format(states), expand=False)
    ab = sr.str.extract('[0-9](a|b|A|B)', expand=False).str.upper().fillna('')
    cleaned = n + ab + "/" + state
    return cleaned


letter = "a-zA-Z' çúáéíóàâêôãõÇÚÁÉÍÓÀÂÊÔÃÕ"
