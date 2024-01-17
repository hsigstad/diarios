import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.io import read_files


def parse_consulta_stj(infiles):
    df = pd.concat(map(pd.read_csv, infiles))
    df = df.query('status=="OK"')
    df = df.drop_duplicates()
    if len(df) != len(df.drop_duplicates('num_npu')):
        raise ValueError("Duplicate observations per case")
    df = df.set_index("num_npu")
    proc = get_proc(df.detalhes)
    proc['date_scraped'] = df.date_scraped
    parte, adv = get_parte_adv(df.detalhes)
    decisao = get_decisao(df.decisoes)
    peticao = get_peticao(df.peticoes)
    pauta = get_pauta(df.pautas)
    mov = get_mov(df.fases)
    return df, proc, parte, mov, adv, decisao, peticao, pauta


def get_proc(detalhes):
    regexes = {
        'classe': 'PROCESSO:',
        'localizacao': 'LOCALIZAÇÃO:',
        'tipo': 'TIPO:',
        'data_autuacao': 'AUTUAÇÃO:',
        'relator': 'RELATOR\(A\):',
        'ramo': 'RAMO DO DIREITO:',
        'assuntos': 'ASSUNTO\(S\):',
        'tribunal_origem': 'TRIBUNAL DE ORIGEM:',
        'numeros_de_origem': 'NÚMEROS DE ORIGEM:',
        'ultima_fase': 'ÚLTIMA FASE:',
    }
    proc = pd.DataFrame()
    for k, v in regexes.items():
        proc[k] = detalhes.str.extract(f'{v}(.*)')
    cols = list(regexes.keys())
    proc['data_autuacao'] = pd.to_datetime(proc.data_autuacao, dayfirst=True, errors='coerce')
    return proc.loc[:, cols]


def get_parte_adv(detalhes):
    parte = (
        detalhes
        .str.extract('(?s)PROCESSO.*?\n(.*?)LOCALIZAÇÃO', expand=False)
        .str.extractall('(?P<key>.*?):(?P<parte>.*?)\n')
    )
    parte['key'] = clean_text(parte.key)
    parte['parte'] = clean_text(parte.parte)
    mapping = {
        'JUSTICA PUBLICA': 'MP',
        'MINISTERIO PUBLICO': 'MP'
    }    
    parte['parte'] = map_regex(parte.parte, mapping)
    parte['parte_id'] = gen_parte_id(parte)
    isadv = parte.key.str.contains("ADV")
    adv = parte.loc[isadv].rename(columns={'parte': 'advogado'})
    parte = parte.loc[~isadv]
    parte.index = parte.index.droplevel('match')    
    adv.index = adv.index.droplevel('match')
    adv[['advogado', 'oab']] = adv.advogado.str.split(' (?=[A-Z]{2}[0-9])', expand=True)
    return parte, adv


def gen_parte_id(parte):
    df = parte.copy()
    df['one'] = 1
    isadv = df.key.str.contains("ADV")
    df['parte_id'] = df.loc[~isadv].one.cumsum()
    df['parte_id'] = df.parte_id.fillna(method='ffill')
    return df.parte_id


def get_decisao(decisoes):
    classes = 'RtP|Ag|ARE |AREsp|RE |REsp|EDcl'
    decisao = decisoes.str.extractall(f'(?s)(?P<decisao>(?:{classes}).*?\n.*?)(?=(?:{classes}|$))')
    regexes = {
        'classe': '(.*?)[0-9]',
        'ministro': '(Min.*)',
        'data_decisao': '([0-9]{2}/[0-9]{2}/[0-9]{4})',
    }
    for k, v in regexes.items():
        decisao[k] = decisao.decisao.str.extract(v, expand=False).str.strip()
    decisao['ministro'] = (
        clean_text(decisao.ministro)
        .str.replace('MIN(ISTR[AO])?', '', regex=True)
        .str.strip()
    )
    decisao['monocratica'] = decisao.decisao.str.contains('Decisão Monocrática')*1
    decisao['data_decisao'] = pd.to_datetime(
        decisao.data_decisao,
        dayfirst=True,
        errors='coerce',
    )
    decisao = decisao.drop(columns='decisao')
    decisao = decisao.reset_index(level='match')
    decisao['n_decisao'] = decisao.match + 1
    decisao = decisao.drop(columns='match')
    return decisao


def get_peticao(peticoes):
    regex = '(?P<num_peticao>[0-9/]+)(?P<classe>\w+) *(?P<data_protocolo>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<data_processamento>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<peticionario>.*)'
    peticao = peticoes.str.extractall(regex)
    for c in ['data_protocolo', 'data_processamento']:
        peticao[c] = pd.to_datetime(peticao[c], dayfirst=True, errors='coerce')
    peticao['peticionario'] = clean_text(peticao.peticionario)
    mapping = {
        'JUSTICA PUBLICA': 'MP',
        'MPF': 'MP',
        'MINISTERIO PUBLICO': 'MP',
    }    
    peticao['peticionario'] = map_regex(peticao.peticionario, mapping)
    return peticao


def get_pauta(pautas):
    regex = '(?P<data_pauta>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<hora_pauta>[0-9]{2}:[0-9]{2})(?P<turma>.*)'
    pauta = pautas.str.extractall(regex)
    pauta['data_pauta'] = pd.to_datetime(pauta.data_pauta, dayfirst=True, errors='coerce')
    pauta.index = pauta.index.droplevel('match')
    return pauta


def get_mov(fases):
    regex = '(?P<data_mov>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<hora_mov>[0-9]{2}:[0-9]{2}) (?P<text>.*)'
    mov = fases.str.extractall(regex)
    mov['data_mov'] = pd.to_datetime(mov.data_mov, dayfirst=True, errors='coerce')
    mov.index = mov.index.droplevel('match')
    return mov


def test_parte(proc, parte, adv):
    sm = proc.sample().iloc[0].name
    print(proc['detalhes'].loc[sm])
    try:
        print(parte.loc[sm])
    except:
        pass
    try:
        print(adv.loc[sm])
    except:
        pass
    return sm


def test(pautas, pauta, max_str=1000, max_rows=10):
    # Works for peticao etc also
    sm = pautas.sample()
    print(sm.iloc[0][0:max_str])
    try:
        out = pauta.loc[sm.index[0]]
        if type(out) == pd.core.frame.DataFrame:
            out = out.head(max_rows).reset_index(drop=True)
        print(out)
    except:
        print("Nothing extracted")
    return sm


def add_decisao_text(decisao, infiles):
    df = read_files(infiles, text_col='decisao')
    df['num_npu'] = df.infile.str.extract(r'(\d{7}-\d{2}\.\d{4}\.\d{1}\.\d{2}\.\d{4})')
    df['n_decisao'] = pd.to_numeric(df.infile.str.extract(r'-(\d+)\.[a-z]{2,3}', expand=False))
    df = df.drop_duplicates(['num_npu', 'n_decisao', 'decisao'])
    df = df.drop_duplicates(['num_npu', 'n_decisao']) # Should not be necessary
    decisao = (
        decisao
        .reset_index()
        .merge(df, on=['num_npu', 'n_decisao'], validate='1:1', how='left')
        .set_index('num_npu')
    )
    return decisao
