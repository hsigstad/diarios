import pandas as pd
from diarios.clean import clean_text
from diarios.clean import clean_oab
from diarios.clean import map_regex
from diarios.io import read_files


def parse_consulta_stf(infiles):
    df = pd.concat(map(pd.read_csv, infiles))
    df = df.query('status=="OK"')
    df = df.drop_duplicates()
    if len(df) != len(df.drop_duplicates('num_npu')):
        raise ValueError("Duplicate observations per case")
    df = df.set_index("num_npu")
    proc = get_proc(df)
    proc['date_scraped'] = df.date_scraped
    parte, adv = get_parte_adv(df.partes)
    mov = get_mov(df.andamentos)
    decisao = get_decisao(df.decisoes)
    deslocamento = get_deslocamento(df.deslocamentos)
    pauta = get_pauta(df.pautas)
    return df, proc, parte, adv, mov, decisao, deslocamento, pauta


def get_proc(df):
    regexes = {
        'assunto': 'Assunto:',
        'data_protocolo': 'Data de Protocolo:',
        'orgao_origem': 'Órgão de Origem:',
        'origem': 'Origem:',
        'numero_origem': 'Número de Origem:',
    }
    for k, v in regexes.items():
        df[k] = df.informacoes.str.extract(f'{v}\n(.*)')
    cols = list(regexes.keys())
    df['data_protocolo'] = pd.to_datetime(df.data_protocolo, dayfirst=True, errors='coerce')
    relator = df.andamentos.str.extract('Distribuído\nCertidão\n(?P<relator>.*)')
    relator['relator'] = clean_text(
        relator.relator.str.replace('MIN\. ', '', regex=True)
    )
    df = df.join(relator, how='left')
    return df.loc[:, cols + ['relator', 'sessao_virtual', 'date_scraped', 'peticoes', 'recursos']]


def get_parte_adv(partes):
    parte = (
        partes
        .str.extractall('(?P<key>.*)\n(?P<parte>[^\n]*)\n?')
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
    adv[['advogado', 'oab']] = adv.advogado.str.split(' (?=[0-9])', expand=True, n=1)
    adv['oab2'] = adv.oab.str.extract(' (.*)')
    adv['oab'] = adv.oab.str.replace(' .*', '', regex=True)
    adv['oab'] = clean_oab(adv.oab)
    return parte, adv


def gen_parte_id(parte):
    df = parte.copy()
    df['one'] = 1
    isadv = df.key.str.contains("ADV")
    df['parte_id'] = df.loc[~isadv].one.cumsum()
    df['parte_id'] = df.parte_id.fillna(method='ffill')
    return df.parte_id


def get_pauta(pautas):
    regex = '(?P<data_pauta>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<text>.*\n.*)'
    pauta = pautas.str.extractall(regex)
    pauta['data_pauta'] = pd.to_datetime(pauta.data_pauta, dayfirst=True, errors='coerce')
    pauta.index = pauta.index.droplevel('match')
    return pauta


def get_mov(andamentos):
    regex = '(?s)(?P<data_mov>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<text>.*?)(?=\n[0-9]{2}/[0-9]{2}/[0-9]{4}|$)'
    mov = andamentos.str.extractall(regex)
    mov['text'] = mov.text.str.strip()
    mov['tp_mov'] = mov.text.str.extract('(.*)')
    mov['data_mov'] = pd.to_datetime(mov.data_mov, dayfirst=True, errors='coerce')
    mov.index = mov.index.droplevel('match')
    return mov


def get_decisao(decisoes):
    regex = '(?s)(?P<data_decisao>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<text>.*?)(?=\n[0-9]{2}/[0-9]{2}/[0-9]{4}|$)'
    decisao = decisoes.str.extractall(regex)
    decisao['data_decisao'] = pd.to_datetime(decisao.data_decisao, dayfirst=True, errors='coerce')
    decisao.index = decisao.index.droplevel('match')
    return decisao


def get_deslocamento(deslocamentos):
    regex = '(?P<deslocamento>.*)\n(?P<envidado_por>.*?)(?P<data_envidado>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<guia>Guia.*)[\n$](?:Recebido em (?P<data_recebido>[0-9]{2}/[0-9]{2}/[0-9]{4})[\n$])?'
    des = deslocamentos.str.extractall(regex)
    for c in ['data_envidado', 'data_recebido']:
        des[c] = pd.to_datetime(des[c], dayfirst=True, errors='coerce')
    return des


def get_doc(mov, infiles):
    df = read_files(infiles, text_col='inteiro_teor')
    df['num_npu'] = df.infile.str.extract(r'(\d{7}-\d{2}\.\d{4}\.\d{1}\.\d{2}\.\d{4})')
    df['n_doc'] = pd.to_numeric(df.infile.str.extract(r'-(\d+)\.[a-z]{2,3}', expand=False))
    doc_terms = [
        "Termo de baixa",
        "Inteiro teor do acórdão",
        "Certidão de trânsito em julgado",
        "Decisão monocrática",
        "Decisão de Julgamento",
        "Certidão",
    ]
    doc_mov = mov.copy().reset_index()
    tp_mov2 = doc_mov.text.str.extract('\n(.*)', expand=False)  
    doc_mov = doc_mov.loc[tp_mov2.isin(doc_terms)]
    doc_mov['one'] = 1
    doc_mov['n_doc'] = doc_mov.groupby('num_npu').one.cumsum() + 1
    doc = (
        doc_mov
        .merge(df, on=['num_npu', 'n_doc'], validate='1:1', how='outer')
        .set_index('num_npu')
    )
    return doc


def test_parte(proc, parte, adv):
    sm = proc.sample().iloc[0].name
    print(proc['partes'].loc[sm])
    try:
        print(parte.loc[sm])
    except:
        pass
    try:
        print(adv.loc[sm])
    except:
        pass
    return sm


def test(pautas, pauta, max_str=1000, max_rows=10, max_col_str=None):
    # Works for peticao etc also
    sm = pautas.sample()
    try:
        print(sm.iloc[0][0:max_str])
        out = pauta.loc[sm.index[0]]
        if type(out) == pd.core.frame.DataFrame:
            out = out.head(max_rows).reset_index(drop=True)
            if max_col_str:
                for c in out.columns:
                    if out[c].dtype=="O":
                        out[c] = out[c].str[0:max_col_str]
        print(out)
    except:
        print("Nothing extracted")
    return sm
