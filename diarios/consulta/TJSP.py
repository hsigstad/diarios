import path
import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.clean import generate_id


def parse_consulta_tjsp(infiles, instancia=2):
    df = pd.DataFrame({'infile': infiles})
    df['text'] = df.infile.apply(read)
    df['num_npu'] = df.infile.str.extract('/([^/]+)\.md')
    if len(df) != len(df.drop_duplicates('num_npu')):
        raise ValueError("Duplicate observations per case")
    df = df.set_index('num_npu')
    sections = get_sections(instancia)
    for section, marker in sections.items():
        df[section] = df.text.str.extract(f'(?s){marker}(.*?)(?:##|$)')
    regexes = get_regexes(instancia)
    for k, v in regexes.items():
        df[k] = df.text.str.extract(v)
    df = clean(df, instancia)
    parte, adv = get_parte_adv(df.partes)
    mov = get_mov(df.movimentacoes)
    return df, mov, parte, adv


def read(infile):
    with open(infile, 'r') as f:
        return f.read()


def get_sections(instancia):
    if instancia==1:
        return {
            'partes': '## Partes',
            'movimentacoes': '## Movimentações',
            'peticoes_diversas': '## Petições diversas',
            'incidentes': '## Incidentes',
            'audiencias': '## Audiências',
            'historico_de_classes': '## Histórico de classes',
        }
    if instancia==2:
        return {
            'partes': '## Partes',
            'movimentacoes': '## Movimentações',
            'recursos': '## Subprocessos',
            'peticoes_diversas': '## Petições diversas',
            'composicao_julgamento': '## Composição do Julgamento',
            'julgamentos': '## Julgamentos',
        }


def get_regexes(instancia):
    if instancia==1:
        return {
            'status': '[0-9]{4} (.*)',
            'classe': 'Classe\s+(.*)',
            'assunto': 'Assunto\s+(.*)',
            'foro': 'Foro\s+(.*)',
            'vara': 'Vara\s+(.*)',
            'juiz': 'Juiz\s+(.*)',
            'distribuicao': 'Distribuição\s+(.*)',
            'local_fisico': '(?s)Local Físico\s+(.*)\n\n',
            'controle': 'Controle\s+(.*)',
            'area': 'Área\s+(.*)',
            'outros_numeros': 'Outros números\s+(.*)',
        }
    if instancia==2:
        return {
            'status': '[0-9]{4} (.*)',
            'classe': 'Classe\s+(.*)',
            'assunto': '(?s)Assunto\s+(.*?)\n\n',
            'secao': 'Seção\s+(.*)',
            'orgao_julgador': 'Órgão Julgador\s+(.*)',
            'area': 'Área\s+(.*)',
            'relator': 'Relator\s+(.*)',
            'revisor': 'Revisor\s+(.*)',
            'outros_numeros': 'Outros números\s+(.*)',
            'origem': '(?s)Origem\s+(.*?)\n\n',
            'volume': 'Volume / Apenso\s+(.*)',
        }


def clean(df, instancia):
    if instancia==1:
        df['data_distribuicao'] = pd.to_datetime(
            df.distribuicao.str.extract('([0-9]{2}/[0-9]{2}/[0-9]{4})')[0],
            format='%d/%m/%Y',
            errors='coerce'
        )
        df['distribuicao'] = df.distribuicao.str.extract(' - (.*)')[0]
        return df
    if instancia==2:
        return df

    
def get_parte_adv(partes):    
    parte = partes.str.extractall('\n(?P<key>.*?)[|:](?P<parte>.*)')
    parte['parte'] = clean_text(parte.parte)
    parte['key'] = clean_text(parte.key)
    parte = parte.query('parte != "" & key != ""')
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
    return parte, adv

def gen_parte_id(parte):
    df = parte.copy()
    df['one'] = 1
    isadv = df.key.str.contains("ADV")
    df['parte_id'] = df.loc[~isadv].one.cumsum()
    df['parte_id'] = df.parte_id.fillna(method='ffill')
    return df.parte_id


def get_mov(movimentacoes):
    mov = movimentacoes.str.extractall('(?s)\n(?P<date>[0-9]{2}/[0-9]{2}/[0-9]{4}).*?\|.*?\|(?P<text>.*?)(?=\n[0-9]{2}/[0-9]{2}/[0-9]{4})')
    mov['date'] = pd.to_datetime(mov.date, format='%d/%m/%Y', errors='coerce')
    mov['text'] = mov.text.str.strip()
    mov.index = mov.index.droplevel('match')
    return mov


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


def test_mov(proc, mov):
    sm = proc.sample().iloc[0].name
    try:
        print(proc['movimentacoes'].loc[sm][0:1000])
        mov2 = mov.loc[sm].head().reset_index(drop=True)
        mov2['text'] = mov2.text.str[0:20]
        print(mov2)
    except:
        pass
    return sm


def test_proc(df):
    sm = df.sample().iloc[0]
    print(sm.text[0:1000])
    print(sm)
    return sm
