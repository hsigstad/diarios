import path
import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
import zipfile
import gc

def parse_consulta_tjsp_from_zip(
        zip_paths, case_numbers=None,
        directory="1", instancia=1, **kwargs
):
    if isinstance(zip_paths, str):
        zip_paths = [zip_paths]

    zip_files = [zipfile.ZipFile(p, 'r') for p in zip_paths]

    try:
        # Map each .md file path to its corresponding zipfile
        file_to_zip = {
            name: z
            for z in zip_files
            for name in z.namelist()
            if name.startswith(f"{directory}/") and name.endswith(".md")
        }

        # Filter for relevant files
        if case_numbers is None:
            selected_files = list(file_to_zip.keys())
        else:
            expected = {f"{directory}/{cn}.md" for cn in case_numbers}
            selected_files = list(expected & file_to_zip.keys())

        # Define reader function
        def read_from_zip(path):
            z = file_to_zip[path]
            with z.open(path) as f:
                return f.read().decode("utf-8")

        return parse_consulta_tjsp_in_chunks(
            selected_files,
            instancia=instancia,
            read_func=read_from_zip,
            **kwargs
        )

    finally:
        for z in zip_files:
            z.close()


def parse_consulta_tjsp_in_chunks(
    infiles,
    instancia=1,
    chunk_size=100_000,
    save_mov=True,
    **kwargs
):
    print("Parsing", len(infiles), "cases")

    proc = pd.DataFrame()
    parte = pd.DataFrame()
    adv = pd.DataFrame()
    mov = pd.DataFrame()

    for i in range(0, len(infiles), chunk_size):
        print("Parsed", i, "of", len(infiles))
        chunk = infiles[i:i + chunk_size]

        # Parse the chunk
        proc_chunk, mov_chunk, parte_chunk, adv_chunk = parse_consulta_tjsp(
            chunk, instancia=instancia, **kwargs
        )

        # Incrementally concatenate results
        proc = pd.concat([proc, proc_chunk])
        parte = pd.concat([parte, parte_chunk])
        adv = pd.concat([adv, adv_chunk])
        if save_mov:
            mov = pd.concat([mov, mov_chunk])

        # Clean up memory
        del proc_chunk, mov_chunk, parte_chunk, adv_chunk, chunk
        gc.collect()

    return proc, mov, parte, adv


def parse_consulta_tjsp(infiles, instancia=1, read_func=None):
    df = pd.DataFrame({'infile': infiles})
    if read_func is None:
        read_func = read
    df['text'] = df.infile.apply(read_func)
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
    proc = df.drop(columns=['movimentacoes', 'partes', 'text'])
    return proc, mov, parte, adv


def read(infile):
    with open(infile, 'r') as f:
        return f.read()


def read(path):
    with zipfile_obj.open(path) as f:
        return f.read().decode('utf-8')


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
