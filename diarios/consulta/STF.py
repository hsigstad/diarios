"""Parser for STF (Supremo Tribunal Federal) scraped case data."""

from __future__ import annotations

from typing import List, Optional, Tuple

import pandas as pd
from diarios.clean import clean_text
from diarios.clean import clean_oab
from diarios.clean import map_regex
from diarios.io import read_files


def parse_consulta_stf(
    infiles: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse scraped STF case CSV files into relational DataFrames.

    Args:
        infiles: List of CSV file paths from the STF scraper.

    Returns:
        Tuple of (df, proc, parte, adv, mov, decisao, deslocamento, pauta).
    """
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


def get_proc(df: pd.DataFrame) -> pd.DataFrame:
    """Extract case metadata from the informacoes column.

    Args:
        df: DataFrame with ``informacoes`` and ``andamentos`` columns.

    Returns:
        DataFrame with case metadata (assunto, data_protocolo, relator, etc.).
    """
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


def get_parte_adv(partes: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract parties and lawyers from the partes column.

    Args:
        partes: Series of raw party text blocks.

    Returns:
        Tuple of (parte, adv) DataFrames.
    """
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


def gen_parte_id(parte: pd.DataFrame) -> pd.Series:
    """Generate sequential party IDs, grouping lawyers with their party.

    Args:
        parte: DataFrame with a ``key`` column identifying party type.

    Returns:
        Series of cumulative party IDs.
    """
    df = parte.copy()
    df['one'] = 1
    isadv = df.key.str.contains("ADV")
    df['parte_id'] = df.loc[~isadv].one.cumsum()
    df['parte_id'] = df.parte_id.ffill()
    return df.parte_id


def get_pauta(pautas: pd.Series) -> pd.DataFrame:
    """Extract hearing schedule entries from the pautas column.

    Args:
        pautas: Series of raw schedule text.

    Returns:
        DataFrame with ``data_pauta`` and ``text`` columns.
    """
    regex = '(?P<data_pauta>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<text>.*\n.*)'
    pauta = pautas.str.extractall(regex)
    pauta['data_pauta'] = pd.to_datetime(pauta.data_pauta, dayfirst=True, errors='coerce')
    pauta.index = pauta.index.droplevel('match')
    return pauta


def get_mov(andamentos: pd.Series) -> pd.DataFrame:
    """Extract case movements from the andamentos column.

    Args:
        andamentos: Series of raw movement text.

    Returns:
        DataFrame with ``data_mov``, ``text``, and ``tp_mov`` columns.
    """
    regex = '(?s)(?P<data_mov>[0-9]{2}/[0-9]{2}/[0-9]{4})(?P<text>.*?)(?=\n[0-9]{2}/[0-9]{2}/[0-9]{4}|$)'
    mov = andamentos.str.extractall(regex)
    mov['text'] = mov.text.str.strip()
    mov['tp_mov'] = mov.text.str.extract('(.*)')
    mov['data_mov'] = pd.to_datetime(mov.data_mov, dayfirst=True, errors='coerce')
    mov.index = mov.index.droplevel('match')
    return mov


def get_decisao(decisoes: pd.Series) -> pd.DataFrame:
    """Extract decisions from the decisoes column.

    Args:
        decisoes: Series of raw decision text.

    Returns:
        DataFrame with ``data_decisao`` and ``text`` columns.
    """
    regex = '(?s)(?P<data_decisao>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<text>.*?)(?=\n[0-9]{2}/[0-9]{2}/[0-9]{4}|$)'
    decisao = decisoes.str.extractall(regex)
    decisao['data_decisao'] = pd.to_datetime(decisao.data_decisao, dayfirst=True, errors='coerce')
    decisao.index = decisao.index.droplevel('match')
    return decisao


def get_deslocamento(deslocamentos: pd.Series) -> pd.DataFrame:
    """Extract case transfers from the deslocamentos column.

    Args:
        deslocamentos: Series of raw transfer text.

    Returns:
        DataFrame with transfer details (dates, guide info).
    """
    regex = '(?P<deslocamento>.*)\n(?P<envidado_por>.*?)(?P<data_envidado>[0-9]{2}/[0-9]{2}/[0-9]{4})\n(?P<guia>Guia.*)[\n$](?:Recebido em (?P<data_recebido>[0-9]{2}/[0-9]{2}/[0-9]{4})[\n$])?'
    des = deslocamentos.str.extractall(regex)
    for c in ['data_envidado', 'data_recebido']:
        des[c] = pd.to_datetime(des[c], dayfirst=True, errors='coerce')
    return des


def get_doc(mov: pd.DataFrame, infiles: List[str]) -> pd.DataFrame:
    """Match document files to case movements.

    Args:
        mov: Movements DataFrame (from ``get_mov``).
        infiles: List of document file paths to read.

    Returns:
        DataFrame with movements joined to document text.
    """
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


def test_parte(
    proc: pd.DataFrame, parte: pd.DataFrame, adv: pd.DataFrame
) -> str:
    """Print a random case's party information for debugging.

    Args:
        proc: Process DataFrame with ``partes`` column.
        parte: Parties DataFrame.
        adv: Lawyers DataFrame.

    Returns:
        The sampled case number (num_npu).
    """
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


def test(
    pautas: pd.Series,
    pauta: pd.DataFrame,
    max_str: int = 1000,
    max_rows: int = 10,
    max_col_str: Optional[int] = None,
) -> pd.Series:
    """Print a random schedule entry for debugging.

    Args:
        pautas: Raw pautas Series.
        pauta: Parsed pauta DataFrame.
        max_str: Maximum characters to print from raw text.
        max_rows: Maximum rows to display from parsed DataFrame.
        max_col_str: If set, truncate string columns to this length.

    Returns:
        The sampled row as a Series.
    """
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
