"""Parser for TRF1 (Tribunal Regional Federal da 1a Região) scraped case data."""

from __future__ import annotations

from typing import List, Tuple

import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.clean import split_series


def parse_consulta_trf1(
    infiles: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse scraped TRF1 case CSV files into relational DataFrames.

    Args:
        infiles: List of CSV file paths from the TRF1 scraper.

    Returns:
        Tuple of (df, proc, mov, parte, adv, pub, it, peticao).
    """
    df = get_df(infiles)
    proc = get_proc(df)
    parte, adv = get_parte_adv(df.partes)
    mov = get_mov(df.movimentacao)
    pub = get_pub(df.publicacao)
    it = get_inteiro_teor(df['inteiro teor'])
    peticao = get_peticao(df.peticoes)
    return df, proc, mov, parte, adv, pub, it, peticao


def get_df(infiles: List[str]) -> pd.DataFrame:
    """Load and deduplicate scraped CSV files.

    Args:
        infiles: List of CSV file paths.

    Returns:
        DataFrame indexed by (npu, instancia).
    """
    df = pd.concat(map(pd.read_csv, infiles))
    cols = ['peticoes', 'incidentes']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df.query('error.isnull()')
    df['instancia'] = df.municipio.apply(lambda x: 2 if x == 'TRF 1A REGIAO' else 1)
    df = df.drop_duplicates(['npu', 'instancia']) # Drops 2
    df = df.set_index(['npu', 'instancia'])
    return df


def get_proc(df: pd.DataFrame) -> pd.DataFrame:
    """Extract case metadata from the processo column.

    Args:
        df: DataFrame with a ``processo`` column of raw text.

    Returns:
        DataFrame with case metadata (classe, relator, vara, etc.).
    """
    keys = {
        'Grupo:': 'grupo',
        'Data de Autuação:': 'data_autuacao',
        'Órgão Julgador:': 'orgao_julgador',
        'Juiz Relator:': 'relator',
        'Vara:': 'vara',
        'Juiz:': 'juiz',
        'Classe:': 'classe',
        'Distribuição:': 'distribuicao',
        'Assunto da Petição:|Assunto:': 'assunto',
        'Localização:': 'localizacao',
        'Observação:': 'observacao',
        'Nº de volumes:': 'volumes',
        'Processo Originário:': 'processo_originario',
        'Principal:': 'principal',
    }
    for k, v in keys.items():
        df[v] = df.processo.str.extract(f'({k})(.*)')[1]
    df['relator'] = df.relator.str.replace("(DESEMBARGADOR)A? FEDERAL ", "", regex=True)
    text_cols = ['relator', 'juiz', 'vara', 'orgao_julgador', 'assunto', 'localizacao', 'grupo']
    for col in text_cols:
        df[col] = clean_text(df[col])
    df['data_distribuicao'] = df.distribuicao.str.extract('(\d{2}/\d{2}/\d{4})')[0]
    date_cols = ['data_autuacao', 'data_distribuicao']
    df['distribuicao'] = df.distribuicao.str.extract(' - (.*?) - ')[0]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    df['relator'] = clean_text(df.relator.str.replace("(DESEMBARGADOR)A? FEDERAL ", "", regex=True))
    classes = {
        'Ação Penal': 'APN',
        'Ação Civil Pública': 'ACP',
        'Ação Civil de Improbidade Administrativa': 'ACIA',
        'APCIV': 'APCIV',
        'APCRIM': 'APCRIM',
    }
    df['classe'] = df.classe.fillna(df.grupo)
    df['classe'] = map_regex(df.classe, classes)
    proc_cols = [
        'municipio', 'date_scraped', 'distribuicao',
        'incidentes', 'data_autuacao', 'orgao_julgador',
        'relator', 'vara', 'juiz', 'classe', 'assunto', 'localizacao', 'observacao',
        'volumes', 'processo_originario', 'principal', 'data_distribuicao',
    ]
    proc_cols = [c for c in proc_cols if c in df.columns]
    proc = df.loc[:, proc_cols]
    return proc


def get_parte_adv(partes: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract parties and lawyers from the partes column.

    Args:
        partes: Series of raw party text.

    Returns:
        Tuple of (parte, adv) DataFrames.
    """
    keys = [
        'Réu', 'Autor', 'LITISAT', 'Apelante', 'Apelado',
        'ASSIST.', 'ASSISTA', 'REQTE.', 'REQDO.',
        'LITISCONSORTE ATIVO', 'PERITO', 'LITISPA',
    ]
    regex = '|'.join(keys)
    regex = f'(?P<key>{regex})'
    parte = split_series(
        partes,
        regex,
        drop_end=True,
        level_name='parte_id',
        text_name='parte'
    )
    parte['parte'] = parte.parte.str.replace('^[,0-9]+', '', regex=True)
    parte['key'] = clean_text(parte.key)
    adv_regex = 'ADVOGAD[OA]|PROCURADORA?,|PROC/S/OAB|,'
    parte[['parte', 'adv']] = parte.parte.str.split(adv_regex, n=1, expand=True)
    adv = split_series(parte.adv, '\n', level_name='adv_id')
    adv['adv'] = adv.adv.str.replace('(ADVOGAD[OA]|PROCURADORA?|PROC/S/OAB),*', '', regex=True)
    adv = adv.query('adv != ""')
    oab_regex = '([A-Z]{2}[0-9]+)'
    adv['oab'] = adv.adv.str.extract(oab_regex)[0]
    adv['adv'] = adv.adv.str.replace(oab_regex, '', regex=True)
    adv['adv'] = adv.adv.str.replace('E OUTROS(AS)', '', regex=False)
    adv['adv'] = clean_text(adv.adv)
    adv.index = adv.index.droplevel('adv_id')
    adv = adv.query('adv!=""')
    parte = parte.drop(columns="adv")
    parte['parte'] = clean_text(parte.parte)
    return parte, adv


def get_mov(movimentacao: pd.Series) -> pd.DataFrame:
    """Extract case movements from the movimentacao column.

    Args:
        movimentacao: Series of raw movement text.

    Returns:
        DataFrame with movement details (date, time, text).
    """
    mov = split_series(movimentacao, "\n")
    mov[['data_mov', 'time_mov', 'n_mov', 'text1_mov']] = mov.movimentacao.str.split('[ ,]', expand=True, n=3)
    mov[['text1_mov', 'text2_mov']] = mov.text1_mov.str.split(',', expand=True, n=1)
    mov = mov.drop(columns='movimentacao')
    mov['data_mov'] = pd.to_datetime(mov.data_mov, dayfirst=True)
    mov.index = mov.index.droplevel('group')
    return mov


def get_pub(publicacao: pd.Series) -> pd.DataFrame:
    """Extract publication entries from the publicacao column.

    Args:
        publicacao: Series of raw publication text.

    Returns:
        DataFrame with publication details (date, type, text).
    """
    pub = split_series(publicacao, "\n")
    pub[['data_pub', 'tp_pub', 'text_pub']] = pub.publicacao.str.split(',', expand=True, n=2)
    pub = pub.drop(columns='publicacao')
    pub['data_pub'] = pd.to_datetime(pub.data_pub, dayfirst=True, errors='coerce')
    pub.index = pub.index.droplevel('group')
    return pub


def get_peticao(peticoes: pd.Series) -> pd.DataFrame:
    """Extract petition entries from the peticoes column.

    Args:
        peticoes: Series of raw petition text.

    Returns:
        DataFrame with petition details, or empty DataFrame if none found.
    """
    df = split_series(peticoes, "\n", text_name='text')
    try:
        df[['n_peticao', 'data_peticao1', 'data_peticao2', 'text_peticao', 'parte_peticao']] = df.text.str.split(',', expand=True, n=4)
    except ValueError:
        print('Appears to be no peticoes')
        return pd.DataFrame()
    df = df.drop(columns='text')
    df['data_peticao1'] = pd.to_datetime(df.data_peticao1, dayfirst=True, errors='coerce')
    df['data_peticao2'] = pd.to_datetime(df.data_peticao2, dayfirst=True, errors='coerce')
    df.index = df.index.droplevel('group')
    return df


def get_inteiro_teor(inteiro_teor: pd.Series) -> pd.DataFrame:
    """Extract full-text document entries from the inteiro teor column.

    Args:
        inteiro_teor: Series of raw inteiro teor text.

    Returns:
        DataFrame with document details (number, type, date).
    """
    df = split_series(inteiro_teor, "\n", text_name='text')
    df.index = df.index.droplevel('group')
    df = df.reset_index()
    df.loc[df.instancia==2, 'text'] = df.text.str.replace(' ', ',', regex=False)
    df = df.set_index(['npu', 'instancia'])
    df[['n_inteiro_teor', 'tp_inteiro_teor', 'date']] = df.text.str.split(',', expand=True, n=2)
    df['n_inteiro_teor'] = pd.to_numeric(df.n_inteiro_teor, errors='coerce')
    df = df.drop(columns='text')
    df[['data_inteiro_teor', 'time_inteiro_teor', 'vis']] = df.date.str.split(
        ' ', n=2, expand=True
    )
    df = df.drop(columns=['vis'])
    df['data_inteiro_teor'] = pd.to_datetime(
        df.data_inteiro_teor,
        dayfirst=True,
        errors='coerce'
    )
    return df


def test_parte(
    proc: pd.DataFrame, parte: pd.DataFrame, adv: pd.DataFrame
) -> str:
    """Print a random case's party information for debugging.

    Args:
        proc: Process DataFrame with ``partes`` column.
        parte: Parties DataFrame.
        adv: Lawyers DataFrame.

    Returns:
        The sampled case identifier.
    """
    sm = proc.sample().iloc[0].name
    print(proc['partes'].loc[sm])
    try:
        print(parte.loc[sm])
    except Exception:
        pass
    try:
        print(adv.loc[sm])
    except Exception:
        pass
    return sm


def test_mov(proc: pd.DataFrame, mov: pd.DataFrame) -> str:
    """Print a random case's movements for debugging.

    Args:
        proc: Process DataFrame with ``movimentacao`` column.
        mov: Movements DataFrame.

    Returns:
        The sampled case identifier.
    """
    sm = proc.sample().iloc[0].name
    try:
        print(proc['movimentacao'].loc[sm][0:1000])
        mov2 = mov.loc[sm].head().reset_index(drop=True)
        mov2['text1_mov'] = mov2.text1_mov.str[0:20]
        mov2['text2_mov'] = mov2.text2_mov.str[0:20]
        print(mov2)
    except Exception:
        pass
    return sm


def test_proc(df: pd.DataFrame, proc: pd.DataFrame) -> pd.Series:
    """Print a random case's process details for debugging.

    Args:
        df: Raw DataFrame with ``processo`` text column.
        proc: Parsed process DataFrame.

    Returns:
        The sampled row as a Series.
    """
    sm = df.sample().iloc[0]
    print(sm.processo[0:1000])
    print(proc.loc[sm.name])
    return sm
