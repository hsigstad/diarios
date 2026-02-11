"""Generate classe.csv and classe_dispositivo.csv from CNJ unified tables."""

import os
from typing import Dict, Tuple

import pandas as pd
from diarios.clean import clean_text

os.chdir('/home/henrik/diarios')
os.chdir('/home/henrik/Dropbox/brazil/diarios/diarios')


def read(infile: str) -> pd.DataFrame:
    """Read a CSV from the tabelas_unificadas dump directory.

    Args:
        infile: Filename relative to the dump/CSV directory.

    Returns:
        DataFrame from the CSV file.
    """
    indir = os.environ["DB_DIR"]
    infile = os.path.join(indir, 'tabelas_unificadas', 'dump', 'CSV', infile)
    return pd.read_csv(infile)


def get_classe() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build classe and dispositivo DataFrames from CNJ data.

    Returns:
        Tuple of (classe DataFrame, dispositivo DataFrame).
    """
    classe = read('classes.csv')
    item = read('itens.csv')
    classe = classe.merge(item,
                          how='left',
                          left_on=['cod_classe', 'tipo_item'],
                          right_on=['cod_item', 'tipo_item'])
    classe = classe.sort_values('nome')
    classe['classe'] = clean_text(classe.nome)
    classe['classe_id'] = classe.groupby('classe').ngroup()
    disp = get_dipositivo(classe)
    classe = clean_classe(classe)
    classe = add_manually(classe)
    return classe, disp


def clean_classe(classe: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate and rename columns in the classe DataFrame.

    Args:
        classe: Raw classe DataFrame with nome and sigla columns.

    Returns:
        Cleaned classe DataFrame.
    """
    classe = classe.loc[:, ('classe_id', 'classe', 'nome', 'sigla')]
    mapping = get_sigla_mapping()
    for key, val in mapping.items():
        classe.loc[classe.nome == key, 'sigla'] = val
    classe = classe.sort_values('sigla', na_position='last').drop_duplicates(
        'classe_id', keep='first')
    cols = {'nome': 'classe_accents', 'sigla': 'classe_sigla'}
    classe = classe.rename(columns=cols)
    return classe


def add_manually(classe: pd.DataFrame) -> pd.DataFrame:
    """Append manually defined classes not present in CNJ data.

    Args:
        classe: Existing classe DataFrame.

    Returns:
        DataFrame with additional rows appended.
    """
    add = {'Ação de Impugnação de Registro de Candidatura': 'AIRC'}
    max_id = max(classe.classe_id)
    new = pd.DataFrame({
        'classe_id': [max_id + 1 + i for i in range(len(add))],
        'classe_accents': add.keys(),
        'classe_sigla': add.values()
    })
    new['classe'] = clean_text(new.classe_accents)
    return pd.concat([classe, new])


def get_sigla_mapping() -> Dict[str, str]:
    """Return manual sigla overrides for specific classe names.

    Returns:
        Dict mapping classe name to sigla abbreviation.
    """
    return {
        'Agravo de Instrumento': 'AI',
        'Apelação Cível': 'AC',
        'Agravo de Instrumento em Recurso Extraordinário': 'AG/RE',
        'Exceção de Impedimento': 'Impedi',
        'Exceção de Suspeição': 'Suspei',
        'Prestação de Contas': 'PC',
        'Procedimento Comum': 'ProOrd',
        'Recurso Ordinário': 'RO',
        'Recurso Ordinário em Habeas Corpus': 'ROHC'
    }


def get_dipositivo(classe: pd.DataFrame) -> pd.DataFrame:
    """Melt the classe DataFrame into a long-form dispositivo table.

    Args:
        classe: Classe DataFrame with justice-type indicator columns.

    Returns:
        Long-form DataFrame of applicable justice types per classe.
    """
    value_vars = [
        'just_es_1grau', 'just_es_2grau', 'just_es_juizado_es',
        'just_es_turmas', 'just_es_1grau_mil', 'just_es_2grau_mil',
        'just_es_juizado_es_fp', 'just_tu_es_un', 'just_fed_1grau',
        'just_fed_2grau', 'just_fed_juizado_es', 'just_fed_turmas',
        'just_fed_nacional', 'just_fed_regional', 'just_trab_1grau',
        'just_trab_2grau', 'just_trab_tst', 'stf', 'stj', 'cjf', 'cnj',
        'just_mil_uniao_1grau', 'just_mil_uniao_stm', 'just_mil_est_1grau',
        'just_mil_est_tjm', 'just_elei_1grau', 'just_elei_2grau',
        'just_elei_tse', 'mpe', 'mpt', 'mpm', 'mpf', 'cnmp', 'just_trab_csjt'
    ]
    id_vars = [
        'classe_id', 'natureza', 'dispositivo_legal', 'artigo', 'polo_ativo',
        'polo_passivo'
    ]
    disp = classe.melt(id_vars=id_vars, value_vars=value_vars)
    disp = disp.loc[disp.value == 'S']
    disp = disp.drop(columns='value')
    disp = disp.rename(columns={'variable': 'justica'})
    return disp


classe, disp = get_classe()

classe.to_csv('diarios/data/classe.csv', index=False)
disp.to_csv('diarios/data/classe_dispositivo.csv', index=False)
