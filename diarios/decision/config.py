"""Regex patterns, configuration, and utilities for decision parsing."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from diarios.clean import get_cardinal_number_regex

__all__ = [
    "get_main_sentence_regexes",
    "get_dispositivo_regexes",
    "get_desfecho_regexes",
    "get_mode",
    "get_key_order",
    "get_subject",
    "print_truncated",
    "intersect",
    "get_pena_regexes",
    "get_split_keys",
    "remove_pena_base",
    "get_df",
]

# TODO: Differentiate between reclusao and detencao (sometimes 2 anos reclusao e 3 meses detencao)


def get_main_sentence_regexes() -> List[str]:
    """Return regexes for extracting the main sentence from a ruling."""
    regexes = [
        r'julgo\b[^.]+',
        'decide.{0,15}turma[^.]+',
        'a turma.{0,10}(unanimidade|maioria)[^.]+',
        r'\.\s+(nego|dou)\b[^.]+',
        r'(para.{0,7}(condenar|absolver)|condeno|absolvo)\b[^.]+',
        'retirado\s+d[ae]\s+pauta[^.]+',
        '(pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto[^.]+',
        'em\s+face.{0,15}considerações[^.]+',
        'is[ts]o.{0,10}posto[^.]+',
        '(diante|posto).{0,5}is[st]o[^.]+',
        'após o voto[^.]+',
        'proferido.{0,10}relatório[^.]+',
    ]
    regexes = [f'(?i)(?s)({regex})' for regex in regexes]
    return regexes


def get_dispositivo_regexes() -> List[str]:
    """Return regexes for extracting the dispositivo section of a ruling."""
    regexes = [
        r'julgo\b.*',
        'decide.{0,15}turma.*',
        'a turma.{0,10}(?:unanimidade|maioria).*',
        'retirado\s+d[ae]\s+pauta.*',
        '(?:pelo|diante|por\s+todo|ante|vista|em\s+face).{0,15}exposto.*',
        'em\s+face.{0,15}considerações.*',
        'is[ts]o.{0,10}posto.*',
        '(?:diante|posto).{0,5}is[st]o.*',
        'após o voto.*',
        'proferido.{0,10}relatório.*',
        r'\b(?:nego|dou|absolvo|condeno|declaro|concedo)\b.*',
    ]
    regexes = [f'(?i)(?s)({regex})' for regex in regexes]
    return regexes


def get_desfecho_regexes(classes: Union[str, List[str]]) -> Dict[str, str]:
    """Return outcome regexes for the given case classes.

    Args:
        classes: A single class string or list of class strings.

    Returns:
        Dict mapping regex patterns to outcome labels.
    """
    if isinstance(classes, str):
        classes = [classes]
    regexes = {}
    for classe in classes:
        regexes = {**regexes, **_get_desfecho_regexes(classe)}
    return regexes


def get_mode() -> Dict[str, str]:
    """Return regex mapping for decision mode (unanimity or majority)."""
    return {
        'UNANIMIDADE': 'UNANIMIDADE',
        'MAIORIA': 'MAIORIA',
    }


def get_key_order(key: pd.Series, order: List[str]) -> pd.Series:
    """Map decision keys to their sort order.

    Args:
        key: Series of decision key strings.
        order: Ordered list defining the sort priority.

    Returns:
        Series with numeric sort positions.
    """
    order = {k: i for i, k in enumerate(order)}
    return key.map(order)




def _get_desfecho_regexes(classe: str) -> Dict[str, str]:
    """Return outcome regexes for a single case class.

    Args:
        classe: Case class identifier (e.g. ``'APN'``, ``'HC'``, ``'Ap'``).

    Returns:
        Dict mapping regex patterns to outcome labels.
    """
    regexes = {
        'HOMOLOG(O|ASE).*DESISTENCIA':
        'HOMOLOGAR-DESISTENCIA',
    }
    if classe in ["all"]: # Not caring about object
        regexes = {
            **regexes,
            'PARCIALMENTE PROCEDENTE': 'PARCIALMENTE PROCEDENTE-',
            'PROCEDENTE EM PARTE': 'PARCIALMENTE PROCEDENTE-',
            'JULG(O|ASE) PROCEDENTE': 'PROCEDENTE-',
            'JULG(O|ASE) IMPROCEDENTE': 'IMPROCEDENTE-',
            'EXT.*SEM.*MERITO': 'S/MERITO-',
            'NAO (SE )?CONHEC': 'NAO CONHECER-',
            'JULG(O|ASE) EXTINTO|EXTINGO': 'EXTINTO-',
            '(INDEFIRO|INDEFERESE)': 'INDEFERIR-',
            r'\bDEFIRO': 'DEFERIR-',
            r'\b(DOU|JULGO|DECLARO|JULGASE).*PREJUDICAD': 'PREJUDICADO-',
            r'\bD(OU|EU|AR|ASE) (SEGU|PROV)IMENTO PARCIAL': 'PARCIAL PROVIMENTO-',
            r'\bD(OU|EU|AR|ASE) PARCIAL (SEGU|PROV)IMENTO': 'PARCIAL PROVIMENTO-',
            r'\bD(OU|EU|AR|ASE) (SEGU|PROV)IMENTO': 'PROVIMENTO-',
            r'\bNEG(AR|O|ASE).*(SEGU|PROV)IMENTO': 'IMPROVIMENTO-',
            'REJEITO': 'REJEITAR-',
            'ACOLH.*PARTE': 'ACOLHER EM PARTE-',
            'ACOLHO': 'ACOLHER-',
            r'CONCEDO': 'CONCEDER-',
            r'\b(NEGO|DENEGO)': 'NEGAR-',
            'ABSOLVO': 'ABSOLVER-',
            'CONDENO': 'CONDENAR-',
            '(NAO |IN)ADMITO': 'NAO ADMITIR-',
            'ADMITO': 'ADMITIR-',
            '(DECLAR|JULG|DECRET)[^.]*EXTIN[^.]*(PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO-',
        }
    if classe in ["APN", "ProOrd", "ACIA", "ACP"]:
        regexes = {
            **regexes,
            'PARCIALMENTE PROCEDENTE': 'PARCIALMENTE PROCEDENTE-',
            'PROCEDENTE EM PARTE': 'PARCIALMENTE PROCEDENTE-',
            'JULGO PROCEDENTE': 'PROCEDENTE-',
            'JULGO IMPROCEDENTE': 'IMPROCEDENTE-',
            'EXT.*SEM.*MERITO': 'S/MERITO-',
            'JULGO EXTINTO|EXTINGO': 'EXTINTO-',
        }
    if classe in ["ED"]:
        regexes = {
            **regexes,
            'REJEIT.*EMBARGOS.*DECL':
            'REJEITAR-EMBARGOS DE DECLARACAO',
            'REJEIT.*EMBARGOS':
            'REJEITAR-EMBARGOS',
            'ACOLH.*PARTE.*EMBARGOS.*DECL':
            'ACOLHER EM PARTE-EMBARGOS',
            'ACOLH.*EMBARGOS.*DECL':
            'ACOLHER-EMBARGOS',
        }
    if classe in ["Ag"]:
        obj = '(RECURSO|AGRAVO)'
        regexes = {
            **regexes,
            f'\\b(DOU|JULGO|DECLARO|JULGASE).*PREJUDICAD.*{obj}':
            'PREJUDICADO-RECURSO',
            f'\\b(DOU|DASE).*(SEGU|PROV)IMENTO.*{obj}':
            'PROVIMENTO-RECURSO',
            f'\\b(RESOLVO NEGAR|NEGO|NEGASE).*(SEGU|PROV)IMENTO.*{obj}':
            'IMPROVIMENTO-RECURSO',
            f'NAO (SE )?CONHEC.*{obj}':
            'NAO CONHECER-RECURSO',
        }
    if classe in ["HC"]:
        obj = '(ORDEM|HABEAS|WRIT|IMPETRACAO|PEDIDO)'
        regexes = {
            **regexes,
            f'\\b(DEFIRO|CONCEDO).*{obj}':
            'CONCEDER-ORDEM',
            f'\\b(NEGO|DENEGO|INDEFIRO|INDEFERESE).*{obj}':
            'DENEGAR-ORDEM',
            'REJEITO.*INICIAL':
            'REJEITAR-INICIAL',
            f'NAO CONHEC.*{obj}':
            'NAO CONHECER-ORDEM',
            f'\\b(DOU|JULGO|DECLARO).*PREJUDICAD.*{obj}':
            'PREJUDICADA-ORDEM',
            'JULGO.*EXTINTO.*SEM.*MERITO':
            'S/MERITO-ORDEM',
        }
    if classe in ["RC"]: # Revisao Criminal
        obj = '(PEDIDO|REVISAO CRIMINAL|PLEITO|ACAO)'
        regexes = {
            **regexes,
            f'\\b(DEFIRO|CONCEDO|JULGO PROCEDENTE).*{obj}':
            'CONCEDER-PEDIDO',
            f'\\b(JULGO IMPROCEDENTE|NEGO|DENEGO|INDEFIRO|INDEFERESE|NAO ADMITO).*{obj}':
            'DENEGAR-PEDIDO',
            f'NAO (SE )?CONHEC.*{obj}':
            'NAO CONHECER-PEDIDO',
            'JULGO.*EXTINT.*SEM.*MERITO':
            'S/MERITO-ORDEM',
        }
    if classe in ["APN"]:
        regexes = {
            **regexes,
            '(DECLAR|JULG|DECRET)[^.]*EXTIN[^.]*(PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO-',
            '(DECLAR|JULG|DECRET|PRONUNC).*PRESCRI':
            'PRESCRICAO-',
            'ACOLH.*DENUNCIA': 'ACOLHER-DENUNCIA',
            'CONDEN(O|AR|A-?L[AO])': 'CONDENAR',
            'ABSOLV(O|ER)': 'ABSOLVER',
        }
    if classe in ["Ap", "ApCrim", "ApCiv"]:
        for obj in ["APELACAO",
                    "APELO",
                    "APELOS",
                    "APELACOES",
                    "RECURSO",
                    "REMESSA OFICIAL",
                    "REMESSA NECESSARIA"]:
            regexes = {
                **regexes,
                f'D(OU|EU|AR) PROVIMENTO PARCIAL.*{obj}':
                f'PARCIAL PROVIMENTO-{obj}',
                f'D(OU|EU|AR) PARCIAL PROVIMENTO.*{obj}':
                f'PARCIAL PROVIMENTO-{obj}',
                f'D(OU|EU|AR) PROVIMENTO.*{obj}': f'PROVIMENTO-{obj}',
                f'NEG.*PROVIMENTO.*{obj}': f'IMPROVIMENTO-{obj}',
                f'NAO CONHEC.*{obj}': f'NAO CONHECER-{obj}',
            }
    if classe in ["ACIA"]:
        regexes = {
            **regexes,
            'RECEBO.*INICIAL': 'RECEBER-INICIAL',
            'REJEITO.*INICIAL': 'REJEITAR-INICIAL',
        }
    if classe in ["REsp"]:
        regexes = {
            **regexes,
            'NAO CONHEC.*RECURSO ESPECIAL':
            'NAO CONHECER-RECURSO ESPECIAL',
            '(NAO.{0,5}|IN)ADMIT.*RECURSO ESPECIAL':
            'NAO ADMITIR-RECURSO ESPECIAL',
            'ADMIT.*RECURSO ESPECIAL':
            'ADMITIR-RECURSO ESPECIAL',
        }
    if classe in ["RE"]:
        regexes = {
            **regexes,
            'NAO CONHEC.*RECURSO EXTRAORDINARIO':
            'NAO CONHECER-RECURSO EXTRAORDINARIO',
            '(NAO.{0,5}|IN)ADMIT.*RECURSO EXTRAORDINARIO':
            'NAO ADMITIR-RECURSO EXTRAORDINARIO',
            'ADMIT.*RECURSO EXTRAORDINARIO':
            'ADMITIR-RECURSO EXTRAORDINARIO',
        }
    return regexes


def get_subject() -> Dict[str, str]:
    """Return regex mapping for decision subject (turma or camara)."""
    return {
        'TURMA': 'TURMA',
        'CAMARA': 'CAMARA',
    }


def get_split_keys(classes: List[str]) -> Dict[str, str]:
    """Return combined split keys for the given case classes.

    Args:
        classes: List of case class identifiers.

    Returns:
        Dict mapping regex patterns to canonical decision key names.
    """
    keys = {}
    for classe in classes:
        keys = {**keys, **_get_split_keys(classe)}
    return keys


def _get_split_keys(classe: str) -> Dict[str, str]:
    """Return split keys for a single case class.

    Args:
        classe: Case class identifier.

    Returns:
        Dict mapping regex patterns to canonical decision key names.
    """
    keys = {}
    if classe in ["ACIA", "APN"]:
        keys = {
            **keys,
            'CONDENO(?!-)': 'CONDENO',
            'PARA CONDENAR': 'CONDENO',
            'JULGO PROCEDENTES?': 'CONDENO',
            'JULGO IMPROCEDENTES?': 'ABSOLVO',
            'PARA JULGAR PROCEDENTES?': 'CONDENO',
            'DEIXO.{0,10}CONDENAR': 'ABSOLVO',
            'CONDEN(?:AR|A-?L[AO])': 'CONDENO',
            'ABSOLV(?:O|ER)': 'ABSOLVO',
        }
    if classe == "APN":
        keys = {
            **keys,
            'DOSIMETRIA': 'CONDENO',
            'CALCULO DA PENA': 'CONDENO',
            '(?:DECLARO|DECRETO|JULGO)[^.]*?EXTIN[^.]*?(?:PUNIBILIDADE|PUNITIVA)':
            'PRESCRICAO',
            '(?:JULGO|DECLARO|VERIFICO)[^.]*?PRESCRICAO': 'PRESCRICAO',
            'PRONUNCI(?:O|AR)': 'PRONUNCIO',
        }
    if classe == "Ap":
        keys = {
            **keys,
            'D(?:OU|AR) PARCIAL PROVIMENTO': 'PARCIAL PROVIMENTO',
            'D(?:OU|AR) PROVIMENTO PARCIAL': 'PARCIAL PROVIMENTO',
            'D(?:OU|AR) PROVIMENTO': 'PROVIMENTO',
            'NEG(?:O|AR) PROVIMENTO': 'IMPROVIMENTO',
        }
    return keys


def intersect(list1: List[Any], list2: List[Any]) -> bool:
    """Check whether two lists share at least one common element.

    Args:
        list1: First list.
        list2: Second list.

    Returns:
        ``True`` if the lists have at least one element in common.
    """
    return len(set(list1).intersection(list2)) > 0


def get_pena_regexes(classes: List[str] = ['APN', 'ACIA']) -> Tuple[List[str], Dict[str, str]]:
    """Return regex patterns for extracting penalty information.

    Args:
        classes: Case class identifiers determining which penalties to extract.

    Returns:
        Tuple of (named-group regexes, boolean regex dict).
    """
    n = get_cardinal_number_regex().replace('(?i)(?s)', '')
    s = '[^.]{0,5}?'
    m = '[^.]{0,10}?'
    l = '[^.]{0,40}?'
    l2 = '[^.0-9]{0,40}?'
    years = f'(?P<years>{n}){l2}\\bANOS?\\b'
    months = f'(?P<months>{n}){l2}\\bMES(ES)?\\b'
    days = f'(?P<days>{n}){l2}\\bDIAS?\\b'
    tp = '(DETENCAO|RECLUSAO)'
    regexes = [
        f'MULTA[^.]+?\\bR[$\s]*(?P<multaR>{n})'
    ]
    boolean_regexes = {
        'has_multa': 'MULTA',
    }
    if intersect(['APN', 'ApCrim'], classes):
        regexes += [
            f'{months}{m}({days}{m})?{tp}',
            f'{years}{m}({months}{m}({days}{m})?)?{tp}',
            f'REGIME{l}(?P<regime>FECHADO|SEMI{s}ABERTO|ABERTO)',
            f'(?P<diasmulta>{n}){l2}DIAS{s}MULTA',
            f'DIAS{s}MULTA{m}(?P<diasmulta>{n})',
            f'PRESTACAO{s}SERVICOS{s}COMUNIDADE{l}(?P<comunidade>{n}){s}HORAS',
        ]
    if 'ACIA' in classes:
        regexes += [
            f'PROIB{m}CONTRATAR[^.]+?(?P<contratar>{n}){s}ANOS',
            f'DIREITOS{s}POLITICOS[^.]+?(?P<dirpol>{n}){s}ANOS',
            f'(RESSARCI|REPARACAO DO DANO|RECOLHIMENTO)[^.]+?\\bR[$\s]*(?P<ressarcirR>{n})',
            f'MULTA{l}(?P<multa>{n}){l}(?P<multa_unit>REMUNERACAO|DANO)',
        ]
        boolean_regexes = {
            **boolean_regexes,
            'perda_funcao': f'PERDA.{l}FUNCAO',
            'ressarcir': 'RESSARCI|RESTITUICAO|REPARACAO DO DANO',
        }
    return regexes, boolean_regexes


def remove_pena_base(text: pd.Series) -> pd.Series:
    """Remove base penalty text (e.g. ``'FIXO PENA BASE DE 2 ANOS'``).

    Args:
        text: Series of dispositivo text strings.

    Returns:
        Series with base penalty sections removed.
    """
    # Removing "FIXO PENA BASE DE 2 ANOS" etc
    # Important: Make sure \2 captures everything after (^.*)
    regex = '(?s)(^.*)(((TORN|FIX|FICA).{0,15}DEFINITIV[OA]|TOTALIZ(O|AM))[^.]+(DETENCAO|RECLUSAO))'
    return text.str.replace(regex, r'\2', regex=True)


def print_truncated(string: str, max_str: int, first_share: float = 0.9) -> None:
    """Print a string, truncating the middle if it exceeds the maximum length.

    Args:
        string: Text to print.
        max_str: Maximum number of characters to display.
        first_share: Fraction of ``max_str`` to show from the beginning.
    """
    if len(string) > max_str:
        print(string[:round(max_str*first_share)])
        print('...')
        print(string[-round(max_str*(1-first_share)):])
    else:
        print(string)


def get_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load sample data for testing decision parsing.

    Returns:
        Tuple of (case DataFrame, party DataFrame).
    """
    df = pd.read_csv('../audit/build/clean/TRF1_inteiro_teor_text.csv')
    it = pd.read_csv('../audit/build/clean/TRF1_inteiro_teor.csv')
    df = df.merge(it, on=['num_npu', 'instancia', 'n_inteiro_teor'])
    #df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)condeno')]
    df = df.query('instancia==2')
    df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)apela..o')]
    #df = df.loc[df.inteiro_teor.fillna('').str.contains('(?i)deten[cç][aã]o|reclus[aã]o')]
    df2 = df.drop_duplicates('num_npu').sample(100)
    df2 = df2.set_index('num_npu')
    parte = pd.read_csv('../audit/build/clean/TRF1_parte.csv')
    parte = parte.loc[parte.key.isin(['REU', 'REQDO'])]
    parte = parte.drop_duplicates(['num_npu', 'parte'])
    parte = parte.set_index('num_npu')
    parte = parte.join(df2, lsuffix='1')
    parte['has_parte'] = 1
    df2['has_parte'] = parte.groupby('num_npu').has_parte.sum() > 0
    df2 = df2.query('has_parte')
    return df2, parte


def _extract_regexes(text: pd.Series, regexes: List[str]) -> pd.Series:
    """Extract first matching regex from a list, applied per row.

    Args:
        text: Series of strings to search.
        regexes: Ordered list of regex patterns to try.

    Returns:
        Series with the first match found for each row.
    """
    out = pd.Series(index=text.index, dtype=object, name=text.name)
    for regex in regexes:
        empty = out.isnull()
        out.loc[empty] = text.loc[empty].str.extract(regex)[0]
    return out
