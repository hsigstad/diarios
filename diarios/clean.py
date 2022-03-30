import pandas as pd
import numpy as np
import glob
from unidecode import unidecode
import os
import re
from copy import copy
import warnings
from diarios.misc import get_user_config

warnings.filterwarnings("ignore", "This pattern has match groups")


class TRT:
    def __init__(self, n):
        if type(n) == str:
            if re.match("TRT", n):
                n = int(n[3])
        self.n = n
        self.name = "TRT{}".format(n)
        self.estados = get_trt_estados(self.name)


class TRF:
    def __init__(self, n):
        if type(n) == str:
            if re.match("TRF", n):
                n = int(n[3])
        self.n = n
        self.name = "TRF{}".format(n)
        self.estados = get_trf_estados(self.name)


def get_trt_estados(trt):
    df = get_data("trt_estado.csv")
    return df.loc[df.trt == trt, "estado"].tolist()


def get_trf_estados(trf):
    mapping = get_trf_estados_mapping()
    return mapping[trf]


def get_trf_estados_mapping():
    return {
        "TRF1": [
            "AC",
            "AM",
            "AP",
            "BA",
            "DF",
            "GO",
            "MA",
            "MG",
            "MT",
            "PA",
            "PI",
            "RO",
            "RR",
            "TO",
        ],
        "TRF2": [
            "ES",
            "RJ",
        ],
        "TRF3": [
            "MS",
            "SP",
        ],
        "TRF4": [
            "PR",
            "RS",
            "SC",
        ],
        "TRF5": [
            "AL",
            "CE",
            "PB",
            "PE",
            "RN",
            "SE",
        ],
    }


def title(sr):
    sr = sr.str.title()
    tolower = {"De": "de", "Da": "da", "Do": "do", "Das": "das", "Dos": "dos", "E": "e"}
    for key, val in tolower.items():
        sr = sr.str.replace(r"\b{}\b".format(key), val)
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
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAPA": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARA": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPIRITO SANTO": "ES",
        "GOIAS": "GO",
        "MARANHAO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARA": "PA",
        "PARAIBA": "PB",
        "PARANA": "PR",
        "PERNAMBUCO": "PE",
        "PIAUI": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDONIA": "RO",
        "RORAIMA": "RR",
        "SAO PAOLO": "SP",
        "SANTA CATARINA": "SC",
        "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }


def get_capital(estado):
    mapping = {
        "AC": "RIO BRANCO",
        "AL": "MACEIO",
        "AP": "MACAPA",
        "AM": "MANAUS",
        "BA": "SALVADOR",
        "CE": "FORTALEZA",
        "ES": "VITORIA",
        "GO": "GOIANIA",
        "MA": "SAO LUIS",
        "MT": "CUIABA",
        "MS": "CAMPO GRANDE",
        "MG": "BELO HORIZONTE",
        "PA": "BELEM",
        "PB": "JOAO PESSOA",
        "PR": "CURITIBA",
        "PE": "RECIFE",
        "PI": "TERESINA",
        "RJ": "RIO DE JANEIRO",
        "RN": "NATAL",
        "RS": "PORTO ALEGRE",
        "RO": "PORTO VELHO",
        "RR": "BOA VISTA",
        "SC": "FLORIANOPOLIS",
        "SP": "SAO PAULO",
        "SE": "ARACAJU",
        "TO": "PALMAS",
        "DF": "BRASILIA",
    }
    return estado.map(mapping)


def get_municipio_regex(estados=None):
    mun = get_data("municipio.csv")
    corr1 = get_data("municipio_correction_tse.csv")
    corr2 = get_data("municipio_correction_manual.csv")
    ar = np.concatenate(
        [
            mun.loc[:, ("estado", "municipio")].values,
            mun.loc[:, ("estado", "municipio_accents")].values,
            corr1.loc[:, ("estado", "wrong")].values,
            corr2.loc[:, ("estado", "wrong")].values,
        ]
    )
    df = (
        pd.DataFrame(ar, columns=["estado", "municipio"])
        .drop_duplicates()
        .query("estado.notnull()")
    )
    df["municipio"] = title(df.municipio)
    df2 = copy(df)
    df2["municipio"] = df2.municipio.str.upper()
    df = pd.concat([df, df2])
    df3 = copy(df)
    df3["municipio"] = df3.municipio.str.replace("'", "´")
    df = pd.concat([df, df3]).drop_duplicates()
    if estados:
        if not type(estados) == list:
            estados = [estados]
        df = df.loc[df.estado.isin(estados)]
    regex = r"\b({})\b".format("|".join(df.municipio.values))
    return regex


def clean_municipio(municipio, estado):
    municipio = clean_text(municipio, drop="^A-Z\- ")
    df = pd.DataFrame({"wrong": municipio, "estado": estado, "index": municipio.index})
    corr1 = get_data("municipio_correction_tse.csv")
    corr2 = get_data("municipio_correction_manual.csv")
    corr = pd.concat([corr1, corr2], sort=True).drop_duplicates()
    df = pd.merge(df, corr, on=["wrong", "estado"], validate="m:1", how="left")
    df.loc[df.correct.isnull(), "correct"] = df.wrong
    df.index = df["index"]
    return df.correct


def get_municipio_id(municipio, estado):
    df = pd.DataFrame(
        {"municipio": municipio, "estado": estado, "index": municipio.index}
    )
    ids = get_data("municipio_id.csv").dropna()
    ids = pd.merge(df, ids, on=["municipio", "estado"], validate="m:1", how="left")
    ids.index = ids["index"]
    return ids.municipio_id


def clean_comarca(comarca):
    comarca = clean_text(comarca)
    comarca = comarca.str.replace("comarca de", "").str.strip()
    return comarca


def clean_vara(vara):
    vara = clean_text(vara, drop="^a-z0-9 ")
    return vara


def clean_valor(valores):
    return valores.str.replace(".", "").str.replace(",", ".")


def clean_date(dates):
    return (
        dates.fillna("")
        .astype(str)
        .str.replace("/", "-")
        .str.extract("([0-9]{4}-[0-9]{2}-[0-9]{2})")
    )


def clean_parte(
    partes,
    delete=None,
    remove="[^ ]+:.*",
    remove_after=[
        "(^| )DRA?S? ",
        "^(OS?|AS?|S) ",
        " E OUTRO.*",
        " E$",
    ],
    mapping={"MINISTERIO PUBLICO": "MP", "JUSTICA PUBLICA": "MP"},
    **kwargs
):
    if type(remove) == list:
        remove = "|".join(remove)
    if type(remove_after) == list:
        remove_after = "|".join(remove_after)
    if type(delete) == list:
        delete = "|".join(delete)
    partes = partes.str.replace(remove, "")
    partes = clean_text(partes, **kwargs)
    partes = map_regex(partes, mapping)
    partes = partes.str.replace(remove_after, "")
    if delete:
        partes.loc[partes.str.contains(delete)] = ""
    return partes


def clean_line(lines):
    return pd.to_numeric(lines, errors="coerce")


def clean_classe(classes):
    classes = clean_text(classes)
    mapping = {
        "IMPROB": "ACIA",
        "POPULAR": "APop",
        "PUBLICA": "ACP",
        "AGRAVO DE INSTRUMENTO": "AI",
        "APELACAO": "Ap",
        "PROCEDIMENTO ORDINARIO": "ProOrd",
        "PROCEDIMENTO SUMARIO": "ProSum",
    }
    return map_regex(classes, mapping)


def clean_parte_key(keywords):
    return clean_text(keywords).str.replace(" A?O?S?$", "").str.strip()


def clean_tipo_parte(keywords):
    mapping = {
        "INTERESSADO": "THIRD PARTY",
        "VITIM": "VICTIM",
        "ADV|DR|REPR": "LAWYER",  # HAS TO BE BEFORE DEFD AND PLAINTIFF
        "AUTOR DO FATO": "DEFENDANT",
        "AUTOR|ENTE$|ANTE$|REQTE|EXEQTE": "PLAINTIFF",
        "LIT AT|ATIV": "PLAINTIFF",
        "^-$|^\*\*$": "PLAINTIFF",
        "PROMOTOR": "PLAINTIFF",
        "REU|^RES?$|PARTE RE|DOS?$|DAS?$": "DEFENDANT",
        "REQUERID": "DEFENDANT",
        "REQUERENT": "PLAINTIFF",
        "REQD|EXECTD": "DEFENDANT",
        "^X$": "DEFENDANT",
        "PASSIV|LIT PA?S|LITISPA": "DEFENDANT",
        "PACIENTE": "PACIENTE",
    }
    return map_regex(keywords, mapping)


def get_procedencia(
    texts,
    regex=(
        "(?s)(?i)((julgo\s.{0,20}procedentes?)" "(\sparcialmente\s)?(\sem\sparte.?\s)?)"
    ),
    mapping={
        "PAR.{0,10}PROCEDENTE": "PARCIALMENTE PROCEDENTE",
        "PROCEDENTES? ((PARCIALMENTE)|(EM PARTE))": "PARCIALMENTE PROCEDENTE",
        r"\bPROCEDENTE": "PROCEDENTE",
        r"\bIMPROCEDENTE": "IMPROCEDENTE",
    },
):
    decision = texts.str.extract(regex)[0]
    decision = clean_text(decision)
    return map_regex(decision, mapping)


def get_plaintiffwins(decision, parcial=1):
    return decision.map(get_plaintiffwins_mapping(parcial=parcial))


def get_plaintiffwins_mapping(parcial=1):
    return {
        "IMPROCEDENTE": 0,
        "PARCIALMENTE PROCEDENTE": PARCIAL,
        "PROCEDENTE": 1,
        "RECEBO INICIAL": 1,
        "REJEITO INICIAL": 0,
        "DEFIRO LIMINAR": 1,
        "INDEFIRO LIMINAR": 0,
        "DEFIRO DESBLOQUEIO": 0,
        "INDEFIRO DESBLOQUEIO": 1,
        "DEFIRO BLOQUEIO": 1,
        "INDEFIRO BLOQUEIO": 0,
        "MANTENHO BLOQUEIO": 1,
        "REJEITO EMBARGOS": 1,
        "PRELIMINAR NÃO ACHOLIDA": 1,
        "EXTINTO SEM MERITO": 0,
        "EXTINTO PUNIBILIDADE": 0,
        "DERAM": 1,
        "NEGAR": 0,
        "DENEGAR": 0,
        "REJEIT": 0,
        "NAO CONHECER": 0,
    }


def clean_decision(decisions, grau="1"):
    decisions = clean_text(decisions)
    mapping = {
        "JULGO .{0,5}PAR.{0,10}PROCEDENTE": "PARCIALMENTE PROCEDENTE",
        "JULGO.{0,5} PROCEDENTES? ((PARCIALMENTE)|(EM PARTE))": "PARCIALMENTE PROCEDENTE",
        "JULGO.{0,5} PROCEDENTE": "PROCEDENTE",
        "JULGO.{0,5} IMPROCEDENTE": "IMPROCEDENTE",
        "INDEFIRO.{0,20}DESBLOQUEIO": "INDEFIRO DESBLOQUEIO",
        "^DEFIRO.{0,20}DESBLOQUEIO": "DEFIRO DESBLOQUEIO",
        "RECEBO.{0,20}( ACAO|INICIAL)": "RECEBO INICIAL",
        "^DEFIRO.{0,20}BLOQUEIO": "DEFIRO BLOQUEIO",
        "^DEFIRO.{0,20}LIMINAR": "DEFIRO LIMINAR",
        "INDEFIRO.{0,20}BLOQUEIO": "INDEFIRO BLOQUEIO",
        "INDEFIRO.{0,20}LIMINAR": "INDEFIRO LIMINAR",
        "MANTENHO.{0,20}BLOQUEIO": "MANTENHO BLOQUEIO",
        "EMBARGOS.{0,30}REJEIT": "REJEITO EMBARGOS",
        "PRELIMINAR.{0,10}NAO.{0,10}ACHOLIDAS": "INDEFIRO LIMINAR",
        "(INDEF|REJEIT).{0,20}( ACAO|INICIAL)": "REJEITO INICIAL",
        "REJEITO.{0,20}EMBARGOS": "REJEITO EMBARGOS",
        "HOMOLOGO.{0,70}ACORDO": "HOMOLOGO ACORDO",
        "HOMOLOGADA.{0,10}TRANSACAO": "HOMOLOGO ACORDO",
        "HOMOLOGO.{0,40}PEDIDO DE DESISTENCIA": "HOMOLOGO DESISTENCIA",
        "SEM .{10,20} MERITO": "EXTINTO SEM MERITO",
        "EXTINT.{0,20}PUNIBILIDADE": "EXTINTO PUNIBILIDADE",
    }
    if grau == "2":
        mapping = {
            "DERAM": "DERAM",
            "NEGAR": "NEGAR",
            "DENEGAR": "DENEGAR",
            "REJEIT": "REJEIT",
            "NAO CONHECER": "NAO CONHECER",
        }
    return map_regex(decisions, mapping)


def extract_series(text, regex):
    """Extract regex series from text list

    Keyword arguments:
    text -- pandas Series with text
    regex -- regex or pandas Series with regexes

    Returns:
    pandas DataFrame of named capture groups for regex matches
    """
    df = pd.DataFrame({"text": text, "regex": regex})
    return df.apply(
        lambda row: _search_row(row.regex, row.text), axis=1, result_type="expand"
    )


def _search_row(regex, text):
    try:
        return re.search(regex, text).groupdict()
    except AttributeError:
        return dict()
    except TypeError:
        return dict()


def extractall_series(text, regex):
    """Extract regex series from text list

    Keyword arguments:
    text -- pandas Series with text
    regex -- regex or pandas Series with regexes

    Returns:
    pandas DataFrame of named capture groups for all regex matches
    """
    df = pd.DataFrame({"text": text, "regex": regex}, index=text.index)
    out = df.apply(lambda row: _searchall_row(row.regex, row.text), axis=1)
    out = out.apply(pd.Series).stack()
    out = out.apply(pd.Series)
    return out


def _searchall_row(regex, text):
    try:
        return [a.groupdict() for a in re.finditer(regex, text)]
    except AttributeError:
        return []
    except TypeError:
        return []


def split_series(text, regex, text_pos="right", drop_end=False, level_name="group"):
    """Split text on regex

    Keyword arguments:
    text -- pandas Series with text
    regex -- regex to split on with named capture group(s)
    text_pos -- keep text to the 'left' or 'right' of split
    level_name -- new index level name

    Returns:
    pandas DataFrame of with columns the capture groups
    and the text to the left or right of split

    Will split on first regex match so be careful
    """
    df = text.str.split(regex, expand=True).stack()
    df.index = df.index.set_names("match", level=-1)
    df = df.reset_index(level="match")
    regex = re.compile(regex)
    group = df.match % (regex.groups + 1)
    group_names = {v: k for k, v in regex.groupindex.items()}
    df["variable"] = group.replace(group_names).replace({0: "text"})
    shift = {"left": 0, "right": 1}
    df[level_name] = (df.match + shift[text_pos]) // (regex.groups + 1)
    df = df.set_index(["variable", level_name], append=True)
    out = df.unstack("variable")[0]
    if drop_end:
        end = out.isnull().sum(axis=1) == len(out.columns) - 1
        out = out.loc[~end]
    out.columns.name = None
    return out


def map_regex(series, mapping, keep_unmatched=True, flags=0):
    """Map using regex

    Keyword arguments:
    series -- pandas Series, numpy ndarray, or str
    mapping -- dict with regexes as keys
    keep_unmatched -- keep original if no match
    flags -- re module flags

    Returns:
    pandas Series or str with values of first matching regex in dict
    """
    if series is np.NaN:
        return np.NaN
    if type(series) == str:
        for key, val in mapping.items():
            if re.search(key, series):
                return val
        if keep_unmatched:
            return series
        else:
            return np.NaN
    if type(series) == np.ndarray:
        series = pd.Series(series)
    if sum(series.isnull()) == len(series):
        print("Empty Series")
        return series
    ix = series.index
    series = series.reset_index(drop=True)
    mapped = pd.Series(index=series.index)
    for key, val in mapping.items():
        mapped.loc[series.str.contains(key, flags=flags) & mapped.isnull()] = val
    if keep_unmatched:
        mapped.loc[mapped.isnull()] = series
    mapped.index = ix
    return mapped


def remove_regexes(texts, regex_list, flags="(?s)"):
    for regex in regex_list:
        regex = r"{}{}".format(flags, regex)
        texts = texts.str.replace(regex, "")
    return texts


def get_decision(texts, grau="1"):
    regex_list = [
        "(JULGO .{0,20}PROCEDENTES?)( PARCIALMENTE )?( EM PARTE.? )?",
        "RECEBO .{0,20}( ACAO|INICIAL)",
        "INICIAL .{0,20}RECEBIDA",
        "DOU .{0,20}SANEADO",
        "PRELIMINARES.{0,20}ACOLHIDAS",
        "MANTENHO A DECISAO AGRAVADA",
        "RECEBO A APELACAO",
        "DECLARO ENCERRADA A INSTRUCAO PROCESSUAL",
        "MANTENHO .{0,20}BLOQUEIOS?",
        "(IN)?DEFIRO .{0,20}DESBLOQUEIO",
        "RECEBO OS EMBARGOS.{0,40}REJEITO",
        "DECLARO SUSPENSO",
        "DEFIRO .{0,20}BLOQUEIO",
        "CONHECO .{0,10}EMBARGOS.{0,20}PROVIMENTO",
        "(IN)?DEFIRO .{0,20}LIMINAR",
        "REJEIT(O|A) .{0,30}( ACAO|INICIAL|EMBARGOS)",
        "INDEF(ERIDA|IRO).{0,20}INICIAL",
        "HOMOLOGO .{0,70}ACORDO",
        "HOMOLOGADA .{0,10}TRANSACAO",
        "HOMOLOGO .{0,40}PEDIDO DE DESISTENCIA",
        "EXTINGO .{10,30} SEM .{10,20} MERITO",
        "EXTINT.{0,20}PUNIBILIDADE",
        "HOMOLOGO .{0,10}DESISTENCIA.{0,20}TESTEMUNHAS?",
    ]
    if grau == "2":
        regex_list = [
            (
                "(REJEI|DERAM|NEGAR|DENEGAR|NAO CONHECER)"
                ".{0,40}(RECURSO|INTERNO|AGRAVO|EMBARGOS|ORDEM)"
                "(.{0,5}V\. ?U\.)?"
            ),
            "(RECURSO.{0,20}PROVIDO)(.{0,5}V\. ?U\.)?",
        ]
    return extract_from_list(texts, regex_list)


def extract_from_list(series, regex_list):
    extracted = pd.Series(index=series.index)
    for regex in regex_list:
        regex = "({})".format(regex)
        extracted.loc[extracted.isnull()] = series.str.extract(regex)[0]
    return extracted


def clean_number(numbers, types=["CNJ"]):
    numbers = numbers.str.extract("([0-9].*[0-9])", expand=False).str.replace(" ", "")
    if "CNJ" in types:
        numbers = clean_cnj_number(numbers, errors="ignore")
    # if 'antigo' in types:
    #     numbers = clean_number_antigo(numbers, errors='ignore')
    return numbers


def is_cnj_number(numbers):
    regex = get_number_regex("CNJ")
    return numbers.str.match(regex)


def clean_cnj_number(numbers, errors="coerce"):
    cleaned = numbers.fillna("")
    # Remove any dot in ddddddd:
    cleaned = cleaned.str.replace("^[^\d]*(\d{1,5})\.(\d{1,5})-", r"\1\2-")
    cleaned = cleaned.str.replace(
        "(\d+)(\-|\.)?(\d{2})\.?"
        "((20|19)\d{2})\.?"
        "(\d)\.?"
        "(\d{2})\.?"
        "(\d{4}).*",
        r"0000000\1-\3.\4.\6.\7.\8",
    ).str[-25:]
    if errors == "ignore":
        # Not sure if this is needed any longer:
        cleaned.loc[cleaned.isnull()] = numbers
    if errors == "coerce":
        cleaned.loc[~cleaned.str.match("\d{7}-\d{2}.\d{4}.\d.\d{2}.\d{4}")] = pd.NA
    return cleaned


def get_number_regex(tp="CNJ"):
    regexes = get_number_regexes()
    return regexes[tp]


def get_number_regexes():
    return {
        "CNJ": (
            "(\d+)(\-|\.)?(\d{2})\."
            "(?P<filingyear>(20|19)\d{2})\."
            "(?P<code_j>\d)\.?"
            "(?P<code_tr>\d{2})\."
            "(?P<oooo>\d{4})"
        ),
        "TJAL": ("\d+\." "(?P<filingyear>\d{2})" "\.\d+-\d"),
        "TJAM": ("\d+\." "(?P<filingyear>\d{2})" "\.\d+-\d"),
        "TJCE": ("\d+\." "(?P<filingyear>(199|200|201)\d)" "\.\d{4}\.\d{3}"),
        "TJGO": ("(?P<filingyear>(199|200|201)\d)" "\d{7,10}"),
        "TJMA": ("\d{4,10}" "(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJMA_2": ("\d+-\d{2}\." "(?P<filingyear>(199|200|201)\d)" "\.\d{2}\.\d{4}"),
        "TJMG": ("\d{4}" "(?P<filingyear>\d{2})" "\d{6}\-\d"),
        "TJMS": ("\d+\." "(?P<filingyear>\d{2})" "\.\d{6}-\d"),
        "TJPA": ("\d{9}" "(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJPB": ("\d{3}" "(?P<filingyear>(199|200|201)\d)" "\d{6}-\d"),
        "TJPR": ("\d{1,6}/" "(?P<filingyear>\d{4})"),
        "TJSC": ("\d+\." "(?P<filingyear>\d{2})" "\.\d{6}-\d"),
        "TJSE": ("(?P<filingyear>(199|200|201)\d)" "\d{7}"),
        "TJSP": (
            "\d+\.\d{2}\." "(?P<filingyear>(199|200|201)\d)" "\.\d{6}-\d"
        ),  # 625.01.1996.002168-3
        "TJTO": ("(?P<filingyear>(199|200|201)\d)" "\.\d{4}\.\d{4}(-|–)\d"),
        "TRF1": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}-\d"),
        "TRF2": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}-\d"),
        "TRF4": ("\d{1,4}\." "(?P<filingyear>\d{4})" "\.\d\.?\d{2}\.\d{4}"),
        "TRF4_2": ("(?P<filingyear>\d{4})" "\.\d{2}\.\d{2}\.\d{6}"),
        "TRE-PB": ("\d+/" "(?P<filingyear>\d{4})"),
        "TRE-MA": ("\d+-\d{2}/" "(?P<filingyear>\d{2})"),
        "TRE-MT": ("\d+/" "(?P<filingyear>\d{4})"),
    }


def get_verificador_cnj(n, remainder):
    """
    Args:
       n: NNNNNN
       remainder: YYYY.J.TT.FFFF

    NB: Cannot be vectorized since floats are imprecise
    """
    base = "{}{}00".format(n, remainder)
    try:
        return str(int(98 - (int(base) % 97))).zfill(2)
    except ValueError:
        pass


def clean_number_antigo(number, tribunal, errors="coerce"):
    # TJAL: 035.07.000018-7
    # TJGO: 200302975980
    # TJMG: 0024.09.000095-2
    # TJMA: 61-61.2012.10.0105
    # TJMS: 018.07.001979-4
    # TJPB: 0482001001567-4
    # TJPI: 2004.40.00.001847-7
    # TJPR: 226/2005
    # TJRO: 003.2008.006388-5
    # TJSC: 011.11.008037-9
    # TJSE: 20165200070
    # TJSP: 660.01.2010.002107-1
    # TJTO: 2011.0011.4133-0
    # TRF1: 2003.37.00.007789-9
    # TRF2: 2016.51.01.164775-3
    # TRF4: 2009.70.09.000477
    df = pd.DataFrame({"number": number, "tribunal": tribunal})
    df["number"] = df.number.fillna("")
    df.loc[df.tribunal == "TRF2", "number"] = clean_number_antigo1(
        df.number, errors=errors
    )
    return df.number


def clean_number_antigo1(number, errors="coerce"):
    cleaned = number.fillna("").str.replace(
        "[^0-9]*((20|19)\d{2})\.?" "(\d{2})\.?" "(\d{2})\.?" "(\d{6})-?" "(\d)[^0-9]*",
        r"\1.\3.\4.\5-\6",
    )
    if errors == "coerce":
        cleaned.loc[~cleaned.str.match("\d{4}.\d{2}.\d{2}.\d{6}-\d")] = pd.NA
    return cleaned


def is_number_antigo(number, tribunal):
    df = pd.DataFrame({"number": number, "tribunal": tribunal})
    regexes = get_number_regexes()
    df["is_antigo"] = False
    for t, r in regexes.items():
        df.loc[df.tribunal == t.replace("_2", ""), "is_antigo"] = df.number.str.match(r)
    return df["is_antigo"]


def convert_number_antigo(number, tribunal, errors="ignore"):
    # TRF1: 2009.37.00.009224-9 -> 0000628-30.2010.4.01.3700 (nnnnnnn?)
    # TRF2: 2016.51.01.164775-3 -> 0164775-04.2016.4.02.5101
    # TRF4: 2009.72.08.003061 -> (same as TRF2?)
    # TJSP: 660.01.2010.002107-1 -> 0002107-31.2010.8.26.0660
    # TJSC: 011.11.008037-9 -> 0008037-57.2011.8.24.0011
    # TJMS: 018.07.001979-4 -> 0001979-89.2007.8.12.0018
    # TJTO: 2008.0003.0041-8 -> 5000033-46.2008.827.2733 (no pattern?)
    # TJPB: 0342011000042-8 -> ???
    # TJSE: not transitioned to numeracao unica
    # TJGO: 200803065609 -> 306560-09.2008.8.09.0120 (how to get oooo?)
    if type(number) == list:
        number = pd.Series(number)
    antigo = clean_number_antigo(number, tribunal)
    antigo.loc[is_number_antigo(antigo, tribunal) == False] = pd.NA
    df = antigo.str.split(r"[.\-]", expand=True)
    df.columns = df.columns.map(str)
    df["j"] = transform(tribunal, "tribunal", "code_j").astype(str)
    df["tr"] = transform(tribunal, "tribunal", "code_tr").astype(str).str.zfill(2)
    df["tribunal"] = tribunal
    df["aaaa"] = _get_aaaa(df)
    df["oooo"] = _get_oooo(df)
    df["n"] = _get_n(df)
    df["remainder"] = df["aaaa"] + df["j"] + df["tr"] + df["oooo"]
    df["dd"] = df.apply(lambda x: get_verificador_cnj(x.n, x.remainder), axis=1)
    cnj = (
        df["n"]
        + "-"
        + df["dd"]
        + "."
        + df["aaaa"]
        + "."
        + df["j"]
        + "."
        + df["tr"]
        + "."
        + df["oooo"]
    )
    if errors == "ignore":
        cnj.loc[cnj.isnull()] = number
    return cnj


def _get_aaaa(df):
    df = df.copy()
    df["aaaa"] = pd.NA
    df.loc[df.tribunal == "TRF2", "aaaa"] = df["0"]
    df.loc[df.tribunal == "TJSP", "aaaa"] = df["2"]
    df.loc[df.tribunal.isin(["TJMS", "TJSC"]), "aaaa"] = "20" + df["1"]
    return df.aaaa


def _get_oooo(df):
    df = df.copy()
    df["oooo"] = pd.NA
    df.loc[df.tribunal == "TRF2", "oooo"] = df["1"] + df["2"]
    df.loc[df.tribunal == "TJSP", "oooo"] = "0" + df["0"]
    df.loc[df.tribunal.isin(["TJMS", "TJSC"]), "oooo"] = "0" + df["0"]
    return df.oooo


def _get_n(df):
    df = df.copy()
    df["n"] = pd.NA
    df.loc[df.tribunal == "TRF2", "n"] = df["3"]
    df.loc[df.tribunal == "TJSP", "n"] = df["3"]
    df.loc[df.tribunal.isin(["TJMS", "TJSC"]), "n"] = df["2"]
    df["n"] = df["n"].fillna("").str.zfill(7)
    return df.n


def get_old_format(df, col):
    regex = r"\d{7}\-\d{2}\.[1-2]\d{3}\.\d.\d{2}\.\d{4}"
    df["valid"] = df[col].str.contains(regex)
    df = df[df.valid.astype(str).str.contains("False")]
    return df


def get_tribunal(series, input_type="number", output="tribunal"):
    """
    Args:
       input_type: 'number' or 'diario'
       output: 'tribunal' or 'tribunal_id'
    """
    if input_type == "number":
        tribunal = get_data("tribunal.csv").set_index(["code_j", "code_tr"])
        info = extract_info_from_case_numbers(series, types=["CNJ"])
        info = info.join(tribunal, on=["code_j", "code_tr"])
        return info[output]
    if input_type == "diario":
        diario = get_data("diario.csv").set_index("diario")
        return series.to_frame(name="diario").join(diario, on="diario").loc[:, (output)]


def transform(x, from_var, to_var, keep_unmatched=False, infile=None, dropna=True):
    if type(x) == list:
        x = pd.Series(x)
    if infile is not None:
        df = pd.read_csv(infile)
    else:
        infile = "{}.csv".format(from_var.replace("_id", ""))
        df = get_data(infile)
    if (type(from_var) == list) and len(from_var) == 1:
        from_var = from_var[0]
    if dropna:
        df = df[df[from_var].notnull()]
    df = df.set_index(from_var)
    if type(x) == pd.DataFrame:
        x.columns = from_var
        df = x.join(df, on=from_var, how="left")
        if keep_unmatched:
            raise ValueError("keep_unmatched not supported for dataframes")
        return df[to_var]
    if type(x) == pd.Series:
        df = x.to_frame(name=from_var).join(df, on=from_var, how="left")
        if keep_unmatched:
            df[to_var] = df[to_var].fillna(df[from_var])
        return df[to_var]
    else:
        return df.loc[x, to_var]


def extract_info_from_case_numbers(number, types=["CNJ"]):
    regexes = map(get_number_regex, types)
    info = pd.DataFrame(index=number.index)
    for regex in regexes:
        df = number.str.extract(regex)
        new_cols = set(df.columns) - set(info.columns)
        info = info.join(df.loc[:, new_cols])
        if info.isnull().any().any():
            info[info.isnull()] = df
    info = info.apply(pd.to_numeric, errors="coerce")
    return info


def move_columns_first(df, cols):
    for col in list(reversed(cols)):
        if col in df.columns:
            c = list(df)
            c.insert(0, c.pop(c.index(col)))
            df = df.loc[:, c]
    return df


def get_decisao_id(decisoes):
    ids = get_data("decisao.csv")
    mapping = dict(zip(ids.decisao, ids.id))
    return decisoes.map(mapping)


def get_tipo_parte_id(tipo_partes):
    ids = get_data("tipo_parte.csv")
    mapping = dict(zip(ids.tipo_parte, ids.id))
    return tipo_partes.map(mapping)


def get_foro_id(numbers):
    return get_foro_info(numbers).loc[:, "id"]


def get_comarca_id(number=None, comarca=None, tribunal=None):
    if number is not None:
        comarca_id = get_foro_info(number).loc[:, "comarca_id"]
    elif comarca is not None and tribunal is not None:
        df = pd.DataFrame(
            {"comarca": comarca, "tribunal": tribunal, "index": comarca.index}
        )
        comarca = get_data("comarca.csv")
        df = df.merge(comarca, on=["tribunal", "comarca"], how="left", validate="m:1")
        df.index = df["index"]
        comarca_id = df["comarca_id"]
    else:
        raise Exception("Either number or comarca and" " tribunal must be specified")
    return comarca_id


def get_comarca(numbers):
    ids = get_foro_info(numbers).loc[:, "comarca_id"].to_frame()
    comarca = get_data("comarca.csv").set_index("id")
    df = ids.join(comarca, on="comarca_id", how="left")
    return df["comarca"]


def get_foro_info(numbers):
    foro = get_data("foro.csv")
    index_name = numbers.index.name
    if not index_name:
        index_name = "index"
    foro_info = (
        extract_info_from_case_numbers(numbers, types=["CNJ"])
        .reset_index()
        .merge(
            foro,
            left_on=["code_tr", "oooo"],
            right_on=["estado_id", "oooo"],
            how="left",
        )
    )
    foro_info.index = foro_info[index_name]
    return foro_info


def get_filing_year(numbers, types=["CNJ"]):
    filingyear = extract_info_from_case_numbers(numbers, types).loc[:, "filingyear"]
    filingyear.loc[filingyear.between(0, 18)] = filingyear + 2000
    filingyear.loc[filingyear.between(80, 99)] = filingyear + 1900
    return filingyear


def read_csv(regex):
    infiles = glob.glob(regex)
    return pd.concat(map(pd.read_csv, infiles), sort=True)


def get_caderno_id(diario, caderno):
    ids = get_data("caderno.csv").set_index(["diario", "caderno"])
    df = pd.DataFrame({"diario": diario, "caderno": caderno}, index=caderno.index)
    df2 = df.join(ids, on=["diario", "caderno"])
    return df2["caderno_id"]


def clean_diario_text(text):
    return clean_text(
        text,
        upper=False,
        lower=False,
        drop=None,
        accents=True,
        links=False,
        newline=True,
    )


def clean_text(
    text,
    drop="^A-Za-z0-9 ",
    replace_character="",
    lower=False,
    upper=True,
    accents=False,
    links=False,
    newline=False,
    pagebreak=False,
    cr=False,
    multiple_spaces=False,
    strip=True,
):
    text = text.fillna("").astype(str)
    if not links:
        text = remove_links(text)
    if not cr:
        text = text.str.replace(r"\r", "\n")
    if not newline:
        text = text.str.replace("\n", " ")
    if not pagebreak:
        text = text.str.replace("==>.*?<==", "")
    if lower:
        text = text.str.lower()
    if upper:
        text = text.str.upper()
    if not accents:
        text = text.apply(unidecode)
    if drop:
        text = text.str.replace("[{}]".format(drop), replace_character)
    if not multiple_spaces:
        text = text.str.replace("  +", " ")
    if strip:
        text = text.str.strip()
    return text


def remove_links(text):
    return text.str.replace(r"\[(.*?)\]", r"\1").str.replace(r"(?s)\(http.*?\)", r"")


def clean_text_columns(df, exclude=[], drop="^A-Z0-9 ", **kwargs):
    for col in df.select_dtypes(include="object").columns:
        if col not in exclude:
            df[col] = clean_text(df[col], drop=drop, **kwargs)
    return df


def get_data(datafile):
    infile = get_data_file(datafile)
    return pd.read_csv(infile)


def get_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, "data", datafile)


def generate_id(df, by=None, suffix=None, suffix_length=2):
    """
    Args:
       df: series or df
       suffix: Either None or a max two-digit
               number to be appended to id
    """
    if type(df) == pd.DataFrame:
        df = df.loc[:, by].astype(str)
        if (type(by) == list) & (len(by) > 1):
            df = df.apply(lambda x: "_".join(x), axis=1)
    ids = (df.astype("category").cat.codes) + 1
    if suffix:
        ids = ids.apply(lambda x: x * 10 ** suffix_length + suffix)
    return ids


def title(sr):
    sr = sr.str.title()
    tolower = {"De": "de", "Da": "da", "Do": "do", "Das": "das", "Dos": "dos", "E": "e"}
    for key, val in tolower.items():
        sr = sr.str.replace(r"\b{}\b".format(key), val)
    return sr


def get_municipio_regex(estados=None):
    mun = get_data("municipio.csv")
    corr1 = get_data("municipio_correction_tse.csv")
    corr2 = get_data("municipio_correction_manual.csv")
    ar = np.concatenate(
        [
            mun.loc[:, ("estado", "municipio")].values,
            mun.loc[:, ("estado", "municipio_accents")].values,
            corr1.loc[:, ("estado", "wrong")].values,
            corr2.loc[:, ("estado", "wrong")].values,
        ]
    )
    df = (
        pd.DataFrame(ar, columns=["estado", "municipio"])
        .drop_duplicates()
        .query("estado.notnull()")
    )
    df["municipio"] = title(df.municipio)
    df2 = copy(df)
    df2["municipio"] = df2.municipio.str.upper()
    df = pd.concat([df, df2])
    df3 = copy(df)
    df3["municipio"] = df3.municipio.str.replace("'", "´")
    df = pd.concat([df, df3]).drop_duplicates()
    if estados:
        if not type(estados) == list:
            estados = [estados]
        df = df.loc[df.estado.isin(estados)]
    regex = r"\b({})\b".format("|".join(df.municipio.values))
    regex = regex.replace(" ", r"\s+")
    return regex


def clean_oab(sr):
    if type(sr) == str:
        sr = pd.Series([sr])
    n = (
        pd.to_numeric(sr.str.replace("[^0-9]", ""), errors="coerce")
        .astype(str)
        .str.replace("\.0", "")
    )
    states = "|".join(get_estado_mapping().values())
    state = sr.str.extract("({})".format(states), expand=False)
    ab = sr.str.extract("\d(a|b|A|B)", expand=False).str.upper().fillna("")
    cleaned = n + ab + "/" + state
    return cleaned


def clean_reais(sr):
    return pd.to_numeric(
        sr.str.replace(",\d{2}([^0-9].*|$)", "").str.replace("[^0-9]", ""),
        errors="coerce",
    )


def clean_integer(sr):
    mapping = get_integer_mapping()
    regex = list(mapping.keys()) + ["\d+"]
    regex = "({})".format("|".join(regex))
    sr = sr.str.extract(regex, expand=False)
    sr = map_regex(sr, mapping)
    return pd.to_numeric(sr, errors="coerce")


def get_integer_mapping():
    return {
        "UMA?": "1",
        "DOIS": "2",
        "DUAS": "2",
        "DUPLO": "2",
        "TRES": "3",
        "TRIPLO": "3",
        "QUATRO": "4",
        "CINCO": "5",
        "SEIS": "6",
        "SETE": "7",
        "OITO": "8",
        "NOVE": "9",
        "DEZ": "10",
        "VINTE": "20",
        "TRINTA": "30",
        "QUARENTA": "40",
        "CINQUENTA": "50",
        "SESSENTA": "60",
        "SETENTA": "70",
        "OITENTA": "80",
        "NOVENTA": "90",
        "CEM": "100",
    }


def add_leads_and_lags(df, variables, ivar, tvar, leads_and_lags):
    for l in leads_and_lags:
        df2 = df.copy().loc[:, variables + [ivar, tvar]].drop_duplicates()
        df2[tvar] -= l
        df = pd.merge(df, df2, on=[ivar, tvar], suffixes=["", l], how="left")
    return df


def clean_oab(sr):
    if type(sr) == str:
        sr = pd.Series([sr])
    n = (
        pd.to_numeric(sr.str.replace("[^0-9]", ""), errors="coerce")
        .astype(str)
        .str.replace("\.0", "")
    )
    states = "|".join(get_estado_mapping().values())
    state = sr.str.extract("({})".format(states), expand=False)
    ab = sr.str.extract("\d(a|b|A|B)", expand=False).str.upper().fillna("")
    cleaned = n + ab + "/" + state
    return cleaned


def clean_cpf(cpf, as_str=False):
    cpf = pd.to_numeric(cpf, errors="coerce")
    if as_str:
        cpf = cpf.astype(str).str.replace("\.0$", "").str.zfill(11)
    return cpf


letter = "a-zA-Z' çúáéíóàâêôãõÇÚÁÉÍÓÀÂÊÔÃÕ"
estados = list(get_estado_mapping().values())
