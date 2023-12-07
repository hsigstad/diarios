import pandas as pd
import numpy as np
import re
import diarios.clean as clean


class CaseParser:
    """Class to parse court cases"""

    def __init__(
        self,
        regexes=[r"(?P<number>[0-9.\-]{19,30})"],
        regexes_before_split=None,
        cleaners={"number": clean.clean_number},
        parte="AUTOR:|RÉU:",
        split_parte_on=",|-|;",
        split_text_on=None,
        id_suffix=None,
        suffix_length=2,
        clean_text=clean.clean_diario_text,
        clean_parte=clean.clean_parte,
        clean_last_parte=lambda x: x,
        clean_parte_key=clean.clean_parte_key,
        clean_tipo_parte=clean.clean_tipo_parte,
        max_name_length=100,
        last_name_length=50,
        clean_proc=lambda x: x,
        clean_mov=lambda x: x,
        df_proc_cols=[],
        df_mov_cols=[],
        df_parte_cols=[],
        drop_if_no_number=True,
        parte_levels=["mov_id", "proc_id"],
        advogado=None,
        split_adv=False,
    ):
        self.parte = parte
        self.regexes_before_split = regexes_before_split
        self.regexes = regexes
        self.cleaners = cleaners
        self.split_parte_on = split_parte_on
        self.split_text_on = split_text_on
        self.id_suffix = id_suffix
        self.suffix_length = suffix_length
        self.clean_text = clean_text
        self.clean_parte = clean_parte
        self.clean_last_parte = clean_last_parte
        self.clean_parte_key = clean_parte_key
        self.clean_tipo_parte = clean_tipo_parte
        self.max_name_length = max_name_length
        self.last_name_length = last_name_length
        self.clean_proc = clean_proc
        self.clean_mov = clean_mov
        self.df_proc_cols = df_proc_cols
        self.df_mov_cols = df_mov_cols
        self.df_parte_cols = df_parte_cols
        self.drop_if_no_number = drop_if_no_number
        self.split_adv = split_adv
        self.parte_levels = parte_levels
        self.advogado = advogado

    def parse(self, df):
        df = self._add_cols_before_split(df)
        df["text"] = self.clean_text(df.text)
        df = self._split_text(df)
        self.text = df.text
        df = self._add_cols(df)
        self.cleaners = {k: v for k, v in self.cleaners.items() if k in df.columns}
        df.loc[:, self.cleaners.keys()] = df.transform(self.cleaners)
        if len(df) == 0:
            return
        proc = self._get_proc(df)
        parte = self._get_parte(df)
        mov = self._get_mov(df)
        out = (proc, parte, mov)
        if self.split_adv:
            parte, adv = self._split_adv(parte)
            out = (proc, parte, mov, adv)
        return out

    def _add_cols_before_split(self, df):
        if self.regexes_before_split:
            cols = extract_regexes(df.text, self.regexes_before_split)
            df = df.join(cols)
        return df

    def _split_text(self, df):
        if self.split_text_on:
            df = split_col(df, "text", split_on=self.split_text_on).reset_index()
        return df

    def _add_cols(self, df):
        cols = extract_regexes(df.text, self.regexes)
        df = df.join(cols)
        if self.drop_if_no_number:
            df = df.loc[df.number.notnull()]
        df["number"] = self.cleaners["number"](df.number)
        df["proc_id"] = clean.generate_id(
            df.number, suffix=self.id_suffix, suffix_length=self.suffix_length
        )
        df["mov_id"] = df.index
        df["mov_id"] = clean.generate_id(
            df.mov_id, suffix=self.id_suffix, suffix_length=self.suffix_length
        )
        return df

    def _get_parte(self, df):
        df_id = keep_cols(df, self.parte_levels + self.df_parte_cols)
        text = df.text
        df = extract_keywords(
            text,
            self.parte,
            max_name_length=self.max_name_length,
            last_name_length=self.last_name_length,
        )
        if len(df) == 0:
            return get_empty_parte()
        df["lastname"] = self.clean_last_parte(df.lastname)
        df = self._split_parte(df)
        df["parte"] = np.where(df["name"] == "", df["lastname"], df["name"])
        df["parte"] = self.clean_parte(df.parte)
        df = self._add_advogado(df, text)
        df["key"] = self.clean_parte_key(df.key)
        df["tipo_parte"] = self.clean_tipo_parte(df.key)
        df["tipo_parte_id"] = clean.transform(
            df.tipo_parte, "tipo_parte", "tipo_parte_id"
        )
        df = self._drop_partes(df)
        df = df.join(df_id)
        idcols = ["parte", "tipo_parte_id"] + self.parte_levels
        df = df.drop_duplicates(idcols)
        df["parte_id"] = clean.generate_id(
            df, by=idcols, suffix=self.id_suffix, suffix_length=self.suffix_length
        )
        cols = [
            "parte_id",
            "parte",
            "tipo_parte",
            "tipo_parte_id",
            "key",
            "oab",
            "name_group",
        ] + self.parte_levels
        df = keep_cols(df, cols + self.df_parte_cols)
        df = df.drop_duplicates("parte_id")  # Just in case
        return df

    def _add_advogado(self, df, text):
        if not self.advogado:
            return add_oab(df)
        adv = text.str.extractall(self.advogado)
        adv = adv.reset_index("match", drop=True)
        adv["key"] = "advogado"
        adv["parte"] = clean.clean_oab(adv.parte)
        df = pd.concat([df, adv])
        return df

    def _split_parte(self, df):
        df = split_col(df, "name", split_on=self.split_parte_on, group_id="name_group")
        df = split_col(
            df, "lastname", split_on=self.split_parte_on, group_id="lastname_group"
        )
        df["name_group"] = np.where(
            df.name_group == "", df.lastname_group, df.name_group
        )
        return df.drop("lastname_group", 1)

    def _drop_partes(self, df):
        df = df.loc[df.parte != ""]
        df = df.loc[
            (df.parte.str.len() > 8)
            | (df.parte == "MP")
            | df.parte.str.match("[0-9]+/[A-Z]{2}")
        ]
        return df

    def _split_adv(self, df):
        cols = {"parte": "advogado"}
        adv = df.rename(columns=cols).reset_index()
        adv.loc[adv.tipo_parte_id == 4, "name_group"] = np.nan
        adv["name_group"] = adv.groupby(self.parte_levels)["name_group"].fillna(
            method="ffill"
        )
        df = df.loc[df.tipo_parte_id != 4].copy()
        adv = adv.loc[adv.tipo_parte_id == 4]
        adv = adv.drop("parte_id", 1)
        df["parte_id"] = clean.generate_id(
            df,
            by=["tipo_parte_id", "parte"] + self.parte_levels,
            suffix=self.id_suffix,
            suffix_length=self.suffix_length,
        )
        cols = ["parte_id", "name_group"]
        adv = df.loc[:, cols].merge(adv, on="name_group")
        cols = ["parte_id", "advogado", "oab"]
        adv = adv.loc[:, cols]
        if "oab" in df.columns:
            df = df.drop("oab", 1)
        df = df.drop_duplicates("parte_id")  # Just in case
        return df, adv

    def _get_keywords(self):
        regex = [c.regex for c in self.keyword_cols]
        if type(self.parte_regex) == str:
            regex += [self.parte_regex]
        else:
            regex += self.parte_regex
        return regex

    def _get_proc(self, df):
        cols = ["proc_id"] + self.df_proc_cols
        df = keep_cols(df, cols)
        proc = _drop_duplicate_procs(df)
        proc = proc.set_index("proc_id")
        proc = self.clean_proc(proc)
        proc = proc.reset_index()
        return proc

    def _get_mov(self, df):
        cols = ["mov_id", "proc_id", "text"]
        mov = keep_cols(df, cols + self.df_mov_cols)
        mov = self.clean_mov(mov)
        return mov


def get_empty_parte():
    df = pd.DataFrame(
        {
            "mov_id": [],
            "proc_id": [],
            "parte_id": [],
            "parte": [],
            "key": [],
            "tipo_parte_id": [],
        }
    )
    return df


def _drop_duplicate_procs(df):
    # Could try
    # proc=df.groupby('proc_id').agg(lambda x: scipy.stats.mode(x)[0])
    # But probably quite slow
    df["nmissing"] = df.isnull().sum(axis=1)
    proc = df.sort_values("nmissing").drop_duplicates("proc_id", keep="first")
    proc = proc.drop("nmissing", 1)
    return proc


def clean_diario_proc(proc, number_types=["CNJ"]):
    proc["tribunal_id"] = clean.transform(proc["tribunal"], "tribunal", "tribunal_id")
    proc["filingyear"] = clean.get_filing_year(proc.number, types=number_types)
    proc["comarca_id"] = clean.get_comarca_id(proc.number)
    return proc


def clean_diario_mov(mov):
    mov["caderno_id"] = clean.get_caderno_id(mov["tribunal"], mov["caderno"])
    mov["line_end"] = pd.to_numeric(mov["line"]) + mov.text.str.count("\n")
    return mov


class DiarioParser(CaseParser):
    def __init__(
        self,
        number_types="CNJ",
        df_mov_cols=["tribunal", "number", "date", "caderno", "line"],
        df_proc_cols=["tribunal", "number", "classe"],
        **kwargs
    ):
        super(DiarioParser, self).__init__(
            df_proc_cols=df_proc_cols,
            df_mov_cols=df_mov_cols,
            clean_proc=lambda x: clean_diario_proc(x, number_types),
            clean_mov=clean_diario_mov,
            **kwargs
        )


def add_oab(df):
    df["oab"] = ""
    df.loc[:, ("parte", "oab")] = split_name_oab(df.parte).values
    return df


def split_name_oab(sr):
    df = sr.str.split(r"(?i)\boab", expand=True, n=1)
    if not 1 in df.columns:
        df[1] = ""
    df[1] = clean.clean_oab(df[1])
    df[0] = clean.clean_text(df[0])
    return df


def split_col(df, name_col, split_on=",|-", group_id=None):
    df = df.reset_index()
    df.index.name = "temp"
    names = df[name_col].fillna("").str.split(split_on, expand=True).stack()
    names.name = name_col
    df = df.drop(columns=name_col)
    df = df.join(names)
    if group_id:
        df[group_id] = df.index.get_level_values("temp")
    return df.set_index("index")


def parse_diario_extract(infile, nchar=None):
    with open(infile, "r") as f:
        text = f.read()
    if nchar:
        text = text[:nchar]
    if text == "":
        return pd.DataFrame()
    tribunal = re.match(".*?/", text).group(0)
    split_regex = "{}(?=[0-9]{{4}}/" "[0-9]{{2}}/[0-9]{{2}}/)".format(tribunal)
    df = (
        pd.Series(re.split(split_regex, text))
        .str.replace(";", "", regex=True)
        .str.replace(r"^([0-9]{4}/[0-9]{2}/[0-9]{2})/", r"\1;", n=3, regex=True)
        .str.replace(r"\.(md|txt)", r";", n=1, regex=True)
        .str.replace(r"(-|:)([0-9]+)(-|:)", r"\2;", n=1, regex=True)
        .str.split(";", expand=True)
    )
    df.columns = ["date", "caderno", "line", "text"]
    df["tribunal"] = tribunal[:-1]
    df["date"] = df["date"].str.replace("/", "-", regex=True)
    return df.loc[df.line.notnull()]


def extract_regexes(
        text, regexes, flags=0,
        extractall=False, axis=1,
        match_index=False,
        update=False
):
    if type(regexes) == str:
        regexes = [regexes]
    if extractall:
        func = lambda x: text.str.extractall(x, flags=flags)
    else:
        func = lambda x: text.str.extract(x, flags=flags)
    if update:
        if axis==0:
            raise(ValueError("Update only implemented for axis=1"))
        df = func(regexes[0])
        for regex in regexes[1:]:
            df2 = func(regex)
            old_cols = set(df.columns).intersection(df2.columns)
            new_cols = set(df2.columns) - set(df.columns)
            df = pd.concat([df, df2[list(new_cols)]], axis=1)
            df.update(df2[list(old_cols)], overwrite=False)
    else:
        df = pd.concat(map(func, regexes), axis=axis, sort=True)
    drop_cols = [c for c in df.columns if isinstance(c, int)]
    df = df.drop(columns=drop_cols)
    if extractall:
        df = df.droplevel("match")
        if match_index:
            df["match"] = df.groupby(df.index).cumcount()
            df.set_index("match", append=True, inplace=True)
    return df


def extract_keywords(text, keywords, max_name_length=100, last_name_length=50):
    regex = get_keyword_regex(keywords, max_name_length, last_name_length)
    df = text.str.extractall(regex)
    df.index = df.index.droplevel(1)
    return df


def get_keyword_regex(keyword, max_name_length=100, last_name_length=50):
    """
    Args:
       keywords: list of keywords or regex
    """
    if type(keyword) == list:
        keyword = "|".join(keyword)
    name = ".{{0,{0}}}?(?={1})".format(max_name_length, keyword)
    last_name = ".{{0,{0}}}".format(last_name_length)
    # The ?s makes sure . includes newline
    regex = ("(?s)(?P<key>{0})" "((?P<name>{1})|(?P<lastname>{2}))").format(
        keyword, name, last_name
    )
    return regex


def keep_cols(df, cols):
    cols = set(cols).intersection(set(df.columns))
    return df.loc[:, cols]


def inspect(
    proc, parte, mov, adv=None, tp="parte", min_mov_length=100, parte_level="mov_id"
):
    if len(mov) == 0:
        return None
    # mov = (
    #    mov.loc[(mov.text.str.len() > min_mov_length) & (mov.number != "")]
    #    .merge(proc.reset_index().loc[:, "proc_id"], on="proc_id")
    #    .merge(parte.loc[:, "proc_id"], on="proc_id", how="left")
    # )
    ex = mov.sample().iloc[0]
    proc_id = ex.proc_id
    print(ex["text"])
    prt = parte.loc[parte[parte_level] == ex[parte_level], ("parte_id", "parte", "key")]
    if tp == "parte":
        print("\n")
        for _, r in prt.iterrows():
            print("{}: {}".format(r.key, r.parte))
            if adv is not None:
                advs = adv.loc[adv.parte_id == r.parte_id]
                for _, r in advs.iterrows():
                    print(" ", r.oab, r.advogado)
    if tp == "proc":
        prc = proc.query("proc_id == {}".format(proc_id)).iloc[0]
        print(prc)
    if tp == "mov":
        print(ex)
    if tp == "all":
        prc = proc.query("proc_id == {}".format(proc_id)).iloc[0]
        print(prt)
        print(ex)
        print(prc)
    return ex
