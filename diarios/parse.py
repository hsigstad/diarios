"""Court case and diary parsers for extracting structured data from text."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
import re
import diarios.clean as clean


class CaseParser:
    """Class to parse court cases"""

    def __init__(
        self,
        regexes: List[str] = [r"(?P<number>[0-9.\-]{19,30})"],
        regexes_before_split: Optional[List[str]] = None,
        cleaners: Dict[str, Callable[[pd.Series], pd.Series]] = {"number": clean.clean_number},
        parte: str = "AUTOR:|RÉU:",
        split_parte_on: str = ",|-|;",
        split_text_on: Optional[str] = None,
        id_suffix: Optional[str] = None,
        suffix_length: int = 2,
        clean_text: Callable[[pd.Series], pd.Series] = clean.clean_diario_text,
        clean_parte: Callable[[pd.Series], pd.Series] = clean.clean_parte,
        clean_last_parte: Callable[[pd.Series], pd.Series] = lambda x: x,
        clean_parte_key: Callable[[pd.Series], pd.Series] = clean.clean_parte_key,
        clean_tipo_parte: Callable[[pd.Series], pd.Series] = clean.clean_tipo_parte,
        max_name_length: int = 100,
        last_name_length: int = 50,
        clean_proc: Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x,
        clean_mov: Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x,
        df_proc_cols: List[str] = [],
        df_mov_cols: List[str] = [],
        df_parte_cols: List[str] = [],
        drop_if_no_number: bool = True,
        parte_levels: List[str] = ["mov_id", "proc_id"],
        advogado: Optional[str] = None,
        split_adv: bool = False,
    ) -> None:
        """Initialize the case parser with regex patterns and cleaning functions.

        Args:
            regexes: Regex patterns for extracting case number and other fields.
            regexes_before_split: Regex patterns applied before splitting text.
            cleaners: Mapping of column names to cleaning functions.
            parte: Regex pattern for identifying party types.
            split_parte_on: Delimiter pattern for splitting party names.
            split_text_on: Delimiter pattern for splitting text entries.
            id_suffix: Suffix appended to generated IDs.
            suffix_length: Length of the ID suffix.
            clean_text: Function to clean raw diary text.
            clean_parte: Function to clean party names.
            clean_last_parte: Function to clean the last part of party names.
            clean_parte_key: Function to clean party type keys.
            clean_tipo_parte: Function to clean party type labels.
            max_name_length: Maximum character length for extracted names.
            last_name_length: Maximum character length for last names.
            clean_proc: Function to clean the process DataFrame.
            clean_mov: Function to clean the movement DataFrame.
            df_proc_cols: Extra columns to keep in the process DataFrame.
            df_mov_cols: Extra columns to keep in the movement DataFrame.
            df_parte_cols: Extra columns to keep in the party DataFrame.
            drop_if_no_number: Whether to drop rows without a case number.
            parte_levels: Index levels used for party deduplication.
            advogado: Regex pattern for extracting lawyer information.
            split_adv: Whether to split lawyers into a separate DataFrame.
        """
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

    def parse(
        self, df: pd.DataFrame
    ) -> Optional[Union[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame], Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]]:
        """Parse a DataFrame of diary text into process, party, and movement tables.

        Args:
            df: DataFrame with a ``text`` column containing raw diary entries.

        Returns:
            Tuple of (proc, parte, mov) DataFrames, or (proc, parte, mov, adv)
            if ``split_adv`` is True. Returns None if the input is empty.
        """
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

    def _add_cols_before_split(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply pre-split regexes and join extracted columns."""
        if self.regexes_before_split:
            cols = extract_regexes(df.text, self.regexes_before_split)
            df = df.join(cols)
        return df

    def _split_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split the text column if a split pattern is configured."""
        if self.split_text_on:
            df = split_col(df, "text", split_on=self.split_text_on).reset_index()
        return df

    def _add_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract regex columns and generate process/movement IDs."""
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

    def _get_parte(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and clean party information from parsed text."""
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

    def _add_advogado(self, df: pd.DataFrame, text: pd.Series) -> pd.DataFrame:
        """Add lawyer (advogado) and OAB information to the party DataFrame."""
        if not self.advogado:
            return add_oab(df)
        adv = text.str.extractall(self.advogado)
        adv = adv.reset_index("match", drop=True)
        adv["key"] = "advogado"
        adv["parte"] = clean.clean_oab(adv.parte)
        df = pd.concat([df, adv])
        return df

    def _split_parte(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split party name and lastname columns on the configured delimiter."""
        df = split_col(df, "name", split_on=self.split_parte_on, group_id="name_group")
        df = split_col(
            df, "lastname", split_on=self.split_parte_on, group_id="lastname_group"
        )
        df["name_group"] = np.where(
            df.name_group == "", df.lastname_group, df.name_group
        )
        return df.drop("lastname_group", 1)

    def _drop_partes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove invalid or too-short party names."""
        df = df.loc[df.parte != ""]
        df = df.loc[
            (df.parte.str.len() > 8)
            | (df.parte == "MP")
            | df.parte.str.match("[0-9]+/[A-Z]{2}")
        ]
        return df

    def _split_adv(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Separate lawyer rows from the party DataFrame.

        Args:
            df: Party DataFrame containing both parties and lawyers.

        Returns:
            Tuple of (parties DataFrame, lawyers DataFrame).
        """
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

    def _get_keywords(self) -> List[str]:
        """Build the list of keyword regex patterns from columns and party regex."""
        regex = [c.regex for c in self.keyword_cols]
        if type(self.parte_regex) == str:
            regex += [self.parte_regex]
        else:
            regex += self.parte_regex
        return regex

    def _get_proc(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract the process DataFrame and apply cleaning."""
        cols = ["proc_id"] + self.df_proc_cols
        df = keep_cols(df, cols)
        proc = _drop_duplicate_procs(df)
        proc = proc.set_index("proc_id")
        proc = self.clean_proc(proc)
        proc = proc.reset_index()
        return proc

    def _get_mov(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract the movement DataFrame and apply cleaning."""
        cols = ["mov_id", "proc_id", "text"]
        mov = keep_cols(df, cols + self.df_mov_cols)
        mov = self.clean_mov(mov)
        return mov


def get_empty_parte() -> pd.DataFrame:
    """Return an empty DataFrame with the standard party column schema."""
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


def _drop_duplicate_procs(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate processes, keeping the row with fewest missing values."""
    # Could try
    # proc=df.groupby('proc_id').agg(lambda x: scipy.stats.mode(x)[0])
    # But probably quite slow
    df["nmissing"] = df.isnull().sum(axis=1)
    proc = df.sort_values("nmissing").drop_duplicates("proc_id", keep="first")
    proc = proc.drop("nmissing", 1)
    return proc


def clean_diario_proc(
    proc: pd.DataFrame, number_types: List[str] = ["CNJ"]
) -> pd.DataFrame:
    """Add tribunal ID, filing year, and comarca ID to the process DataFrame.

    Args:
        proc: Process DataFrame with ``tribunal`` and ``number`` columns.
        number_types: Number format types used for filing year extraction.

    Returns:
        Process DataFrame with added identifier columns.
    """
    proc["tribunal_id"] = clean.transform(proc["tribunal"], "tribunal", "tribunal_id")
    proc["filingyear"] = clean.get_filing_year(proc.number, types=number_types)
    proc["comarca_id"] = clean.get_comarca_id(proc.number)
    return proc


def clean_diario_mov(mov: pd.DataFrame) -> pd.DataFrame:
    """Add caderno ID and line-end position to the movement DataFrame.

    Args:
        mov: Movement DataFrame with ``tribunal``, ``caderno``, ``line``,
            and ``text`` columns.

    Returns:
        Movement DataFrame with ``caderno_id`` and ``line_end`` columns added.
    """
    mov["caderno_id"] = clean.get_caderno_id(mov["tribunal"], mov["caderno"])
    mov["line_end"] = pd.to_numeric(mov["line"]) + mov.text.str.count("\n")
    return mov


class DiarioParser(CaseParser):
    """Parser specialized for official diary (Diario) court publications."""

    def __init__(
        self,
        number_types: Union[str, List[str]] = "CNJ",
        df_mov_cols: List[str] = ["tribunal", "number", "date", "caderno", "line"],
        df_proc_cols: List[str] = ["tribunal", "number", "classe"],
        **kwargs: Any,
    ) -> None:
        """Initialize the diary parser with default columns and cleaners.

        Args:
            number_types: Case number format types (e.g. ``"CNJ"``).
            df_mov_cols: Columns to keep in the movement DataFrame.
            df_proc_cols: Columns to keep in the process DataFrame.
            **kwargs: Additional keyword arguments passed to ``CaseParser``.
        """
        super(DiarioParser, self).__init__(
            df_proc_cols=df_proc_cols,
            df_mov_cols=df_mov_cols,
            clean_proc=lambda x: clean_diario_proc(x, number_types),
            clean_mov=clean_diario_mov,
            **kwargs
        )


def add_oab(df: pd.DataFrame) -> pd.DataFrame:
    """Add an ``oab`` column by splitting OAB numbers from party names."""
    df["oab"] = ""
    df.loc[:, ("parte", "oab")] = split_name_oab(df.parte).values
    return df


def split_name_oab(sr: pd.Series) -> pd.DataFrame:
    """Split a Series of names on the 'OAB' token into name and OAB number.

    Args:
        sr: Series of party name strings potentially containing OAB numbers.

    Returns:
        DataFrame with two columns: cleaned name (0) and OAB number (1).
    """
    df = sr.str.split(r"(?i)\boab", expand=True, n=1)
    if not 1 in df.columns:
        df[1] = ""
    df[1] = clean.clean_oab(df[1])
    df[0] = clean.clean_text(df[0])
    return df


def split_col(
    df: pd.DataFrame,
    name_col: str,
    split_on: str = ",|-",
    group_id: Optional[str] = None,
) -> pd.DataFrame:
    """Split a column into multiple rows on a regex delimiter.

    Args:
        df: Input DataFrame.
        name_col: Name of the column to split.
        split_on: Regex pattern used as the delimiter.
        group_id: If provided, adds a column with the original row index.

    Returns:
        DataFrame with the split column expanded into separate rows.
    """
    df = df.reset_index()
    df.index.name = "temp"
    names = df[name_col].fillna("").str.split(split_on, expand=True).stack()
    names.name = name_col
    df = df.drop(columns=name_col)
    df = df.join(names)
    if group_id:
        df[group_id] = df.index.get_level_values("temp")
    return df.set_index("index")


def parse_diario_extract(
    infile: str, nchar: Optional[int] = None
) -> pd.DataFrame:
    """Parse a diary extract file into a structured DataFrame.

    Args:
        infile: Path to the diary extract text file.
        nchar: If provided, truncate the file contents to this many characters.

    Returns:
        DataFrame with ``date``, ``caderno``, ``line``, ``text``, and
        ``tribunal`` columns.
    """
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
    text: pd.Series,
    regexes: Union[str, List[str]],
    flags: int = 0,
    extractall: bool = False,
    axis: int = 1,
    match_index: bool = False,
    update: bool = False,
) -> pd.DataFrame:
    """Apply one or more regex patterns to a text Series and return extracted groups.

    Args:
        text: Series of strings to extract from.
        regexes: One or more regex patterns with named groups.
        flags: Regex flags passed to ``str.extract`` / ``str.extractall``.
        extractall: If True, use ``extractall`` instead of ``extract``.
        axis: Concatenation axis when combining results from multiple regexes.
        match_index: If True and ``extractall``, add a ``match`` level to the index.
        update: If True, overlay results so earlier regexes take priority.

    Returns:
        DataFrame with columns corresponding to named groups in the regexes.

    Raises:
        ValueError: If ``update`` is True and ``axis`` is 0.
    """
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


def extract_keywords(
    text: pd.Series,
    keywords: str,
    max_name_length: int = 100,
    last_name_length: int = 50,
) -> pd.DataFrame:
    """Extract keyword-delimited party names from text.

    Args:
        text: Series of strings to search.
        keywords: Regex pattern matching party-type keywords.
        max_name_length: Maximum characters for the name capture group.
        last_name_length: Maximum characters for the lastname capture group.

    Returns:
        DataFrame with ``key``, ``name``, and ``lastname`` columns.
    """
    regex = get_keyword_regex(keywords, max_name_length, last_name_length)
    df = text.str.extractall(regex)
    df.index = df.index.droplevel(1)
    return df


def get_keyword_regex(
    keyword: Union[str, List[str]],
    max_name_length: int = 100,
    last_name_length: int = 50,
) -> str:
    """Build a regex for extracting names around keyword delimiters.

    Args:
       keyword: Keyword pattern or list of keyword patterns.
       max_name_length: Maximum characters for the name capture group.
       last_name_length: Maximum characters for the lastname capture group.

    Returns:
        Compiled regex string with named groups ``key``, ``name``, and ``lastname``.
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


def keep_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Subset a DataFrame to the intersection of requested and existing columns."""
    cols = set(cols).intersection(set(df.columns))
    return df.loc[:, cols]


def inspect(
    proc: pd.DataFrame,
    parte: pd.DataFrame,
    mov: pd.DataFrame,
    adv: Optional[pd.DataFrame] = None,
    tp: str = "parte",
    min_mov_length: int = 100,
    parte_level: str = "mov_id",
) -> Optional[pd.Series]:
    """Sample a random movement and print associated parties or process info.

    Args:
        proc: Process DataFrame.
        parte: Party DataFrame.
        mov: Movement DataFrame.
        adv: Optional lawyer DataFrame.
        tp: Display mode; one of ``"parte"``, ``"proc"``, ``"mov"``, ``"all"``.
        min_mov_length: Minimum text length (currently unused).
        parte_level: Column used to join parties to movements.

    Returns:
        The sampled movement row as a Series, or None if ``mov`` is empty.
    """
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
