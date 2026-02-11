"""DecisionParser class and text helpers for court ruling analysis."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from diarios.clean import clean_text
from diarios.clean import map_regex
from diarios.clean import extractall_series
from diarios.clean import split_series
from diarios.clean import extract_number
from diarios.parse import extract_regexes

from diarios.decision.config import (
    _extract_regexes,
    get_main_sentence_regexes,
    get_dispositivo_regexes,
    get_desfecho_regexes,
    get_mode,
    get_key_order,
    get_subject,
    get_split_keys,
    get_pena_regexes,
    intersect,
    remove_pena_base,
    print_truncated,
)

__all__ = [
    "DecisionParser",
    "clean_sentenca_text",
]


def _clean_text(
    text: pd.Series,
    replace_text: Dict[str, str],
    remove_dots: List[str],
    remove_regexes: List[str],
) -> pd.Series:
    """Clean text by applying replacements, dot removals, and regex removals.

    Args:
        text: Series of text strings to clean.
        replace_text: Mapping of regex patterns to replacement strings.
        remove_dots: Patterns whose trailing dots should be removed.
        remove_regexes: Regex patterns to remove from text.

    Returns:
        Cleaned text series with name set to ``'text'``.
    """
    for k, v in replace_text.items():
        text = text.str.replace(k, v, regex=True)
    for r in remove_dots:
        text = text.str.replace(f'({r})\.', r'\1', regex=True)
    for regex in remove_regexes:
        text = text.str.replace(regex, '', regex=True)
        text = text.str.replace(regex, '', regex=True) # If matches twice
    text.name = 'text'
    return text


def clean_sentenca_text(text: pd.Series) -> pd.Series:
    """Clean sentence text keeping only alphanumeric and basic punctuation."""
    return clean_text(text, drop="^A-Z0-9;:., \n\-")


class DecisionParser:
    """Class to parse decisions"""

    def __init__(
            self,
            text: pd.Series,
            parte: Optional[pd.Series] = None,
            tipo_parte: Optional[pd.Series] = None,
            classes: Optional[List[str]] = None,
            replace_text: Optional[Dict[str, str]] = None,
            remove_dots: Optional[List[str]] = None,
            remove_regexes: Optional[List[str]] = None,
            key_order: Optional[List[str]] = None,
            name_match_single_parte: bool = False,
            split_desfecho: bool = True,
            main_sentence_regexes: Optional[List[str]] = None,
            get_desfecho_regexes: Callable[..., Dict[str, str]] = get_desfecho_regexes,
            alternative_parte_regexes: Optional[Dict[str, str]] = None,
            dispositivo_regexes: Optional[List[str]] = None,
            all_partes_regexes: Optional[Union[List[str], Dict[Any, List[str]]]] = None,
            more_regexes: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        """Initialize the decision parser.

        Args:
            text: Series of ruling texts to parse.
            parte: Series of party names, if available.
            tipo_parte: Series of party types, if available.
            classes: List of case class identifiers.
            replace_text: Regex replacements to apply during cleaning.
            remove_dots: Patterns whose trailing dots should be removed.
            remove_regexes: Regex patterns to remove from text.
            key_order: Priority order for decision keys when deduplicating.
            name_match_single_parte: Whether to use name matching for single parties.
            split_desfecho: Whether to split outcome into verb and object.
            main_sentence_regexes: Regexes for extracting main sentences.
            get_desfecho_regexes: Callable returning outcome regexes for classes.
            alternative_parte_regexes: Alternate name patterns for parties.
            dispositivo_regexes: Regexes for extracting the dispositivo.
            all_partes_regexes: Patterns matching references to all parties.
            more_regexes: Additional regex mappings to extract.
        """
        if classes is None:
            classes = ["ProOrd", "ACIA", "APN", "ED", "Ap"]
        if replace_text is None:
            replace_text = {r'\.([0-9]{2})\b': r',\1'}
        if remove_dots is None:
            remove_dots = [r'\barts?', r'\bfls?', '[0-9]', r'\bn', r'\bc', r'\bcc']
        if remove_regexes is None:
            remove_regexes = [
                '(?s)(?i)conden[^.]{0,20}honor[^.]{0,10}adv',
                '(?s)(?i)conden[^.]{0,20}custa[^.]{0,10}proc',
            ]
        if key_order is None:
            key_order = ['ABSOLVO', 'PRESCRICAO', 'CONDENO']
        if main_sentence_regexes is None:
            main_sentence_regexes = get_main_sentence_regexes()
        if alternative_parte_regexes is None:
            alternative_parte_regexes = {'MINISTERIO.*PUBLICO': 'MP|MPF|MINISTERIO PUBLICO'}
        if dispositivo_regexes is None:
            dispositivo_regexes = get_dispositivo_regexes()
        if all_partes_regexes is None:
            all_partes_regexes = [r'\bOS REUS\b', r'\bOS REQUERIDOS\b', r'\bOS ACUSADOS']
        if more_regexes is None:
            more_regexes = {
                'mode': get_mode(),
                'subject': get_subject()
            }
        self.text = _clean_text(text, replace_text, remove_dots, remove_regexes)
        self.main_sentence_regexes = main_sentence_regexes
        self.more_regexes = more_regexes
        self.all_partes_regexes = all_partes_regexes
        if parte is not None:
            ix = parte.index.intersection(text.index)
            self.parte = parte.loc[ix]
            self.text = self.text.loc[ix]
        self.tipo_parte = tipo_parte
        self.dispositivo_regexes = dispositivo_regexes
        self.alternative_parte_regexes = alternative_parte_regexes
        self.classes = classes
        self.text_pena_cols = ['regime', 'multa_unit']
        self.split_desfecho = split_desfecho
        self.key_order = key_order
        self.name_match_single_parte = name_match_single_parte
        self.get_desfecho_regexes = get_desfecho_regexes

    def parse(self) -> pd.DataFrame:
        """Parse main sentences and extract outcomes.

        Returns:
            DataFrame with main sentence, outcome, and additional regex matches.
        """
        df = pd.DataFrame(index=self.text.index)
        df['main_sentence'] = _extract_regexes(
            self.text,
            self.main_sentence_regexes
        )
        df['main_sentence'] = clean_text(df.main_sentence)
        regexes = self.get_desfecho_regexes(self.classes)
        desfecho = map_regex(
            df.main_sentence,
            regexes,
            keep_unmatched=False
        )
        if self.split_desfecho:
            desfecho = desfecho.str.split('-', expand=True)
            desfecho.columns = ['verb', 'object']
        else:
            desfecho = desfecho.str.replace('-', ' ').str.strip()
            desfecho.name = 'desfecho'
        df = df.join(desfecho, how='left')
        for k, v in self.more_regexes.items():
            df[k] = map_regex(df.main_sentence, v, keep_unmatched=False)
        self.parsed = df
        return df

    def parse_parte(self) -> pd.DataFrame:
        """Parse party-level outcomes from the dispositivo.

        Returns:
            DataFrame with per-party parsed outcomes and penalties.
        """
        self.dispositivo = clean_sentenca_text(
            _extract_regexes(
                self.text,
                self.dispositivo_regexes
            )
        )
        if self.name_match_single_parte:
            parsed_parte = self._parse_multiple_partes(self.dispositivo)
        else:
            single_parte = self.parte.groupby(self.parte.index.names).size() == 1
            parsed1 = self._parse_single_parte(self.dispositivo.loc[single_parte])
            parsed2 = self._parse_multiple_partes(self.dispositivo.loc[~single_parte])
            parsed_parte = pd.concat([parsed1, parsed2])
        parsed_parte = self._drop_duplicate_parte(parsed_parte)
        self.parsed_parte = parsed_parte
        return parsed_parte

    def _parse_single_parte(self, text: pd.Series) -> pd.DataFrame:
        """Parse outcomes when there is a single party per case.

        Args:
            text: Dispositivo text for single-party cases.

        Returns:
            DataFrame with decision key, penalties, and party name.
        """
        keys = get_split_keys(self.classes)
        df = pd.DataFrame({'text': text})
        df['key'] = map_regex(text, keys, keep_unmatched=False)
        df = df.query('key.notnull()')
        df = self._add_penas(df)
        df['parte'] = self.parte.loc[
            self.parte.index.drop_duplicates(keep=False)
        ]
        return df

    def _add_penas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract penalty information and add columns to the DataFrame.

        Args:
            df: DataFrame with a ``'text'`` column containing dispositivo text.

        Returns:
            DataFrame enriched with penalty columns.
        """
        regexes, boolean_regexes = get_pena_regexes(self.classes)
        if "APN" in self.classes:
            df['text'] = remove_pena_base(df.text)
        pena = extract_regexes(df.text, regexes, update=True)
        df = df.join(pena)
        num_cols = self._get_pena_cols(df, numeric_only=True)
        for c in num_cols:
            if c in df.columns:
                df[c] = extract_number(df[c])

        for k, v in boolean_regexes.items():
            df[k] = df.text.str.contains(v) * 1
        df = self._clean_penas(df)
        return df

    def _clean_penas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean penalty columns by nullifying non-conviction rows and filling NAs.

        Args:
            df: DataFrame with penalty columns.

        Returns:
            Cleaned DataFrame.
        """
        if intersect(["ACIA", "APN"], self.classes):
            pena_cols = self._get_pena_cols(df)
            for c in pena_cols:
                if c in df.columns:
                    df.loc[df.key != "CONDENO", c] = pd.NA
        if "APN" in self.classes:
            df.loc[df.regime.fillna('').str.contains('SEMI'), 'regime'] = "SEMIABERTO"
            df['prisao_dias'] = df.years.fillna(0)*365 + df.months.fillna(0)*30 + df.days.fillna(0)
        df = _fill_na(df)
        return df

    def _parse_multiple_partes(self, text: pd.Series) -> pd.DataFrame:
        """Parse outcomes when there are multiple parties per case.

        Args:
            text: Dispositivo text for multi-party cases.

        Returns:
            DataFrame with per-party decision keys and penalties.
        """
        self.splitted = self._split_on_key(text)
        splitted = self._add_parte_regex(self.splitted)
        df = extractall_series(splitted.text, splitted.parte_regex)
        df = df.join(splitted.key, how='inner')
        df = self._add_penas(df)
        df = self._bfill_penas(df)
        df = self._add_all_partes(df)
        df = df.reset_index(['group', 'match'], drop=True)
        return df

    def _add_all_partes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand collective party references to individual parties.

        Args:
            df: DataFrame with a ``'parte'`` column that may contain collective
                references (e.g. ``'OS REUS'``).

        Returns:
            DataFrame with collective references replaced by individual parties.
        """
        # Changes OS REUS etc to all partes
        if self.all_partes_regexes is None:
            return df
        if isinstance(self.all_partes_regexes, list):
            return self._add_all_partes_sub(df)
        if isinstance(self.all_partes_regexes, dict):
            if self.tipo_parte is None:
                raise("Tipo parte must be specified")
            tipos_parte = self.tipo_parte.drop_duplicates().to_list()
            func = lambda x: self._add_all_partes_sub(df, x)
            return pd.concat(map(func, tipos_parte))

    def _add_all_partes_sub(
        self,
        df: pd.DataFrame,
        tipo_parte: Optional[Any] = None,
    ) -> pd.DataFrame:
        """Replace collective party references for a specific party type.

        Args:
            df: DataFrame with party data.
            tipo_parte: Party type to filter on, or ``None`` for all.

        Returns:
            DataFrame with expanded party rows.
        """
        ix = df.index.names[0]
        df = df.reset_index()
        if tipo_parte is None:
            parte = self.parte.copy()
            all_parte_regex = '|'.join(self.all_partes_regexes)
        else:
            parte = self.parte.loc[self.tipo_parte==tipo_parte].copy()
            all_parte_regex = '|'.join(self.all_partes_regexes[tipo_parte])
        parte = parte.reset_index()
        has_all = df.parte.str.contains(all_parte_regex)
        pena_cols = self._get_pena_cols(df, numeric_only=True)
        all_partes = df.loc[has_all].drop(columns='parte')
        remaining = df.loc[~has_all]
        remaining['no_pena'] = df[pena_cols].sum(axis=1) == 0
        no_pena = (remaining.groupby(ix).no_pena.sum() > 0).reset_index()
        all_partes = all_partes.merge(no_pena, on=ix, how='left')
        all_partes['no_pena'] = all_partes.no_pena.fillna(True)
        all_partes = all_partes.query('no_pena')
        if len(all_partes) > 0:
            all_partes = all_partes.merge(parte, on=ix)
        df = pd.concat([
            df.loc[~has_all],
            all_partes
        ])
        df = df.drop(columns='no_pena')
        df = df.set_index([ix, 'group', 'match'])
        return df

    def _get_pena_cols(self, df: pd.DataFrame, numeric_only: bool = False) -> List[str]:
        """Get penalty column names present in the DataFrame.

        Args:
            df: DataFrame to inspect.
            numeric_only: If ``True``, exclude text penalty columns.

        Returns:
            List of penalty column names.
        """
        regexes, boolean_regexes = get_pena_regexes(self.classes)
        pena_cols = list(set(
            list(boolean_regexes.keys()) +
            pd.Series(regexes).str.extractall('<(.*?)>')[0].tolist()
        ))
        pena_cols = set(df.columns).intersection(pena_cols)
        if numeric_only:
            pena_cols = pena_cols - set(self.text_pena_cols)
        return list(pena_cols)

    def _bfill_penas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Backward-fill penalty values for consecutive name-only rows.

        Args:
            df: DataFrame with penalty columns.

        Returns:
            DataFrame with backward-filled penalties.
        """
        # Backward fill penas in cases like:
        # CONDENO FRANCISCO SOARES E PEDRO SOUZA ...
        just_name = df.text.str.len() - df.parte.str.len() < 5
        condeno = df.key == 'CONDENO'
        pena_cols = self._get_pena_cols(df)
        df.loc[just_name & condeno, pena_cols] = pd.NA
        for col in pena_cols:
            df[col] = df[col].bfill()
        return df

    def _add_parte_regex(self, splitted: pd.DataFrame) -> pd.DataFrame:
        """Attach party-matching regex to the splitted DataFrame.

        Args:
            splitted: DataFrame from ``_split_on_key``.

        Returns:
            DataFrame with a ``'parte_regex'`` column joined in.
        """
        parte_regex = self._get_parte_regex()
        parte_regex = self._add_all_partes_regex(parte_regex)
        parte_regex = (
            r"(?s)(?P<text>\b(?P<parte>" + parte_regex + r")\b.*?)" +
            "(?=" + parte_regex + "|$)"
        )
        return splitted.join(parte_regex, how='inner')

    def _add_all_partes_regex(self, parte_regex: pd.Series) -> pd.Series:
        """Append collective party patterns to the party regex.

        Args:
            parte_regex: Series of party regex patterns.

        Returns:
            Updated series with collective patterns appended.
        """
        apr = self.all_partes_regexes
        if apr is None:
            return parte_regex
        if isinstance(apr, dict):
            apr = [j for i in apr.values() for j in i]
        all_partes = '|'.join(apr)
        return parte_regex + f'|{all_partes}'

    def _get_parte_regex(self) -> pd.Series:
        """Build per-case regex patterns that match party names.

        Returns:
            Series of regex patterns indexed by case, named ``'parte_regex'``.
        """
        parte = clean_text(self.parte) + '|'
        if self.alternative_parte_regexes is not None:
            mapping = {k: f'{v}|' for k, v in self.alternative_parte_regexes.items()}
            mapped = map_regex(parte, mapping, keep_unmatched=False)
            parte = parte + mapped.fillna('')
        parte = parte.loc[parte.notnull()]
        regex = parte.groupby(parte.index.names).sum()
        regex = regex.str.replace(r"\|$", "", regex=True)
        regex.name = 'parte_regex'
        return regex

    def _split_on_key(self, text: pd.Series) -> pd.DataFrame:
        """Split dispositivo text on decision keywords.

        Args:
            text: Series of dispositivo text.

        Returns:
            DataFrame with ``'text'`` and ``'key'`` columns per split segment.
        """
        # Splits on condeno, absolvo, etc
        keys = get_split_keys(self.classes)
        regex = r"(?i)\b(?P<key>{})\b".format('|'.join(keys.keys()))
        df = split_series(text, regex, drop_end=True)
        df['key'] = map_regex(df.key, keys)
        return df

    def _drop_duplicate_parte(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate party rows, keeping the most informative one.

        Args:
            df: DataFrame with possible duplicate party entries.

        Returns:
            Deduplicated DataFrame.
        """
        pena_cols = self._get_pena_cols(df, numeric_only=True)
        df['penas'] = df[pena_cols].sum(axis=1)
        df['key_order'] = get_key_order(df.key, self.key_order)
        df['has_key'] = df.key.notnull() # Should not be necessary
        sort_cols = ['has_key', 'penas', 'key_order']
        ix_names = df.index.names
        df = (
            df
            .sort_values(sort_cols)
            .reset_index()
            .drop_duplicates(ix_names + ['parte'], keep='last')
            .set_index(ix_names)
            .drop(columns=['penas', 'key_order', 'has_key'])
        )
        return df

    def test(
        self,
        regex: Optional[str] = None,
        max_str: int = 2000,
        max_str_sentence: int = 1000,
    ) -> Optional[Any]:
        """Print a random parsed decision for manual inspection.

        Args:
            regex: Optional filter to select texts containing this pattern.
            max_str: Maximum characters to print for full text.
            max_str_sentence: Maximum characters to print for the main sentence.

        Returns:
            The sampled index value, or ``None`` if no text was found.
        """
        if regex:
            text = self.text.loc[self.text.fillna('').str.contains(regex)]
        else:
            text = self.text
        sm = text.sample().index[0]
        parsed = self.parsed.loc[sm]
        try:
            print("TEXT:", self.text.loc[sm][-max_str:])
        except TypeError:
            print("No text")
            return
        print('')
        print("MAIN SENTENCE:", parsed.main_sentence[0:max_str_sentence])
        print('')
        desfecho = ' '.join(
            parsed
            .drop('main_sentence')
            .fillna('')
            .tolist()
        )
        print("DESFECHO:", desfecho)
        return sm

    def test_parte(
        self,
        ix: Optional[Any] = None,
        regex: Optional[str] = None,
        max_str: int = 2000,
        max_str_dispositivo: int = 1000,
        max_str_pena: int = 1000,
    ) -> Optional[Any]:
        """Print a random party-level parsed decision for manual inspection.

        Args:
            ix: Specific index to inspect instead of sampling randomly.
            regex: Optional filter to select texts containing this pattern.
            max_str: Maximum characters to print for full text.
            max_str_dispositivo: Maximum characters to print for the dispositivo.
            max_str_pena: Maximum characters to print for penalty text.

        Returns:
            The sampled or provided index value, or ``None`` if no text was found.
        """
        if regex:
            text = self.text.loc[self.text.fillna('').str.contains(regex)]
        else:
            text = self.text
        if ix:
            sm = ix
        else:
            sm = text.sample().index[0]
        try:
            print("TEXT:", self.text.loc[sm][-max_str:])
        except TypeError:
            print("No text")
            return
        print('')
        print("DISPOSITIVO")
        print('')
        print_truncated(self.dispositivo.loc[sm], max_str_dispositivo)
        print('')
        try:
            partes = self.parte.loc[sm]
            if type(partes) != str:
                partes = ', '.join(partes.tolist())
            print("PARTES:", partes)
        except KeyError:
            print("NO PARTES")
        print('')
        try:
            print(self.splitted.loc[sm].transpose())
        except KeyError:
            print("Nothing extracted")
        print('')
        print('PENAS')
        print('')
        try:
            penas = self.parsed_parte.loc[sm]
            if isinstance(penas, pd.Series):
                print_truncated(penas.text, max_str_pena)
                print(penas)
            else:
                for _, row in penas.iterrows():
                    print_truncated(row.text, max_str_pena)
                    print(row)
        except KeyError:
            print("Nothing extracted")
        return sm


def _fill_na(df: pd.DataFrame) -> pd.DataFrame:
    """Fill NAs with 0 for numeric columns and empty string for object columns.

    Args:
        df: DataFrame to fill.

    Returns:
        DataFrame with NAs replaced.
    """
    num_cols = df.select_dtypes(include='number').columns
    df[num_cols] = df[num_cols].fillna(0)
    obj_cols = df.select_dtypes(include='object').columns
    df[obj_cols] = df[obj_cols].fillna('')
    return df


if __name__ == '__main__':
    from diarios.decision.config import get_df
    #df, parte = get_df()
    parser = DecisionParser(df.inteiro_teor, parte.parte)
    out = parser.parse_parte()
    sm = parser.test_parte()
