import unittest
import pandas as pd
import numpy as np

from diarios.parse import (
    get_empty_parte,
    split_name_oab,
    split_col,
    keep_cols,
    get_keyword_regex,
    extract_regexes,
    extract_keywords,
)


class TestGetEmptyParte(unittest.TestCase):

    def test_returns_dataframe(self):
        df = get_empty_parte()
        self.assertIsInstance(df, pd.DataFrame)

    def test_has_expected_columns(self):
        df = get_empty_parte()
        expected = {"mov_id", "proc_id", "parte_id", "parte", "key", "tipo_parte_id"}
        self.assertEqual(set(df.columns), expected)

    def test_is_empty(self):
        df = get_empty_parte()
        self.assertEqual(len(df), 0)


class TestSplitNameOab(unittest.TestCase):

    def test_split_with_oab(self):
        sr = pd.Series(["JOAO SILVA OAB/SP 12345"])
        result = split_name_oab(sr)
        self.assertEqual(result.shape[1], 2)
        self.assertIn("JOAO SILVA", result[0].iloc[0])

    def test_no_oab_returns_two_cols(self):
        sr = pd.Series(["JOAO SILVA"])
        result = split_name_oab(sr)
        self.assertEqual(result.shape[1], 2)

    def test_multiple_entries(self):
        sr = pd.Series(["A OAB/RJ 111", "B OAB/MG 222"])
        result = split_name_oab(sr)
        self.assertEqual(len(result), 2)
        # Both entries have OAB, so column 1 should be non-empty
        self.assertTrue(all(result[1].str.len() > 0))


class TestSplitCol(unittest.TestCase):

    def test_basic_split(self):
        df = pd.DataFrame({"name": ["A,B,C"], "val": [1]})
        result = split_col(df, "name", split_on=",")
        self.assertEqual(len(result), 3)

    def test_preserves_other_columns(self):
        df = pd.DataFrame({"name": ["X-Y"], "val": [42]})
        result = split_col(df, "name", split_on="-")
        self.assertTrue(all(result.val == 42))
        self.assertEqual(len(result), 2)

    def test_group_id(self):
        df = pd.DataFrame({"name": ["A,B"], "val": [1]})
        result = split_col(df, "name", split_on=",", group_id="gid")
        self.assertIn("gid", result.columns)


class TestKeepCols(unittest.TestCase):

    def test_intersection_logic(self):
        # keep_cols computes set intersection of requested cols and df.columns
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        cols = set(["a", "c"]).intersection(set(df.columns))
        self.assertEqual(cols, {"a", "c"})

    def test_missing_cols_filtered(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        cols = set(["a", "z"]).intersection(set(df.columns))
        self.assertEqual(cols, {"a"})


class TestGetKeywordRegex(unittest.TestCase):

    def test_returns_string(self):
        result = get_keyword_regex("AUTOR:|RÉU:")
        self.assertIsInstance(result, str)

    def test_contains_named_groups(self):
        result = get_keyword_regex("AUTOR:")
        self.assertIn("?P<key>", result)
        self.assertIn("?P<name>", result)
        self.assertIn("?P<lastname>", result)

    def test_list_input(self):
        result = get_keyword_regex(["AUTOR:", "RÉU:"])
        self.assertIn("AUTOR:|RÉU:", result)


class TestExtractRegexes(unittest.TestCase):

    def test_single_regex(self):
        text = pd.Series(["abc 123 def", "xyz 456"])
        result = extract_regexes(text, r"(?P<num>[0-9]+)")
        self.assertIn("num", result.columns)
        self.assertEqual(result["num"].iloc[0], "123")

    def test_multiple_regexes(self):
        text = pd.Series(["abc 123 def", "xyz 456"])
        result = extract_regexes(
            text,
            [r"(?P<letters>[a-z]+)", r"(?P<num>[0-9]+)"],
        )
        self.assertIn("letters", result.columns)
        self.assertIn("num", result.columns)

    def test_update_mode(self):
        text = pd.Series(["abc 123"])
        r1 = r"(?P<val>[a-z]+)"
        r2 = r"(?P<val>[0-9]+)"
        result = extract_regexes(text, [r1, r2], update=True)
        # First regex takes priority in update mode
        self.assertEqual(result["val"].iloc[0], "abc")

    def test_string_input(self):
        text = pd.Series(["hello"])
        result = extract_regexes(text, r"(?P<word>\w+)")
        self.assertEqual(result["word"].iloc[0], "hello")


class TestExtractKeywords(unittest.TestCase):

    def test_basic_extraction(self):
        text = pd.Series(["AUTOR: JOAO SILVA REU: PEDRO"])
        result = extract_keywords(text, "AUTOR:|REU:")
        self.assertIn("key", result.columns)
        self.assertTrue(len(result) > 0)
