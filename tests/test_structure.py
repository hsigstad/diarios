import unittest
import pandas as pd

from diarios.structure import (
    Structure,
    bold,
    parse_structure_string,
    parse,
)


class TestBold(unittest.TestCase):

    def test_wraps_in_ansi(self):
        result = bold("hello")
        self.assertEqual(result, "\033[1mhello\033[0m")

    def test_empty_string(self):
        result = bold("")
        self.assertEqual(result, "\033[1m\033[0m")


class TestStructureInit(unittest.TestCase):

    def test_default_values(self):
        s = Structure()
        self.assertIsNone(s.key)
        self.assertIsNone(s.regex)
        self.assertIsNone(s.text)
        self.assertIsNone(s.children)
        self.assertFalse(s.multiple_matches)
        self.assertEqual(s.level, 0)

    def test_custom_values(self):
        s = Structure(key="test", regex="abc", level=2)
        self.assertEqual(s.key, "test")
        self.assertEqual(s.regex, "abc")
        self.assertEqual(s.level, 2)


class TestStructureAddChild(unittest.TestCase):

    def test_add_to_none(self):
        s = Structure()
        child = Structure(key="child")
        s.add_child(child)
        self.assertEqual(len(s.children), 1)
        self.assertEqual(s.children[0].key, "child")

    def test_add_multiple(self):
        s = Structure()
        s.add_child(Structure(key="a"))
        s.add_child(Structure(key="b"))
        self.assertEqual(len(s.children), 2)


class TestStructureRepr(unittest.TestCase):

    def test_repr_contains_key(self):
        s = Structure(key="test", regex="pat")
        r = repr(s)
        self.assertIn("test", r)
        self.assertIn("pat", r)

    def test_repr_with_text(self):
        s = Structure(key="test", regex="pat", text="some text content here")
        r = repr(s)
        self.assertIn("some text", r)


class TestParseStructureString(unittest.TestCase):

    def test_basic_structure(self):
        structure_string = """
aa,A
bb,B"""
        s = parse_structure_string(structure_string)
        self.assertIsNotNone(s.children)
        self.assertEqual(len(s.children), 2)
        self.assertEqual(s.children[0].key, "aa")
        self.assertEqual(s.children[0].regex, "A")

    def test_nested_structure(self):
        # Need >=2 top-level elements for _build_structure_tree to work
        structure_string = """
aa,A
 bb,B
 cc,C
dd,D"""
        s = parse_structure_string(structure_string)
        self.assertEqual(len(s.children), 2)
        self.assertIsNotNone(s.children[0].children)
        self.assertEqual(s.children[0].children[0].key, "bb")
        self.assertEqual(s.children[0].children[1].key, "cc")

    def test_multiple_matches(self):
        # Multiple match child needs a sibling top-level element
        structure_string = """
aa,A
 bbX,B
cc,C"""
        s = parse_structure_string(structure_string)
        child = s.children[0].children[0]
        self.assertTrue(child.multiple_matches)


class TestStructureParse(unittest.TestCase):

    def test_simple_parse(self):
        structure_string = """
aa,A
bb,B"""
        s = parse_structure_string(structure_string)
        s.parse("hello A world B end")
        self.assertEqual(s.children[0].regex_match, "A")
        self.assertEqual(s.children[1].regex_match, "B")

    def test_parse_function(self):
        structure_string = """
aa,A
bb,B"""
        s = parse("hello A world B end", structure_string)
        self.assertEqual(s.children[0].regex_match, "A")

    def test_nested_parse(self):
        structure_string = """
aa,A
 bb,B
 cc,C
dd,D"""
        s = parse_structure_string(structure_string)
        s.parse("prefix A stuff B inner C tail D end")
        self.assertEqual(s.children[0].regex_match, "A")
        self.assertEqual(s.children[0].children[0].regex_match, "B")
        self.assertEqual(s.children[0].children[1].regex_match, "C")


class TestStructureExtract(unittest.TestCase):

    def test_extract_leaf(self):
        structure_string = """
aa,A
 bb,B
 cc,C
dd,D"""
        s = parse_structure_string(structure_string)
        s.parse("prefix A stuff B inner C tail D end")
        result = s.extract(["aa", "bb"])
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["key"], "bb")

    def test_extract_wildcard(self):
        structure_string = """
aa,A
 bb,B
 cc,C
dd,D"""
        s = parse_structure_string(structure_string)
        s.parse("prefix A stuff B inner C tail D end")
        result = s.extract(["aa", ".*"])
        self.assertEqual(len(result), 2)


class TestStructureToOrg(unittest.TestCase):

    def test_no_match_returns_empty(self):
        s = Structure(key="test", regex="pat")
        self.assertEqual(s.to_org(), "")

    def test_with_match(self):
        s = Structure(key="test", regex="pat", regex_match="matched", text="content", level=0)
        org = s.to_org()
        self.assertIn("* test", org)
        self.assertIn("matched", org)
        self.assertIn("content", org)

    def test_nested_org(self):
        parent = Structure(key="parent", regex="P", regex_match="P", text="", level=0)
        child = Structure(key="child", regex="C", regex_match="C", text="txt", level=1)
        parent.children = [child]
        org = parent.to_org()
        self.assertIn("* parent", org)
        self.assertIn("** child", org)
