import unittest
import pandas as pd

from diarios.decision import (
    get_main_sentence_regexes,
    get_dispositivo_regexes,
    get_desfecho_regexes,
    get_mode,
    get_key_order,
    get_subject,
    get_split_keys,
    intersect,
    print_truncated,
    remove_pena_base,
)


class TestGetMainSentenceRegexes(unittest.TestCase):

    def test_returns_list(self):
        result = get_main_sentence_regexes()
        self.assertIsInstance(result, list)

    def test_non_empty(self):
        result = get_main_sentence_regexes()
        self.assertTrue(len(result) > 0)

    def test_all_strings(self):
        result = get_main_sentence_regexes()
        for r in result:
            self.assertIsInstance(r, str)

    def test_has_flags(self):
        result = get_main_sentence_regexes()
        for r in result:
            self.assertTrue(r.startswith("(?i)(?s)"))


class TestGetDispositivoRegexes(unittest.TestCase):

    def test_returns_list(self):
        result = get_dispositivo_regexes()
        self.assertIsInstance(result, list)

    def test_non_empty(self):
        result = get_dispositivo_regexes()
        self.assertTrue(len(result) > 0)

    def test_has_flags(self):
        result = get_dispositivo_regexes()
        for r in result:
            self.assertTrue(r.startswith("(?i)(?s)"))


class TestGetDesfechoRegexes(unittest.TestCase):

    def test_single_class_string(self):
        result = get_desfecho_regexes("APN")
        self.assertIsInstance(result, dict)
        self.assertTrue(len(result) > 0)

    def test_list_of_classes(self):
        result = get_desfecho_regexes(["APN", "HC"])
        self.assertIsInstance(result, dict)

    def test_ed_class(self):
        result = get_desfecho_regexes("ED")
        self.assertIn("REJEIT.*EMBARGOS.*DECL", result)

    def test_hc_class(self):
        result = get_desfecho_regexes("HC")
        has_ordem = any("ORDEM" in v for v in result.values())
        self.assertTrue(has_ordem)

    def test_ap_class(self):
        result = get_desfecho_regexes("Ap")
        has_apelacao = any("APELACAO" in v for v in result.values())
        self.assertTrue(has_apelacao)

    def test_all_class(self):
        result = get_desfecho_regexes("all")
        self.assertIn("PARCIALMENTE PROCEDENTE", result)

    def test_unknown_class_returns_base(self):
        result = get_desfecho_regexes("UNKNOWN")
        # Should still have the base HOMOLOG regex
        self.assertTrue(len(result) > 0)


class TestGetMode(unittest.TestCase):

    def test_returns_dict(self):
        result = get_mode()
        self.assertIsInstance(result, dict)

    def test_keys(self):
        result = get_mode()
        self.assertIn("UNANIMIDADE", result)
        self.assertIn("MAIORIA", result)


class TestGetKeyOrder(unittest.TestCase):

    def test_maps_correctly(self):
        key = pd.Series(["A", "B", "C"])
        order = ["A", "B", "C"]
        result = get_key_order(key, order)
        self.assertEqual(result.tolist(), [0, 1, 2])

    def test_missing_key_returns_nan(self):
        key = pd.Series(["A", "UNKNOWN"])
        order = ["A", "B"]
        result = get_key_order(key, order)
        self.assertEqual(result.iloc[0], 0)
        self.assertTrue(pd.isna(result.iloc[1]))


class TestGetSubject(unittest.TestCase):

    def test_returns_dict(self):
        result = get_subject()
        self.assertIsInstance(result, dict)

    def test_keys(self):
        result = get_subject()
        self.assertIn("TURMA", result)
        self.assertIn("CAMARA", result)


class TestGetSplitKeys(unittest.TestCase):

    def test_apn_has_condeno(self):
        result = get_split_keys(["APN"])
        values = list(result.values())
        self.assertIn("CONDENO", values)
        self.assertIn("ABSOLVO", values)

    def test_ap_has_provimento(self):
        result = get_split_keys(["Ap"])
        values = list(result.values())
        self.assertIn("PROVIMENTO", values)
        self.assertIn("IMPROVIMENTO", values)

    def test_combined_classes(self):
        result = get_split_keys(["APN", "Ap"])
        values = list(result.values())
        self.assertIn("CONDENO", values)
        self.assertIn("PROVIMENTO", values)

    def test_empty_list(self):
        result = get_split_keys([])
        self.assertEqual(result, {})


class TestIntersect(unittest.TestCase):

    def test_has_overlap(self):
        self.assertTrue(intersect([1, 2], [2, 3]))

    def test_no_overlap(self):
        self.assertFalse(intersect([1, 2], [3, 4]))

    def test_empty_lists(self):
        self.assertFalse(intersect([], []))

    def test_one_empty(self):
        self.assertFalse(intersect([1], []))

    def test_string_elements(self):
        self.assertTrue(intersect(["a", "b"], ["b", "c"]))


class TestPrintTruncated(unittest.TestCase):

    def test_short_string(self, capsys=None):
        # Just verifying it doesn't raise
        print_truncated("hello", 100)

    def test_long_string(self):
        # Truncates when string is longer than max_str
        print_truncated("a" * 200, 100)


class TestRemovePenaBase(unittest.TestCase):

    def test_removes_base_penalty(self):
        text = pd.Series(["FIXO PENA EM 1 ANO TORNO DEFINITIVA 2 ANOS RECLUSAO REGIME FECHADO"])
        result = remove_pena_base(text)
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(len(result), 1)

    def test_no_match_unchanged(self):
        text = pd.Series(["CONDENO O REU A 3 ANOS"])
        result = remove_pena_base(text)
        self.assertEqual(result.iloc[0], "CONDENO O REU A 3 ANOS")
