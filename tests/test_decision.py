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
    split_sentenca_sections,
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


class TestSplitSentencaSections(unittest.TestCase):

    def test_three_section_split(self):
        text = (
            "Trata-se de ação civil pública ajuizada pelo MPF em face de João da Silva. "
            "É o relatório. "
            "Os fatos restaram comprovados pelos documentos de fls. 20/45. "
            "Diante do exposto, JULGO PROCEDENTE o pedido para condenar o réu."
        )
        result = split_sentenca_sections(text)
        self.assertIn("Trata-se", result["relatorio"])
        self.assertNotIn("Diante do exposto", result["relatorio"])
        self.assertIn("É o relatório", result["fundamentacao"])
        self.assertIn("documentos de fls", result["fundamentacao"])
        self.assertNotIn("Diante do exposto", result["fundamentacao"])
        self.assertIn("Diante do exposto", result["dispositivo"])
        self.assertIn("JULGO PROCEDENTE", result["dispositivo"])

    def test_fundamentacao_header_anchor(self):
        text = (
            "RELATÓRIO\nO autor narra que ...\n"
            "FUNDAMENTAÇÃO\nA prova produzida demonstra que ...\n"
            "Diante do exposto, julgo improcedente."
        )
        result = split_sentenca_sections(text)
        self.assertIn("O autor narra", result["relatorio"])
        self.assertTrue(result["fundamentacao"].startswith("FUNDAMENTAÇÃO"))
        self.assertIn("A prova produzida", result["fundamentacao"])
        self.assertIn("Diante do exposto", result["dispositivo"])

    def test_missing_fundamentacao_anchor(self):
        # No fundamentação marker — text up to dispositivo becomes relatório.
        text = (
            "Trata-se de ação proposta por A contra B. Os fatos estão demonstrados. "
            "JULGO PROCEDENTE o pedido."
        )
        result = split_sentenca_sections(text)
        self.assertIn("Trata-se", result["relatorio"])
        self.assertIn("fatos estão demonstrados", result["relatorio"])
        self.assertIsNone(result["fundamentacao"])
        self.assertIn("JULGO PROCEDENTE", result["dispositivo"])

    def test_missing_dispositivo_anchor(self):
        text = (
            "Relatório inicial do caso. "
            "É o relatório. "
            "Análise dos fatos e do direito aplicável, sem dispositivo identificável."
        )
        result = split_sentenca_sections(text)
        self.assertIn("Relatório inicial", result["relatorio"])
        self.assertIn("Análise dos fatos", result["fundamentacao"])
        self.assertIsNone(result["dispositivo"])

    def test_no_anchors_returns_all_none(self):
        text = "Texto qualquer sem marcadores estruturais."
        result = split_sentenca_sections(text)
        self.assertIsNone(result["relatorio"])
        self.assertIsNone(result["fundamentacao"])
        self.assertIsNone(result["dispositivo"])

    def test_dispositivo_keyword_inside_relatorio_not_picked(self):
        # "declaro" appears inside a quoted passage in the relatório;
        # the genuine dispositivo opens with "Diante do exposto" after
        # the fundamentação header. The function should anchor on the
        # fundamentação header and skip the false positive.
        text = (
            'O autor alegou que "declaro nulo o ato". '
            "É o relatório. "
            "A fundamentação segue. "
            "Diante do exposto, julgo procedente."
        )
        result = split_sentenca_sections(text)
        self.assertIn("autor alegou", result["relatorio"])
        self.assertIn("É o relatório", result["fundamentacao"])
        self.assertTrue(result["dispositivo"].startswith("Diante do exposto"))

    def test_empty_and_non_string_inputs(self):
        for value in ["", "   ", None, 123]:
            result = split_sentenca_sections(value)
            self.assertEqual(
                result,
                {"relatorio": None, "fundamentacao": None, "dispositivo": None},
            )

    def test_idempotent(self):
        text = (
            "Trata-se de ACP. É o relatório. Os fatos restaram comprovados. "
            "Diante do exposto, julgo procedente."
        )
        first = split_sentenca_sections(text)
        second = split_sentenca_sections(text)
        self.assertEqual(first, second)

    def test_applies_via_series(self):
        s = pd.Series([
            "Trata-se. É o relatório. Análise. Diante do exposto, condeno.",
            "Texto sem anchors.",
        ])
        out = s.apply(split_sentenca_sections)
        self.assertEqual(len(out), 2)
        self.assertIn("Análise", out.iloc[0]["fundamentacao"])
        self.assertIsNone(out.iloc[1]["relatorio"])
