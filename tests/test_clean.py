import os

from diarios import clean
import unittest
import numpy as np
import pandas as pd

# How to run tests:
# python -m pytest


class TestFunctions(unittest.TestCase):

    def test_map_regex(self):
        in_series = pd.Series(["abc", "aab", "xyz"], index=[5, 5, 2])
        mapping = {"ab": "yyy", "a": "xxx", "^xy": "zzz"}
        out_series = clean.map_regex(in_series, mapping)
        out_series2 = pd.Series(["yyy", "yyy", "zzz"], index=[5, 5, 2])
        self.assertTrue(all(out_series == out_series2))

    def test_convert_number_antigo(self):
        out1 = clean.convert_number_antigo(
            ["2016.51.01.164775-3", "660.01.2010.002107-1", "018.07.001979-4"],
            ["TRF2", "TJSP", "TJMS"],
        )
        out2 = pd.Series(
            [
                "0164775-04.2016.4.02.5101",
                "0002107-31.2010.8.26.0660",
                "0001979-89.2007.8.12.0018",
            ]
        )
        self.assertTrue(all(out1 == out2))

    def test_transform(self):
        mun = clean.transform(175, "municipio_id", "municipio")
        self.assertTrue(mun == "OURO PRETO DO OESTE")
        mun = clean.transform(175, ["municipio_id"], "municipio")
        self.assertTrue(mun == "OURO PRETO DO OESTE")
        sr1 = pd.Series([701, 7935])
        sr2 = pd.Series(["PARECIS", "GRAJAU"])
        mun = clean.transform(sr1, "municipio_id", "municipio")
        self.assertTrue(all(mun == sr2))
        mun = clean.transform(sr1, ["municipio_id"], "municipio")
        self.assertTrue(all(mun == sr2))
        df = pd.DataFrame({"x": [pd.NA, 1, 1, 1, pd.NA], "y": [4, 2, 2, 3, pd.NA]})
        infile = get_test_data_file("test_transform.csv")
        sr1 = clean.transform(df, from_var=["a", "b"], to_var="c", infile=infile)
        sr2 = pd.Series([pd.NA, 5, 5, 7, pd.NA])
        self.assertTrue(all(sr1.dropna() == sr2.dropna()))
        sr1 = clean.transform(df.y, from_var=["b"], to_var="c", infile=infile)
        sr2 = pd.Series([15, 5, 5, 7, pd.NA])
        self.assertTrue(all(sr1.dropna() == sr2.dropna()))


class TestCleanValor(unittest.TestCase):

    def test_basic(self):
        sr = pd.Series(["1.234,56", "100,00"])
        result = clean.clean_valor(sr)
        self.assertEqual(result.tolist(), ["1234.56", "100.00"])

    def test_no_decimals(self):
        sr = pd.Series(["1.000"])
        result = clean.clean_valor(sr)
        self.assertEqual(result.tolist(), ["1000"])


class TestCleanDate(unittest.TestCase):

    def test_yyyy_slash(self):
        result = clean.clean_date(pd.Series(["2021/03/15"]))
        self.assertEqual(result[0].tolist(), ["2021-03-15"])

    def test_yyyy_dash(self):
        result = clean.clean_date(pd.Series(["2020-01-15 extra text"]))
        self.assertEqual(result[0].tolist(), ["2020-01-15"])

    def test_null_and_empty(self):
        result = clean.clean_date(pd.Series([None, ""]))
        self.assertTrue(result[0].isna().all())


class TestCleanLine(unittest.TestCase):

    def test_numeric(self):
        result = clean.clean_line(pd.Series(["5", "10"]))
        self.assertEqual(result.tolist(), [5, 10])

    def test_non_numeric(self):
        result = clean.clean_line(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestRemoveLinks(unittest.TestCase):

    def test_markdown_link(self):
        sr = pd.Series(["[click here](http://example.com) text"])
        result = clean.remove_links(sr)
        self.assertEqual(result.tolist(), ["click here text"])

    def test_no_links(self):
        sr = pd.Series(["plain text"])
        result = clean.remove_links(sr)
        self.assertEqual(result.tolist(), ["plain text"])


class TestGetCapital(unittest.TestCase):

    def test_string(self):
        self.assertEqual(clean.get_capital("SP"), "SAO PAULO")
        self.assertEqual(clean.get_capital("RJ"), "RIO DE JANEIRO")

    def test_series(self):
        result = clean.get_capital(pd.Series(["MG", "BA"]))
        self.assertEqual(result.tolist(), ["BELO HORIZONTE", "SALVADOR"])


class TestCleanParteKey(unittest.TestCase):

    def test_removes_trailing_articles(self):
        result = clean.clean_parte_key(pd.Series(["autor aos", "reu a"]))
        self.assertEqual(result.tolist(), ["AUTOR", "REU"])

    def test_no_trailing_article(self):
        result = clean.clean_parte_key(pd.Series(["advogado"]))
        self.assertEqual(result.tolist(), ["ADVOGADO"])


class TestCleanComarca(unittest.TestCase):

    def test_uppercases(self):
        result = clean.clean_comarca(pd.Series(["recife"]))
        self.assertEqual(result.tolist(), ["RECIFE"])


class TestCleanVara(unittest.TestCase):

    def test_extracts_number(self):
        result = clean.clean_vara(pd.Series(["1a vara civel", "3"]))
        self.assertEqual(result.tolist(), ["1", "3"])


class TestTitle(unittest.TestCase):

    def test_basic(self):
        result = clean.title(pd.Series(["SAO PAULO DE MINAS"]))
        self.assertEqual(result.tolist(), ["Sao Paulo de Minas"])

    def test_prepositions(self):
        result = clean.title(pd.Series(["RIO DE JANEIRO", "OURO PRETO DO OESTE"]))
        self.assertEqual(result.tolist(), ["Rio de Janeiro", "Ouro Preto do Oeste"])


class TestGetEstadoMapping(unittest.TestCase):

    def test_returns_27_states(self):
        m = clean.get_estado_mapping()
        self.assertEqual(len(m), 27)

    def test_known_entries(self):
        m = clean.get_estado_mapping()
        self.assertEqual(m["SAO PAULO"], "SP")
        self.assertEqual(m["BAHIA"], "BA")
        self.assertEqual(m["MINAS GERAIS"], "MG")


class TestGetDataFile(unittest.TestCase):

    def test_returns_existing_path(self):
        path = clean.get_data_file("municipio.csv")
        self.assertTrue(path.endswith(os.path.join("data", "municipio.csv")))
        self.assertTrue(os.path.exists(path))


class TestCleanCpf(unittest.TestCase):

    def test_numeric(self):
        result = clean.clean_cpf(pd.Series(["12345678901"]))
        self.assertEqual(result.tolist(), [12345678901])

    def test_as_str_pads(self):
        result = clean.clean_cpf(pd.Series(["123"]), as_str=True)
        self.assertEqual(result.tolist(), ["00000000123"])

    def test_invalid(self):
        result = clean.clean_cpf(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestCleanReais(unittest.TestCase):

    def test_with_decimals(self):
        result = clean.clean_reais(pd.Series(["1.234,56"]))
        self.assertEqual(result.tolist(), [1234])

    def test_currency_symbol(self):
        result = clean.clean_reais(pd.Series(["R$ 2.500,00"]))
        self.assertEqual(result.tolist(), [2500])

    def test_plain_number(self):
        result = clean.clean_reais(pd.Series(["300"]))
        self.assertEqual(result.tolist(), [300])

    def test_invalid(self):
        result = clean.clean_reais(pd.Series([""]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestCleanText(unittest.TestCase):

    def test_string_input(self):
        result = clean.clean_text("  Hello, World!  São Paulo  ")
        self.assertEqual(result, "HELLO WORLD SAO PAULO")

    def test_series_input(self):
        result = clean.clean_text(pd.Series(["café", None, "  hello  "]))
        self.assertEqual(result.tolist(), ["CAFE", "", "HELLO"])

    def test_returns_string_for_string_input(self):
        result = clean.clean_text("test")
        self.assertIsInstance(result, str)

    def test_returns_series_for_series_input(self):
        result = clean.clean_text(pd.Series(["test"]))
        self.assertIsInstance(result, pd.Series)

    def test_lower(self):
        result = clean.clean_text("Hello World", lower=True, upper=False)
        self.assertEqual(result, "hello world")

    def test_keep_accents(self):
        result = clean.clean_text("São Paulo", accents=True, drop=None)
        self.assertEqual(result, "SÃO PAULO")

    def test_custom_drop(self):
        result = clean.clean_text("abc 123", drop="^a-z ", upper=False)
        self.assertEqual(result, "abc")

    def test_newline_removed_by_default(self):
        result = clean.clean_text("hello\nworld")
        self.assertEqual(result, "HELLO WORLD")

    def test_newline_preserved(self):
        result = clean.clean_text("hello\nworld", newline=True, drop=None)
        self.assertIn("\n", result)

    def test_multiple_spaces_collapsed(self):
        result = clean.clean_text("hello    world")
        self.assertEqual(result, "HELLO WORLD")

    def test_multiple_spaces_preserved(self):
        result = clean.clean_text("hello    world", multiple_spaces=True)
        self.assertEqual(result, "HELLO    WORLD")

    def test_strip(self):
        result = clean.clean_text("  hello  ")
        self.assertEqual(result, "HELLO")

    def test_no_strip(self):
        result = clean.clean_text("  hello  ", strip=False)
        self.assertTrue(result.startswith(" "))


class TestCleanNumber(unittest.TestCase):

    def test_cnj_format(self):
        sr = pd.Series(["0002107-31.2010.8.26.0660"])
        result = clean.clean_number(sr)
        self.assertEqual(result.tolist(), ["0002107-31.2010.8.26.0660"])

    def test_strips_non_digits_around(self):
        sr = pd.Series(["Proc. 0002107-31.2010.8.26.0660 end"])
        result = clean.clean_number(sr)
        self.assertEqual(result.tolist(), ["0002107-31.2010.8.26.0660"])

    def test_no_digits(self):
        sr = pd.Series(["abc"])
        result = clean.clean_number(sr)
        self.assertEqual(result.iloc[0], "")


class TestIsCnjNumber(unittest.TestCase):

    def test_valid(self):
        result = clean.is_cnj_number(pd.Series(["0002107-31.2010.8.26.0660"]))
        self.assertTrue(result.iloc[0])

    def test_invalid(self):
        result = clean.is_cnj_number(pd.Series(["abc", "123"]))
        self.assertFalse(result.iloc[0])
        self.assertFalse(result.iloc[1])


class TestCleanEstado(unittest.TestCase):

    def test_full_names(self):
        result = clean.clean_estado(pd.Series(["São Paulo", "BAHIA"]))
        self.assertEqual(result.tolist(), ["SP", "BA"])

    def test_abbreviations_pass_through(self):
        result = clean.clean_estado(pd.Series(["SP", "RJ"]))
        self.assertEqual(result.tolist(), ["SP", "RJ"])

    def test_unknown(self):
        result = clean.clean_estado(pd.Series(["NEVERLAND"]))
        self.assertTrue(pd.isna(result.iloc[0]))


class TestCleanOab(unittest.TestCase):

    def test_series(self):
        result = clean.clean_oab(pd.Series(["OAB/SP 12345", "67890 RJ"]))
        self.assertEqual(result.tolist(), ["12345/SP", "67890/RJ"])

    def test_string_input(self):
        result = clean.clean_oab("12345 SP")
        self.assertEqual(result.tolist(), ["12345/SP"])


class TestCleanParte(unittest.TestCase):

    def test_removes_titles_and_suffixes(self):
        result = clean.clean_parte(pd.Series(["DRS JOAO SILVA E OUTROS"]))
        self.assertEqual(result.tolist(), ["JOAO SILVA"])

    def test_maps_mp(self):
        result = clean.clean_parte(pd.Series(["MINISTERIO PUBLICO"]))
        self.assertEqual(result.tolist(), ["MP"])

    def test_delete_pattern(self):
        result = clean.clean_parte(
            pd.Series(["JOAO", "SECRET INFO"]),
            delete="SECRET",
        )
        self.assertEqual(result.iloc[0], "JOAO")
        self.assertEqual(result.iloc[1], "")


class TestCleanClasse(unittest.TestCase):

    def test_known_classes(self):
        result = clean.clean_classe(pd.Series([
            "ACAO CIVIL PUBLICA",
            "AGRAVO DE INSTRUMENTO",
            "APELACAO",
        ]))
        self.assertEqual(result.tolist(), ["ACP", "AI", "Ap"])

    def test_unmatched_kept(self):
        result = clean.clean_classe(pd.Series(["xyz"]))
        self.assertEqual(result.tolist(), ["XYZ"])


class TestCleanTipoParte(unittest.TestCase):

    def test_plaintiff_types(self):
        result = clean.clean_tipo_parte(pd.Series(["AUTOR", "REQUERENT"]))
        self.assertEqual(result.tolist(), ["PLAINTIFF", "PLAINTIFF"])

    def test_defendant_types(self):
        result = clean.clean_tipo_parte(pd.Series(["REU", "REQUERID"]))
        self.assertEqual(result.tolist(), ["DEFENDANT", "DEFENDANT"])

    def test_lawyer(self):
        result = clean.clean_tipo_parte(pd.Series(["ADVOGADO"]))
        self.assertEqual(result.tolist(), ["LAWYER"])


class TestCleanDecision(unittest.TestCase):

    def test_grau_1(self):
        result = clean.clean_decision(pd.Series([
            "JULGO PROCEDENTE",
            "JULGO IMPROCEDENTE",
            "JULGO PARCIALMENTE PROCEDENTE",
            "HOMOLOGO O ACORDO",
        ]))
        self.assertEqual(result.tolist(), [
            "PROCEDENTE",
            "IMPROCEDENTE",
            "PARCIALMENTE PROCEDENTE",
            "HOMOLOGO ACORDO",
        ])

    def test_grau_2(self):
        result = clean.clean_decision(
            pd.Series(["DERAM PROVIMENTO", "NEGAR PROVIMENTO"]),
            grau="2",
        )
        self.assertEqual(result.tolist(), ["DERAM", "NEGAR"])

    def test_unmatched_kept(self):
        result = clean.clean_decision(pd.Series(["random text"]))
        self.assertEqual(result.tolist(), ["RANDOM TEXT"])


class TestGetPlaintiffwinsMapping(unittest.TestCase):

    def test_default(self):
        m = clean.get_plaintiffwins_mapping()
        self.assertEqual(m["PROCEDENTE"], 1)
        self.assertEqual(m["IMPROCEDENTE"], 0)
        self.assertEqual(m["PARCIALMENTE PROCEDENTE"], 1)

    def test_parcial_zero(self):
        m = clean.get_plaintiffwins_mapping(parcial=0)
        self.assertEqual(m["PARCIALMENTE PROCEDENTE"], 0)


class TestGetFilingYear(unittest.TestCase):

    def test_cnj_number(self):
        result = clean.get_filing_year(pd.Series(["0002107-31.2010.8.26.0660"]))
        self.assertEqual(result.tolist(), [2010])


class TestCleanInteger(unittest.TestCase):

    def test_digit_string(self):
        result = clean.clean_integer(pd.Series(["42"]))
        self.assertEqual(result.tolist(), [42])

    def test_number_words(self):
        result = clean.clean_integer(pd.Series(["CINCO", "DEZ"]))
        self.assertEqual(result.tolist(), [5, 10])

    def test_invalid(self):
        result = clean.clean_integer(pd.Series(["abc"]))
        self.assertTrue(np.isnan(result.iloc[0]))


class TestExtractFromList(unittest.TestCase):

    def test_first_match_wins(self):
        sr = pd.Series(["JULGO PROCEDENTE a acao", "HOMOLOGO ACORDO"])
        regex_list = ["JULGO .{0,20}PROCEDENTE", "HOMOLOGO .{0,20}ACORDO"]
        result = clean.extract_from_list(sr, regex_list)
        self.assertEqual(result.iloc[0], "JULGO PROCEDENTE")
        self.assertEqual(result.iloc[1], "HOMOLOGO ACORDO")

    def test_no_match(self):
        sr = pd.Series(["nothing here"])
        result = clean.extract_from_list(sr, ["PATTERN"])
        self.assertTrue(pd.isna(result.iloc[0]))


def get_test_data(datafile):
    infile = get_test_data_file(datafile)
    return pd.read_csv(infile)


def get_test_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, "data", datafile)


if __name__ == "__main__":
    unittest.main()
