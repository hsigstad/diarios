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


def get_test_data(datafile):
    infile = get_test_data_file(datafile)
    return pd.read_csv(infile)


def get_test_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, "data", datafile)


if __name__ == "__main__":
    unittest.main()
