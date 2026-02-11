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


def get_test_data(datafile):
    infile = get_test_data_file(datafile)
    return pd.read_csv(infile)


def get_test_data_file(datafile):
    pkg_dir, _ = os.path.split(__file__)
    return os.path.join(pkg_dir, "data", datafile)


if __name__ == "__main__":
    unittest.main()
