import os

os.chdir("/home/henrik/diarios")
from diarios import clean
import unittest
import numpy as np
import pandas as pd

# How to run tests:
# python3 -m unittest tests/test_clean.py


class TestFunctions(unittest.TestCase):

    # def test_get_cpf(self):
    #     names = pd.Series(
    #         [
    #             'wladimir silva furtado endereco rua',
    #             'wladimir silva furtado',
    #             'jaci pena amanajas e outros valor cau',
    #             'antonio dos santos freitas valor causa',
    #             'amiraldo da silva favacho junior valor'
    #         ],
    #         index=[5,1,2,1,9]
    #     )
    #     cpfs = np.array([
    #         '24429473153',
    #         '24429473153',
    #         '4223284215',
    #         '32487509287',
    #         '64691934200'
    #     ])
    #     self.assertTrue(all(clean.get_cpf(names, 'AP')==cpfs))

    # def test_get_number_type(self):
    #     numbers = pd.Series([
    #         '000 020320-09.2016.8.26.0000', '02032009.2016.826.0000',
    #         '...02032009.1994.826.0000 sfd', '032903453245-1'
    #     ],
    #                         index=[5, 1, 1, 10])
    #     types = np.array(["cnj", "cnj", "cnj", "mg"])
    #     self.assertTrue(all(clean.get_number_type(numbers) == types))

    # def test_get_filing_year(self):
    #     numbers = pd.Series([
    #         '000 020320-09.2016.8.26.0000', '992989453245-6',
    #         '...02032009.1994.826.0000 sfd', 'safsd000130.2014.1231234/53asf',
    #         '032903453245-1', '2000-13121111'
    #     ],
    #                         index=[5, 2, 3, 4, 1, 6])
    #     years = pd.Series([2016, 1989, 1994, 2014, 2003, 2000],
    #                       index=[5, 2, 3, 4, 1, 6],
    #                       name='filingyear')
    #     self.assertTrue(
    #         all(
    #             clean.get_filing_year(numbers).sort_index() ==
    #             years.sort_index()))

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
