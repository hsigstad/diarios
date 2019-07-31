from diarios import clean
import unittest
import numpy as np
import pandas as pd


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

    def test_get_number_type(self):
        numbers = pd.Series(
            [
                '000 020320-09.2016.8.26.0000',
                '02032009.2016.826.0000',
                '...02032009.1994.826.0000 sfd',                                
                '032903453245-1'
            ],
            index=[5,1,1,10]
        )
        types = np.array([
            "cnj",
            "cnj",
            "cnj",            
            "mg"
        ])
        self.assertTrue(all(clean.get_number_type(numbers)==types))


    def test_get_filing_year(self):
        numbers = pd.Series(
            [
                '000 020320-09.2016.8.26.0000',
                '992989453245-6',                
                '...02032009.1994.826.0000 sfd',                
                'safsd000130.2014.1231234/53asf',
                '032903453245-1',
                '2000-13121111'
            ],
            index=[5,2,3,4,1,6]
        )
        years = pd.Series(
            [
                2016,
                1989,            
                1994,
                2014,
                2003,
                2000
            ],
            index=[5,2,3,4,1,6],
            name='filingyear'
        )
        self.assertTrue(all(
            clean.get_filing_year(numbers).sort_index()==years.sort_index()
        ))



if __name__ == '__main__':
    unittest.main()
        
