import unittest
import pandas as pd
import os
from src.auto_sql import AutoSql
import sqlite3


class TestAutoSql(unittest.TestCase):

    def setUp(self):
        '''

        Instantiates the test object for the AutoSql class
        '''
        self.test = AutoSql(file='PoliceKillingsUS.csv',
                            db_name='test.db',
                            out_dir=os.path.dirname(__file__),
                            sep=',',
                            encoding='ISO-8859-1')


    def test_count_file_lines(self):
        '''
        Tests if the method counts the correct number of lines in the csv file.
        '''
        self.assertEqual(self.test.count_file_lines(), 2535)

    def test_dataframes(self):
        test_dir = os.path.join(os.path.dirname(__file__), os.path.basename(self.test.db_name))
        self.test.run()
        con = sqlite3.connect(test_dir)
        #)
        #df1 = pd.read_csv(test_dir, sep=self.test.sep, encoding=self.test.encoding)
        #print(df1)
        #https: // stackoverflow.com / questions / 19119320 / how - to - check - if -two - data - frames - are - equal
        df1 = pd.read_csv(self.test.file, sep=self.test.sep, encoding=self.test.encoding)
        df2 = pd.read_sql("SELECT")
        os.remove(test_dir)

if __name__ == '__main__':
    unittest.main()
