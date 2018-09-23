import unittest
import pandas as pd
import os
from auto_sql import AutoSql
import sqlite3

class TestAutoSql(unittest.TestCase):

    def setUp(self):
        '''
        Instantiates the test object for the AutoSql class
        '''
        self.test = AutoSql(file=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                              os.path.basename('surveys.csv')),
                            db_name='test',
                            out_dir=os.path.dirname(os.path.abspath(__file__)),
                            sep=',')


    def test_count_file_lines(self):
        '''
        Tests if the method counts the correct number of lines in the csv file.
        '''
        self.assertEqual(self.test.count_file_lines(), 35549)

    def test_dataframes(self):
        '''
        Tests that a pandas Dataframe created from the surveys.csv file and a pandas Dataframe created from the sqlite
        database created by AutoSql are equivalent in every way as determined by the Dataframe method  "equals".
        '''
        test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.path.basename('test.db'))

        self.test.run()
        con = sqlite3.connect(test_dir)
        df1 = pd.read_csv(self.test.file, sep=self.test.sep, encoding=self.test.encoding)
        df2 = pd.read_sql("SELECT * FROM surveys", con)
        con.close()
        os.remove(test_dir)
        self.assertTrue(df1.equals(df2))

if __name__ == '__main__':
    unittest.main()
