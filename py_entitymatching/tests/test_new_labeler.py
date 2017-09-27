from nose.tools import *
import unittest
import os
from py_entitymatching.labeler.new_labeler.new_labeler import new_label_table
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class NewLabelerTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_label_table_invalid_df(self):
        col_name = 'label'
        new_label_table(None, col_name)

    @raises(AssertionError)
    def test_label_table_invalid_colname(self):
        A = read_csv_metadata(path_a)
        new_label_table(A, None)

    @raises(AssertionError)
    def test_label_invalid_column(self):
        C = read_csv_metadata(path_a)
        col_name = "zipcode"
        new_label_table(C, col_name)

    @raises(AssertionError)

