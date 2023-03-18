import os
# from nose.tools import *
import unittest
import pandas as pd
import numpy as np
import six
from .utils import raises

from py_entitymatching.utils.generic_helper import get_install_path
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.labeler.labeler import _post_process_labelled_table, _init_label_table, _validate_inputs, label_table

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class LabelTableTestCases(unittest.TestCase):
    @unittest.skip("Not a test")
    def _test_label_table(self, table, col_name, label_values):
        _validate_inputs(table, col_name,  verbose=False)
        lbl_table = _init_label_table(table, col_name)
        # mg._viewapp = None
        # from py_entitymatching.gui.table_gui import edit_table
        # edit_table(lbl_table, show_flag=False)
        # mg._viewapp = None

        new_table = lbl_table.copy()
        cm.copy_properties(table, new_table)
        lbl_table = new_table

        lbl_table[col_name] = label_values
        lbl_table = _post_process_labelled_table(table, lbl_table, col_name)
        return lbl_table


    def test_label_table_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_name = 'label'
        num_zeros, num_ones = 8, 7
        label_values = [0]*num_zeros
        label_values.extend([1]*num_ones)
        D = self._test_label_table(C, col_name, label_values)
        self.assertEqual(np.sum(D[col_name]), num_ones)
        p1, p2 = cm.get_all_properties(C), cm.get_all_properties(D)
        self.assertEqual(p1, p2)

    def test_label_table_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_name = 'label'
        num_zeros, num_ones = 8, 7
        label_values = [0]*num_zeros
        label_values.extend([1]*num_ones)
        D = self._test_label_table(C, col_name, label_values)
        self.assertEqual(np.sum(D[col_name]), num_ones)
        p1, p2 = cm.get_all_properties(C), cm.get_all_properties(D)
        self.assertEqual(p1, p2)

    @raises(AssertionError)
    def test_label_table_invalid_df(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_name = 'label'
        label_table(None, col_name)


    @raises(AssertionError)
    def test_label_table_invalid_colname(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_name = 'label'
        label_table(C, None)

    @raises(AssertionError)
    def test_label_table_with_already_colname(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C['label'] = 0
        col_name = 'label'
        num_zeros, num_ones = 8, 7
        label_values = [0]*num_zeros
        label_values.extend([1]*num_ones)
        D = self._test_label_table(C, col_name, label_values)
        self.assertEqual(np.sum(D[col_name]), num_ones)
        p1, p2 = cm.get_all_properties(C), cm.get_all_properties(D)
        self.assertEqual(p1, p2)

    # @raises(AssertionError)
    # def test_label_table_with_already_colname_replace_false(self):
    #     A = read_csv_metadata(path_a)
    #     B = read_csv_metadata(path_b, key='ID')
    #     C = read_csv_metadata(path_c, ltable=A, rtable=B)
    #     C['label'] = 0
    #     col_name = 'label'
    #     num_zeros, num_ones = 8, 7
    #     label_values = [0]*num_zeros
    #     label_values.extend([1]*num_ones)
    #     D = self._test_label_table(C, col_name, label_values)
    #     # self.assertEqual(np.sum(D[col_name]), num_ones)
    #     # p1, p2 = cm.get_all_properties(C), cm.get_all_properties(D)
    #     # self.assertEqual(p1, p2)

    @raises(AssertionError)
    def test_label_table_with_colname_diff_values(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C['label'] = 0
        col_name = 'label'
        num_zeros, num_ones, num_twos = 8, 5, 2
        label_values = [0]*num_zeros
        label_values.extend([1]*num_ones)
        label_values.extend([2]*num_twos)

        D = self._test_label_table(C, col_name, label_values)

    @unittest.skip("Not a test")
    def test_label_table_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        D = label_table(C, 'label')
        p1, p2 = cm.get_all_properties(C), cm.get_all_properties(D)
        self.assertEqual(p1, p2)
