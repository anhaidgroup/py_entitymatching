# coding=utf-8
import sys
import magellan
import os
from nose.tools import *
import unittest
import pandas as pd
import six

from magellan.utils.generic_helper import get_install_path
from magellan.sampler.down_sample import _inv_index, _probe_index, down_sample, _get_str_cols_list
import magellan.catalog.catalog_manager as cm
from magellan.io.parsers import read_csv_metadata

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])

class DownSampleTestCases(unittest.TestCase):
    def test_down_sample_table_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, B, 2, 2)
        self.assertEqual(cm.get_all_properties(C), cm.get_all_properties(D))
        self.assertEqual(len(D), 2)

    def test_down_sample_table_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, B, len(B)+1, 3)
        self.assertEqual(cm.get_all_properties(C), cm.get_all_properties(D))
        self.assertEqual(len(D), len(B))

    @raises(AssertionError)
    def test_down_sample_table_invalid_dfA(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(None, B, 5, 5)

    @raises(AssertionError)
    def test_down_sample_table_invalid_dfB(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, None, 5, 5)

    @raises(AssertionError)
    def test_down_sample_invalid_df_sz0A(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(pd.DataFrame(), B, 5, 5)

    @raises(AssertionError)
    def test_down_sample_invalid_df_sz0B(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, pd.DataFrame(), 5, 5)

    @raises(AssertionError)
    def test_down_sample_invalid_param_size(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, B, 0, 5)

    @raises(AssertionError)
    def test_down_sample_invalid_param_y(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C, D = down_sample(A, B, 5, 0)

class InvertedIndexTestCases(unittest.TestCase):
    def test_down_sample_inv_index_valid_1(self):
        A = read_csv_metadata(path_a)
        inv_index = _inv_index(A)
        self.assertNotEqual(len(inv_index), 0)

class StrColTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_down_sample_get_str_cols_list_valid(self):
        A = read_csv_metadata(path_a)
        str_col_list = _get_str_cols_list(A)
        self.assertNotEqual(len(str_col_list), 3)
