# coding=utf-8
import sys
import py_entitymatching
import os
from nose.tools import *
import unittest
import pandas as pd
import six

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.sampler.down_sample import _inv_index, _probe_index_split, down_sample, _get_str_cols_list
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.io.parsers import read_csv_metadata

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'restA.csv'])
path_b = os.sep.join([datasets_path, 'restB.csv'])

class DownSampleTestCases(unittest.TestCase):
    def setUp(self):
        self.A = read_csv_metadata(path_a, key='ID')
        self.B = read_csv_metadata(path_b, key='ID')

    def tearDown(self):
        del self.A
        del self.B

    def test_down_sample_table_valid_1(self):
        C, D = down_sample(self.A, self.B, 100, 10)
        self.assertEqual(len(D), 100)

    def test_down_sample_table_valid_2(self):
        C, D = down_sample(self.A, self.B, len(self.B)+1, 10)
        self.assertEqual(len(D), len(self.B))

    def test_down_sample_table_valid_3(self):
        C, D = down_sample(self.A, self.B, 100, 10)
        self.assertNotEqual(len(C), 0)

    def test_invalid_data_frame_size(self):
        df_empty = pd.DataFrame({'A': []})
        with self.assertRaises(AssertionError) as ctx:
            down_sample(df_empty, self.B, 100, 10)

        actual = str(ctx.exception)
        expected = 'Size of the input table is 0'
        self.assertEqual(actual, expected)

    @raises(AssertionError)
    def test_down_sample_table_invalid_dfA(self):
        C, D = down_sample(None, self.B, 100, 10)

    @raises(AssertionError)
    def test_down_sample_table_invalid_dfB(self):
        C, D = down_sample(self.A, None, 100, 10)

    @raises(AssertionError)
    def test_down_sample_invalid_df_sz0A(self):
        C, D = down_sample(pd.DataFrame(), self.B, 100, 10)

    @raises(AssertionError)
    def test_down_sample_invalid_df_sz0B(self):
        C, D = down_sample(self.A, pd.DataFrame(), 100, 10)

    @raises(AssertionError)
    def test_down_sample_invalid_param_size(self):
        C, D = down_sample(self.A, self.B, 0, 10)

    @raises(AssertionError)
    def test_down_sample_invalid_param_y(self):
        C, D = down_sample(self.A, self.B, 100, 0)

    @raises(AssertionError)
    def test_down_sample_invalid_seed(self):
        C, D = down_sample(self.A, self.B, 100, 10, seed="test")

    @raises(AssertionError)
    def test_down_sample_invalid_njobs(self):
        C, D = down_sample(self.A, self.B, 100, 10, n_jobs="10")

    @raises(AssertionError)
    def test_down_sample_invalid_rem_stop_words(self):
        C, D = down_sample(self.A, self.B, 100, 10, rem_stop_words="False")

    @raises(AssertionError)
    def test_down_sample_invalid_rem_puncs(self):
        C, D = down_sample(self.A, self.B, 100, 10, rem_puncs="False")

    def test_down_sample_seed(self):
        C, D = down_sample(self.A, self.B, 100, 10, seed=0, show_progress=False)
        E, F = down_sample(self.A, self.B, 100, 10, seed=0, show_progress=False)
        self.assertEqual(D.equals(F), True)
        self.assertEqual(C.equals(E), True)

    # def test_down_sample_norm_njobs(self):
    #     C, D = down_sample(self.A, self.B, 100, 1, seed=0, n_jobs=1, show_progress=False)
    #     C = C.sort_values("ID")
    #     D = D.sort_values("ID")
    #
    #     E, F = down_sample(self.A, self.B, 100, 1, seed=0, n_jobs=-1,
    #                        show_progress=False)
    #     E = E.sort_values("ID")
    #     F = F.sort_values("ID")
    #     print(len(C), len(E))
    #     self.assertEqual(D.equals(F), True)
    #     self.assertEqual(C.equals(E), True)

    def test_down_sample_njobs_fixed(self):
        C, D = down_sample(self.A, self.B, 100, 10, seed=0, n_jobs=2, show_progress=False)
        assert(len(C) > 0)
        assert(len(D) > 0)

    def test_down_sample_njobs_all(self):
        C, D = down_sample(self.A, self.B, 100, 10, seed=0, n_jobs=-1,
                           show_progress=False)





class InvertedIndexTestCases(unittest.TestCase):
    def test_down_sample_inv_index_valid_1(self):
        A = read_csv_metadata(path_a)
        inv_index = _inv_index(A)
        self.assertNotEqual(len(inv_index), 0)

    def test_down_sample_inv_index_key_check(self):
        A = read_csv_metadata(path_a)
        inv_index = _inv_index(A)
        self.assertTrue('meadows' in inv_index)

    def test_down_sample_inv_index_value_check(self):
        A = read_csv_metadata(path_a)
        inv_index = _inv_index(A)
        self.assertNotEqual(len(inv_index.get('beach')), 0)


class StrColTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_down_sample_get_str_cols_list_valid1(self):
        A = read_csv_metadata(path_a)
        str_col_list = _get_str_cols_list(pd.DataFrame())

    def test_down_sample_get_str_cols_list_valid2(self):
        A = read_csv_metadata(path_a)
        str_col_list = _get_str_cols_list(A)
        self.assertNotEqual(len(str_col_list), 0)


class ProbeIndexTestCases(unittest.TestCase):
    def test_down_sample_probe_index_invalid_set(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        in_index = _inv_index(A)
        s_tbl_indices = _probe_index_split(B, 5, len(A), in_index)
        self.assertTrue(type(s_tbl_indices) is set)

    def test_down_sample_probe_index_validchk1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        in_index = _inv_index(A)
        s_tbl_indices = _probe_index_split(B, 5, len(A), in_index)
        self.assertNotEqual(len(s_tbl_indices), 0)
