import os
# from nose.tools import *
import pandas as pd
import unittest
from .utils import raises

import py_entitymatching as em

p = em.get_install_path()
path_a = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_b = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
l_overlap_attr_1 = 'name'
l_overlap_attr_2 = 'address'
r_overlap_attr_1 = 'name'
r_overlap_attr_2 = 'address'
l_output_attrs = ['name', 'address']
r_output_attrs = ['name', 'address']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

# overlap on [r,l]_overlap_attr_1 with overlap_size=1
expected_ids_1 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# overlap on [r,l]_overlap_attr_2 with overlap_size=4
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2')]

# overlap on birth_year with q_val=3, overlap_size=2 (no padding) =6 (padding)
expected_ids_3 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                  ('a5', 'b5')]

# block tables on [l|r]_overlap_attr_2, block candset on [l|r]overlap_attr_3
expected_ids_2_and_3 = [('a2', 'b3'), ('a3', 'b2')]

# overlap on [r,l]_overlap_attr_1, overlap_size=1 on tables with missing values
expected_ids_4 = [('a1', 'b4'), ('a2', 'b3'), ('a2', 'b4'), ('a2', 'b6'),
                  ('a3', 'b2'), ('a3', 'b4'), ('a4', 'b1'), ('a4', 'b2'),
                  ('a4', 'b3'), ('a4', 'b4'), ('a4', 'b5'), ('a4', 'b6'),
                  ('a5', 'b4'), ('a5', 'b5')]

# overlap on [l,r]_overlap_attr_2, overlap_size=4 on tables with missing values
expected_ids_5 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a4', 'b6'),
                  ('a5', 'b4'), ('a5', 'b5')]

class OverlapBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.ob = em.OverlapBlocker()

    def tearDown(self):
        del self.A
        del self.B
        del self.ob

    @raises(AssertionError)
    def test_ob_block_tables_invalid_ltable_1(self):
        self.ob.block_tables(None, self.B, l_overlap_attr_1, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_ltable_2(self):
        self.ob.block_tables([10, 10], self.B, l_overlap_attr_1,
                             r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_ltable_3(self):
        self.ob.block_tables(pd.DataFrame(), self.B, l_overlap_attr_1,
                             r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rtable_1(self):
        self.ob.block_tables(self.A, None, l_overlap_attr_1, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rtable_2(self):
        self.ob.block_tables(self.A, [10, 10], l_overlap_attr_1,
                             r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rtable_3(self):
        self.ob.block_tables(self.A, pd.DataFrame(), l_overlap_attr_1,
                             r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_overlap_attr_1(self):
        self.ob.block_tables(self.A, self.B, None, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_overlap_attr_2(self):
        self.ob.block_tables(self.A, self.B, 10, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_overlap_attr_3(self):
        self.ob.block_tables(self.A, self.B, True, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_bogus_l_overlap_attr(self):
        self.ob.block_tables(self.A, self.B, 'bogus_attr', r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_multi_l_overlap_attr(self):
        self.ob.block_tables(self.A, self.B, ['birth_year', 'zipcode'],
                             r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_overlap_attr_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1, None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_overlap_attr_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1, 10)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_overlap_attr_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1, True)

    @raises(AssertionError)
    def test_ob_block_tables_bogus_r_overlap_attr(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1, 'bogus_attr')

    @raises(AssertionError)
    def test_ob_block_tables_multi_r_overlap_attr(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             ['birth_year', 'zipcode'])

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_attrs_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_attrs=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_attrs_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_attrs='name')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_attrs_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_ob_block_tables_bogus_l_output_attrs(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_attrs_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_attrs=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_attrs_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_attrs='name')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_attrs_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_ob_block_tables_bogus_r_output_attrs(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_prefix_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_prefix=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_prefix_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_prefix=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_l_output_prefix_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, l_output_prefix=True)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_prefix_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_prefix=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_prefix_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_prefix=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_r_output_prefix_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, r_output_prefix=True)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_allow_missing_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, allow_missing=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_allow_missing_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, allow_missing=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_allow_missing_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, allow_missing='yes')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_verbose_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, verbose=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_verbose_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, verbose=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_verbose_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, verbose='yes')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rem_stop_words_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, rem_stop_words=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rem_stop_words_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, rem_stop_words=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_rem_stop_words_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, rem_stop_words='yes')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_qval_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level=False, q_val=1.5)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_qval_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level=False, q_val='1')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_word_level_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_word_level_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level=1)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_word_level_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level='yes')

    @raises(SyntaxError)
    def test_ob_block_tables_invalid_qval_word_level_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, q_val=1)

    @raises(SyntaxError)
    def test_ob_block_tables_invalid_qval_word_level_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, word_level=False)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_overlap_size_1(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, overlap_size=None)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_overlap_size_2(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, overlap_size=1.5)

    @raises(AssertionError)
    def test_ob_block_tables_invalid_overlap_size_3(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, overlap_size='1')

    @raises(AssertionError)
    def test_ob_block_tables_invalid_overlap_size_4(self):
        self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                             r_overlap_attr_1, overlap_size=-1)

    def test_ob_block_tables(self):
        C = self.ob.block_tables(self.A, self.B,
                                 l_overlap_attr_1, r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_empty_ltable(self):
        empty_A = pd.DataFrame(columns=self.A.columns)
        print(empty_A.dtypes)
        em.set_key(empty_A, 'ID')
        C = self.ob.block_tables(empty_A, self.B,
                                 l_overlap_attr_1, r_overlap_attr_1)
        validate_metadata(C)
        validate_data(C)

    def test_ob_block_tables_empty_rtable(self):
        empty_B = pd.DataFrame(columns=self.B.columns)
        em.set_key(empty_B, 'ID')
        C = self.ob.block_tables(self.A, empty_B,
                                 l_overlap_attr_1, r_overlap_attr_1)
        validate_metadata(C)
        validate_data(C)

    def test_ob_block_tables_wi_no_output_tuples(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1, overlap_size=2)
        validate_metadata(C)
        validate_data(C)

    def test_ob_block_tables_wi_null_l_output_attrs(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1, l_output_attrs=None,
                                 r_output_attrs=r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_wi_null_r_output_attrs(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=None)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_wi_empty_l_output_attrs(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1, l_output_attrs=[],
                                 r_output_attrs=r_output_attrs)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_wi_empty_r_output_attrs(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1, l_output_attrs=l_output_attrs,
                                 r_output_attrs=[])
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_wi_qval_non_str_attr(self):
         C = self.ob.block_tables(self.A, self.B, 'birth_year', 'birth_year',
                                  q_val=3, word_level=False, overlap_size=6)
         validate_metadata(C)
         validate_data(C, expected_ids_3)

    def test_ob_block_tables_wi_missing_values_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ob.block_tables(A, B, l_overlap_attr_1, r_overlap_attr_1,
                                 allow_missing=True)
        validate_metadata(C)
        validate_data(C, expected_ids_4)

    def test_ob_block_tables_wi_missing_values_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ob.block_tables(A, B, l_overlap_attr_1, r_overlap_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_1)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_candset_1(self):
        self.ob.block_candset(None, l_overlap_attr_1, r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_candset_2(self):
        self.ob.block_candset([10, 10], l_overlap_attr_1, r_overlap_attr_1)

    @raises(KeyError)
    def test_ob_block_candset_invalid_candset_3(self):
        self.ob.block_candset(pd.DataFrame(), l_overlap_attr_1,
                              r_overlap_attr_1)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_l_overlap_attr_1(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, None, r_overlap_attr_2)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_l_overlap_attr_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, 10, r_overlap_attr_2)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_l_overlap_attr_3(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, True, r_overlap_attr_2)

    @raises(AssertionError)
    def test_ob_block_candset_bogus_l_overlap_attr(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, 'bogus_attr', r_overlap_attr_2)

    @raises(AssertionError)
    def test_ob_block_candset_multi_l_overlap_attr(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, ['birth_year', 'zipcode'], r_overlap_attr_2)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_r_overlap_attr_1(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, None)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_r_overlap_attr_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, 10)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_r_overlap_attr_3(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, True)

    @raises(AssertionError)
    def test_ob_block_candset_bogus_r_overlap_attr(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, 'bogus_attr')

    @raises(AssertionError)
    def test_ob_block_candset_multi_r_overlap_attr(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, ['birth_year', 'zipcode'])

    @raises(AssertionError)
    def test_ob_block_candset_invalid_verbose_1(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                              verbose=None)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_verbose_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2, verbose=1)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_verbose_3(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                              verbose='yes')

    @raises(AssertionError)
    def test_ob_block_candset_invalid_show_progress_1(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                              show_progress=None)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_show_progress_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                              show_progress=1)

    @raises(AssertionError)
    def test_ob_block_candset_invalid_show_progress_3(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1)
        self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                              show_progress='yes')

    def test_ob_block_candset(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                                  rem_stop_words=True,overlap_size=4)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ob_block_candset_empty_input(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1, overlap_size=2)
        validate_metadata(C)
        validate_data(C)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ob_block_candset_empty_output(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_2,
                                 r_overlap_attr_2, overlap_size=4)
        validate_metadata(C)
        validate_data(C, expected_ids_2)
        D = self.ob.block_candset(C, l_overlap_attr_1, r_overlap_attr_1,
                                  overlap_size=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ob_block_candset_wi_qval_non_str_attr(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_2,
                                 r_overlap_attr_2, overlap_size=4)
        validate_metadata(C)
        validate_data(C, expected_ids_2)
        D = self.ob.block_candset(C, 'birth_year', 'birth_year',
                                  q_val=3, word_level=False, overlap_size=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2_and_3)

    def test_ob_block_candset_wi_missing_vals_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ob.block_tables(A, B, l_overlap_attr_1,
                                 r_overlap_attr_1, allow_missing=True)
        validate_metadata(C)
        validate_data(C, expected_ids_4)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                                  rem_stop_words=True, overlap_size=4,
                                  allow_missing=True)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_5)

    def test_ob_block_candset_wi_missing_vals_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ob.block_tables(A, B, l_overlap_attr_1,
                                 r_overlap_attr_1, allow_missing=True)
        validate_metadata(C)
        validate_data(C, expected_ids_4)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                                  rem_stop_words=True, overlap_size=4)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ob_block_tuples_whitespace(self):
        self.assertEqual(self.ob.block_tuples(self.A.loc[1], self.B.loc[2],
                                          l_overlap_attr_1, r_overlap_attr_1),
                     False)
        self.assertEqual(self.ob.block_tuples(self.A.loc[2], self.B.loc[2],
                                          l_overlap_attr_1, r_overlap_attr_1),
                     True)

    def test_ob_block_tuples_qgram(self):
        self.assertEqual(self.ob.block_tuples(self.A.loc[1], self.B.loc[2],
                                          l_overlap_attr_1, r_overlap_attr_1,
                                          q_val=3, word_level=False),
                     False)
        self.assertEqual(self.ob.block_tuples(self.A.loc[0], self.B.loc[0],
                                          l_overlap_attr_1, r_overlap_attr_1,
                                          q_val=3, word_level=False),
                     True)

    def test_ob_block_tuples_wi_missing_vals_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        self.assertEqual(self.ob.block_tuples(A.loc[1], B.loc[3], l_overlap_attr_1,
                                          r_overlap_attr_1, allow_missing=True),
                     False)
        self.assertEqual(self.ob.block_tuples(A.loc[3], B.loc[2], l_overlap_attr_1,
                                          r_overlap_attr_1, allow_missing=True),
                     False)
        self.assertEqual(self.ob.block_tuples(A.loc[3], B.loc[3], l_overlap_attr_1,
                                          r_overlap_attr_1, allow_missing=True),
                     False)
        self.assertEqual(self.ob.block_tuples(A.loc[2], B.loc[2], l_overlap_attr_1,
                                          r_overlap_attr_1, allow_missing=True),
                     True)

    def test_ob_block_tuples_wi_missing_vals_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        self.assertEqual(self.ob.block_tuples(A.loc[1], B.loc[3], l_overlap_attr_1,
                                          r_overlap_attr_1), True)
        self.assertEqual(self.ob.block_tuples(A.loc[3], B.loc[2], l_overlap_attr_1,
                                          r_overlap_attr_1), True)
        self.assertEqual(self.ob.block_tuples(A.loc[3], B.loc[3], l_overlap_attr_1,
                                          r_overlap_attr_1), True)



class OverlapBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.ob = em.OverlapBlocker()

    def tearDown(self):
        del self.A
        del self.B
        del self.ob

    def test_ob_block_tables_njobs_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs, l_output_prefix,
                          r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_ob_block_tables_njobs_all(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=-1)
        validate_metadata(C, l_output_attrs, r_output_attrs, l_output_prefix,
                          r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_ob_block_candset_njobs_2(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                                  rem_stop_words=True, overlap_size=4,
                                  n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ob_block_candset_njobs_all(self):
        C = self.ob.block_tables(self.A, self.B, l_overlap_attr_1,
                                 r_overlap_attr_1,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ob.block_candset(C, l_overlap_attr_2, r_overlap_attr_2,
                                  rem_stop_words=True, overlap_size=4,
                                  n_jobs=-1)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

# helper functions for validating the output

def validate_metadata(C, l_output_attrs=None, r_output_attrs=None,
                      l_output_prefix='ltable_', r_output_prefix='rtable_',
                      l_key='ID', r_key='ID'):
    tc = unittest.TestCase()
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    if l_output_attrs:
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
    if r_output_attrs:
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)
    tc.assertEqual(s1, sorted(C.columns))
    tc.assertEqual(em.get_key(C), '_id')
    tc.assertEqual(em.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    tc.assertEqual(em.get_property(C, 'fk_rtable'), r_output_prefix + r_key)

def validate_data(C, expected_ids=None):
    tc = unittest.TestCase()
    if expected_ids:
        lid = em.get_property(C, 'fk_ltable')
        rid = em.get_property(C, 'fk_rtable')
        C_ids = C[[lid, rid]].set_index([lid, rid])
        actual_ids = sorted(C_ids.index.values.tolist())
        tc.assertEqual(expected_ids, actual_ids)
    else:
        tc.assertEqual(len(C), 0)

def validate_metadata_two_candsets(C, D):
    tc = unittest.TestCase()
    tc.assertEqual(sorted(C.columns), sorted(D.columns))
    tc.assertEqual(em.get_key(D), em.get_key(C))
    tc.assertEqual(em.get_property(D, 'fk_ltable'), em.get_property(C, 'fk_ltable'))
    tc.assertEqual(em.get_property(D, 'fk_rtable'), em.get_property(C, 'fk_rtable'))
