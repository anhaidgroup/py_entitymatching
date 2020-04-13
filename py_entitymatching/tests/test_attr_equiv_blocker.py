import os
from nose.tools import *
import pandas as pd
import unittest

import py_entitymatching as em

p = em.get_install_path()
path_a = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_b = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
l_block_attr_1 = 'zipcode'
l_block_attr_2 = 'birth_year'
l_block_attr_3 = 'name'
r_block_attr_1 = 'zipcode'
r_block_attr_2 = 'birth_year'
r_block_attr_3 = 'name'
l_output_attrs = ['zipcode', 'birth_year']
r_output_attrs = ['zipcode', 'birth_year']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

# attribute equivalence on [l|r]_block_attr_1
expected_ids_1 = [('a1', 'b1'), ('a1', 'b2'), ('a1', 'b6'),
                  ('a2', 'b3'), ('a2', 'b4'), ('a2', 'b5'),
                  ('a3', 'b1'), ('a3', 'b2'), ('a3', 'b6'),
                  ('a4', 'b3'), ('a4', 'b4'), ('a4', 'b5'),
                  ('a5', 'b3'), ('a5', 'b4'), ('a5', 'b5')]

# attribute equivalence on [l|r]_block_attr_1 \intersection [l|r]_block_attr_2
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a5', 'b5')]

# attr equiv on [l|r]_block_attr_1 in tables with missing vals, allow_missing = True
expected_ids_3 = [('a1', 'b1'), ('a1', 'b2'), ('a1', 'b3'), ('a1', 'b4'),
                  ('a1', 'b5'), ('a1', 'b6'), ('a2', 'b2'), ('a2', 'b3'),
                  ('a2', 'b4'), ('a2', 'b5'), ('a3', 'b1'), ('a3', 'b2'),
                  ('a3', 'b4'), ('a3', 'b6'), ('a4', 'b1'), ('a4', 'b2'),
                  ('a4', 'b3'), ('a4', 'b4'), ('a4', 'b5'), ('a4', 'b6'),
                  ('a5', 'b2'), ('a5', 'b3'), ('a5', 'b4'), ('a5', 'b5')]

# attr equiv on [l|r]_block_attr_1 in tables with missing vals, allow_missing = False
expected_ids_4 = [('a2', 'b3'), ('a2', 'b5'), ('a3', 'b1'), ('a3', 'b6'),
                  ('a5', 'b3'), ('a5', 'b5')]

# expected_ids_4 \intersection attr equiv on [l|r]_block_attr_2 in tables with
# missing vals, allow_missing = True
expected_ids_5 = [('a2', 'b3'), ('a2', 'b5'), ('a5', 'b5')]

class AttrEquivBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.ab = em.AttrEquivalenceBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.ab

    @raises(AssertionError)
    def test_ab_block_tables_invalid_ltable_1(self):
        self.ab.block_tables(None, self.B, l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_ltable_2(self):
        self.ab.block_tables([10, 10], self.B, l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_ltable_3(self):
        self.ab.block_tables(pd.DataFrame(), self.B,
                             l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_rtable_1(self):
        self.ab.block_tables(self.A, None, l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_rtable_2(self):
        self.ab.block_tables(self.A, [10, 10], l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_rtable_3(self):
        self.ab.block_tables(self.A, pd.DataFrame(),
                             l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_block_attr_1(self):
        self.ab.block_tables(self.A, self.B, None, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_block_attr_2(self):
        self.ab.block_tables(self.A, self.B, 10, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_block_attr_3(self):
        self.ab.block_tables(self.A, self.B, True, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_bogus_l_block_attr(self):
        self.ab.block_tables(self.A, self.B, 'bogus_attr', r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_multi_l_block_attr(self):
        self.ab.block_tables(self.A, self.B, ['zipcode', 'birth_year'],
                             r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_block_attr_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_block_attr_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, 10)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_block_attr_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, True)

    @raises(AssertionError)
    def test_ab_block_tables_bogus_r_block_attr(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, 'bogus_attr')

    @raises(AssertionError)
    def test_ab_block_tables_multi_r_block_attr(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1,
                             ['zipcode', 'birth_year'])

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_attrs_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, 1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_attrs_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             'name')

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_attrs_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             [1, 2])

    @raises(AssertionError)
    def test_ab_block_tables_bogus_l_output_attrs(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             ['bogus_attr'])

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_attrs_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_attrs=1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_attrs_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_attrs='name')

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_attrs_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_ab_block_tables_bogus_r_output_attrs(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_prefix_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             l_output_prefix=None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_prefix_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             l_output_prefix=1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_l_output_prefix_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             l_output_prefix=True)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_prefix_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_prefix=None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_prefix_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_prefix=1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_r_output_prefix_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             r_output_prefix=True)
    @raises(AssertionError)
    def test_ab_block_tables_invalid_allow_missing_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             allow_missing=None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_allow_misisng_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             allow_missing=1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_allow_missing_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             allow_missing='yes')

    @raises(AssertionError)
    def test_ab_block_tables_invalid_verbose_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             verbose=None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_verbose_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             verbose=1)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_verbose_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             verbose='yes')

    @raises(AssertionError)
    def test_ab_block_tables_invalid_njobs_1(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             n_jobs=None)

    @raises(AssertionError)
    def test_ab_block_tables_invalid_njobs_2(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             n_jobs='1')

    @raises(AssertionError)
    def test_ab_block_tables_invalid_njobs_3(self):
        self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1,
                             n_jobs=1.5)

    def test_ab_block_tables(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_no_output_tuples(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3)
        validate_metadata(C)
        validate_data(C)

    def test_ab_block_tables_wi_null_l_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 None, r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_null_r_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, None)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_l_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 [], r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_r_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, [])
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1, [], [])
        validate_metadata(C)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_block_attr_not_in_output_attrs(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 ['birth_year'], ['birth_year'])
        validate_metadata(C, ['birth_year'], ['birth_year'])
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_missing_values_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ab.block_tables(A, B, l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix, True)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_3)

    def test_ab_block_tables_wi_missing_values_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ab.block_tables(A, B, l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_4)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_candset_1(self):
        self.ab.block_candset(None, l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_candset_2(self):
        self.ab.block_candset([10, 10], l_block_attr_1, r_block_attr_1)

    @raises(KeyError)
    def test_ab_block_candset_invalid_candset_3(self):
        self.ab.block_candset(pd.DataFrame(), l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_l_block_attr_1(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, None, r_block_attr_2)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_l_block_attr_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, 10, r_block_attr_2)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_l_block_attr_3(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, True, r_block_attr_2)

    @raises(AssertionError)
    def test_ab_block_candset_bogus_l_block_attr(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, 'bogus_attr', r_block_attr_2)

    @raises(AssertionError)
    def test_ab_block_candset_multi_l_block_attr(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, ['zipcode', 'birth_year'], r_block_attr_2)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_r_block_attr_1(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, None)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_r_block_attr_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, 10)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_r_block_attr_3(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, True)

    @raises(AssertionError)
    def test_ab_block_candset_bogus_r_block_attr(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, 'bogus_attr')

    @raises(AssertionError)
    def test_ab_block_candset_multi_r_block_attr(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, ['zipcode', 'birth_year'])

    @raises(AssertionError)
    def test_ab_block_candset_invalid_verbose_1(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, verbose=None)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_verbose_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, verbose=1)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_verbose_3(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, verbose='yes')

    @raises(AssertionError)
    def test_ab_block_candset_invalid_show_progress_1(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                              show_progress=None)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_show_progress_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                              show_progress=1)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_show_progress_3(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                              show_progress='yes')

    @raises(AssertionError)
    def test_ab_block_candset_invalid_njobs_1(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=None)

    @raises(AssertionError)
    def test_ab_block_candset_invalid_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs='1')

    @raises(AssertionError)
    def test_ab_block_candset_invalid_njobs_3(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=1.5)

    def test_ab_block_candset(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ab_block_candset_empty_input(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3)
        validate_metadata(C)
        validate_data(C)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                                  show_progress=False)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ab_block_candset_empty_output(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_3, r_block_attr_3,
                                  show_progress=False)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ab_block_candset_wi_missing_values_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ab.block_tables(A, B, l_block_attr_1, r_block_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_4)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                                  allow_missing=True)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_5)

    def test_ab_block_candset_wi_missing_values_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.ab.block_tables(A, B, l_block_attr_1, r_block_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_4)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, [('a5','b5')])


    def test_ab_block_tuples(self):
        assert_equal(self.ab.block_tuples(self.A.loc[1], self.B.loc[2],
                                          l_block_attr_1, r_block_attr_1),
                     False)
        assert_equal(self.ab.block_tuples(self.A.loc[2], self.B.loc[2],
                                          l_block_attr_1, r_block_attr_1),
                     True)

    def test_ab_block_tuples_wi_missing_values_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        assert_equal(self.ab.block_tuples(A.loc[0], B.loc[0], l_block_attr_1,
                                          r_block_attr_1, allow_missing=True),
                     False)
        assert_equal(self.ab.block_tuples(A.loc[1], B.loc[2], l_block_attr_1,
                                          r_block_attr_1, allow_missing=True),
                     False)
        assert_equal(self.ab.block_tuples(A.loc[2], B.loc[1], l_block_attr_1,
                                          r_block_attr_1, allow_missing=True),
                     False)
        assert_equal(self.ab.block_tuples(A.loc[0], B.loc[1], l_block_attr_1,
                                          r_block_attr_1, allow_missing=True),
                     False)
        assert_equal(self.ab.block_tuples(A.loc[2], B.loc[2], l_block_attr_1,
                                          r_block_attr_1, allow_missing=True),
                     True)

    def test_ab_block_tuples_wi_missing_values_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        assert_equal(self.ab.block_tuples(A.loc[0], B.loc[0], l_block_attr_1,
                                          r_block_attr_1), True)
        assert_equal(self.ab.block_tuples(A.loc[1], B.loc[2], l_block_attr_1,
                                          r_block_attr_1), False)
        assert_equal(self.ab.block_tuples(A.loc[2], B.loc[1], l_block_attr_1,
                                          r_block_attr_1), True)
        assert_equal(self.ab.block_tuples(A.loc[0], B.loc[1], l_block_attr_1,
                                          r_block_attr_1), True)
        assert_equal(self.ab.block_tuples(A.loc[2], B.loc[2], l_block_attr_1,
                                          r_block_attr_1), True)


class AttrEquivBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.ab = em.AttrEquivalenceBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.ab

    def test_ab_block_tables_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
    
    def test_ab_block_tables_wi_no_output_tuples_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3, n_jobs=2)
        validate_metadata(C)
        validate_data(C)

    def test_ab_block_tables_wi_null_l_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 None, r_output_attrs, n_jobs=2)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_null_r_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, None, n_jobs=2)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_l_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 [], r_output_attrs, n_jobs=2)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_r_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, [], n_jobs=2)
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix, n_jobs=-1)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_no_output_tuples_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3, n_jobs=-1)
        validate_metadata(C)
        validate_data(C)

    def test_ab_block_tables_wi_null_l_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 None, r_output_attrs, n_jobs=-1)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_null_r_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, None, n_jobs=-1)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_l_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 [], r_output_attrs, n_jobs=-1)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_r_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1,
                                 r_block_attr_1, l_output_attrs, [], n_jobs=-1)
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    def test_ab_block_candset_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ab_block_candset_empty_input_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3)
        validate_metadata(C)
        validate_data(C)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                                  show_progress=False, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ab_block_candset_empty_output_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_3, r_block_attr_3,
                                  show_progress=False, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ab_block_candset_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=-1)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_ab_block_candset_empty_input_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_3, r_block_attr_3)
        validate_metadata(C)
        validate_data(C)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2,
                                  show_progress=False, n_jobs=-1)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_ab_block_candset_empty_output_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B,
                                 l_block_attr_1, r_block_attr_1)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_3, r_block_attr_3,
                                  show_progress=False, n_jobs=-1)
        validate_metadata_two_candsets(C, D)
        validate_data(D)



# helper functions for validating the output
    
def validate_metadata(C, l_output_attrs=None, r_output_attrs=None,
                      l_output_prefix='ltable_', r_output_prefix='rtable_',
                      l_key='ID', r_key='ID'):
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    if l_output_attrs:
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
    if r_output_attrs:
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(em.get_key(C), '_id')
    assert_equal(em.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(em.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    
def validate_data(C, expected_ids=None):
    if expected_ids:
        lid = em.get_property(C, 'fk_ltable')
        rid = em.get_property(C, 'fk_rtable')
        C_ids = C[[lid, rid]].set_index([lid, rid])
        actual_ids = sorted(C_ids.index.values.tolist())
        assert_equal(expected_ids, actual_ids)
    else:
        assert_equal(len(C), 0)
    
def validate_metadata_two_candsets(C, D): 
    assert_equal(sorted(C.columns), sorted(D.columns))
    assert_equal(em.get_key(D), em.get_key(C))
    assert_equal(em.get_property(D, 'fk_ltable'), em.get_property(C, 'fk_ltable'))
    assert_equal(em.get_property(D, 'fk_rtable'), em.get_property(C, 'fk_rtable'))
