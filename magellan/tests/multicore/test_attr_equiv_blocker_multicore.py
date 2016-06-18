import os
from nose.tools import *
import pandas as pd
import unittest

import magellan as mg

p = mg.get_install_path()
path_a = os.sep.join([p, 'datasets', 'table_A.csv'])
path_b = os.sep.join([p, 'datasets', 'table_B.csv'])
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

bogus_attr = 'bogus'
block_attr_multi = ['zipcode', 'birth_year']

# attribute equivalence on [l|r]_block_attr_1
expected_ids_1 = [('a1', 'b1'), ('a1', 'b2'), ('a1', 'b6'),
                  ('a2', 'b3'), ('a2', 'b4'), ('a2', 'b5'),
                  ('a3', 'b1'), ('a3', 'b2'), ('a3', 'b6'),
                  ('a4', 'b3'), ('a4', 'b4'), ('a4', 'b5'),
                  ('a5', 'b3'), ('a5', 'b4'), ('a5', 'b5')]

# attribute equivalence on [l|r]_block_attr_1 \intersection [l|r]_block_attr_2
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a5', 'b5')]

# overlap on birth_year with q_val=3 and overlap_size=2
expected_ids_3 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'), ('a5', 'b5')]

class AttrEquivBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        self.A = mg.read_csv_metadata(path_a)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_b)
        mg.set_key(self.B, 'ID')
        self.ab = mg.AttrEquivalenceBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.ab

    def validate_metadata(self, C, l_output_attrs=None, r_output_attrs=None,
                          l_output_prefix='ltable_', r_output_prefix='rtable_',
                          l_key='ID', r_key='ID'):
        s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
        if l_output_attrs:
            s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
        if r_output_attrs:
            s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    
    def validate_data(self, C, expected_ids=None):
        if expected_ids:
            lid = mg.get_property(C, 'fk_ltable')
            rid = mg.get_property(C, 'fk_rtable')
            C_ids = C[[lid, rid]].set_index([lid, rid])
            actual_ids = sorted(C_ids.index.values.tolist())
            assert_equal(expected_ids, actual_ids)
        else:
            assert_equal(len(C), 0)
    
    def validate_metadata_two_candsets(self, C, D): 
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), mg.get_key(C))
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

    def test_ab_block_tables_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
    
    def test_ab_block_tables_wi_no_output_tuples_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_3, r_block_attr_3, n_jobs=2)
        self.validate_metadata(C)
        self.validate_data(C)

    def test_ab_block_tables_wi_null_l_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, None, r_output_attrs, n_jobs=2)
        self.validate_metadata(C, r_output_attrs=r_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_null_r_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs, None, n_jobs=2)
        self.validate_metadata(C, l_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_l_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, [], r_output_attrs, n_jobs=2)
        self.validate_metadata(C, [], r_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_r_output_attrs_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs, [], n_jobs=2)
        self.validate_metadata(C, l_output_attrs, [])
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix, n_jobs=-1)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_no_output_tuples_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_3, r_block_attr_3, n_jobs=-1)
        self.validate_metadata(C)
        self.validate_data(C)

    def test_ab_block_tables_wi_null_l_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, None, r_output_attrs, n_jobs=-1)
        self.validate_metadata(C, r_output_attrs=r_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_null_r_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs, None, n_jobs=-1)
        self.validate_metadata(C, l_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_l_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, [], r_output_attrs, n_jobs=-1)
        self.validate_metadata(C, [], r_output_attrs)
        self.validate_data(C, expected_ids_1)

    def test_ab_block_tables_wi_empty_r_output_attrs_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs, [], n_jobs=-1)
        self.validate_metadata(C, l_output_attrs, [])
        self.validate_data(C, expected_ids_1)

    def test_ab_block_candset_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=2, show_progress=False)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D, expected_ids_2)

    def test_ab_block_candset_empty_input_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_3, r_block_attr_3)
        self.validate_metadata(C)
        self.validate_data(C)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=2, show_progress=False)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)

    def test_ab_block_candset_empty_output_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.validate_metadata(C)
        self.validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_3, r_block_attr_3, n_jobs=2, show_progress=False)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)

    def test_ab_block_candset_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=-1, show_progress=False)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D, expected_ids_2)

    def test_ab_block_candset_empty_input_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_3, r_block_attr_3)
        self.validate_metadata(C)
        self.validate_data(C)
        D = self.ab.block_candset(C, l_block_attr_2, r_block_attr_2, n_jobs=-1, show_progress=False)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)

    def test_ab_block_candset_empty_output_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.validate_metadata(C)
        self.validate_data(C, expected_ids_1)
        D = self.ab.block_candset(C, l_block_attr_3, r_block_attr_3, show_progress=False, n_jobs=-1)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)

    def test_ab_block_tuples(self):
        assert_equal(self.ab.block_tuples(self.A.ix[1], self.B.ix[2], l_block_attr_1,
                                     r_block_attr_1), False)
        assert_equal(self.ab.block_tuples(self.A.ix[2], self.B.ix[2], l_block_attr_1,
                                     r_block_attr_1), True)
    
