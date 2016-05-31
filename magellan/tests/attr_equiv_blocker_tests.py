import os
from nose.tools import *
import numpy as np
import time

import magellan as mg

p = mg.get_install_path()
path_for_A = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'table_A.csv'])
#path_for_A = os.sep.join([p, 'datasets', 'bowker.csv'])
path_for_B = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'table_B.csv'])
#path_for_B = os.sep.join([p, 'datasets', 'walmart.csv'])
l_key = 'ID'
r_key = 'ID'
l_block_attr_1 = 'zipcode'
l_block_attr_2 = 'birth_year'
l_block_attr_3 = 'name'
#l_block_attr = 'pubYear'
r_block_attr_1 = 'zipcode'
r_block_attr_2 = 'birth_year'
r_block_attr_3 = 'name'
#r_block_attr = 'pubYear'
l_output_attrs = ['zipcode', 'birth_year']
#l_output_attrs = ['pubYear']
r_output_attrs = ['zipcode', 'birth_year']
#r_output_attrs = ['pubYear']
l_output_prefix = 'ltable_'
r_output_prefix = 'rtable_'

def test_ab_block_tables():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, l_output_attrs,
			r_output_attrs, l_output_prefix, r_output_prefix)
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    s1 += [l_output_prefix + x for x in l_output_attrs if x not in [l_key]]
    s1 += [r_output_prefix + x for x in r_output_attrs if x not in [r_key]]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    k1 = np.array(C[l_output_prefix + l_block_attr_1])
    k2 = np.array(C[r_output_prefix + r_block_attr_1])
    assert_equal(all(k1 == k2), True)

def test_ab_block_tables_wi_no_tuples():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_3, r_block_attr_3)
    assert_equal(len(C),  0)
    assert_equal(sorted(C.columns), sorted(['_id', l_output_prefix + l_key,
		 r_output_prefix + r_key]))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)

def test_ab_block_candset():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, l_output_attrs,
			r_output_attrs, l_output_prefix, r_output_prefix)
    D = ab.block_candset(C, l_block_attr_2, r_block_attr_2)
    assert_equal(sorted(C.columns), sorted(D.columns))
    assert_equal(mg.get_key(D), '_id')
    assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
    assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
    k1 = np.array(D[l_output_prefix + l_block_attr_2])
    k2 = np.array(D[r_output_prefix + r_block_attr_2])
    assert_equal(all(k1 == k2), True)

def test_ab_block_candset_empty_input():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_3, r_block_attr_3)
    assert_equal(len(C),  0)
    D = ab.block_candset(C, l_block_attr_2, r_block_attr_2)
    assert_equal(len(D),  0)
    assert_equal(sorted(D.columns), sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
    assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

def test_ab_block_candset_empty_output():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1)
    D = ab.block_candset(C, l_block_attr_3, r_block_attr_3)
    assert_equal(len(D),  0)
    assert_equal(sorted(D.columns), sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
    assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

def test_ab_block_tuples():
    A = mg.read_csv_metadata(path_for_A)
    B = mg.read_csv_metadata(path_for_B)
    ab = mg.AttrEquivalenceBlocker()
    assert_equal(ab.block_tuples(A.ix[1], B.ix[2], l_block_attr_1,
				 r_block_attr_1), False)
    assert_equal(ab.block_tuples(A.ix[2], B.ix[2], l_block_attr_1,
				 r_block_attr_1), True)
