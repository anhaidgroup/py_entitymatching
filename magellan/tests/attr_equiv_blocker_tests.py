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

"""
def test_ab_block_tables_perf():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    start_time = time.time()
    C = ab.block_tables(A, B, l_block_attr, r_block_attr, l_output_attrs,
	r_output_attrs, l_output_prefix, r_output_prefix, verbose=False)
    print "Blocking time: ", (time.time() - start_time)
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    s1 += [l_output_prefix + x for x in l_output_attrs if x not in [l_key]]
    s1 += [r_output_prefix + x for x in r_output_attrs if x not in [r_key]]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    k1 = np.array(C[l_output_prefix + l_block_attr])
    k2 = np.array(C[r_output_prefix + r_block_attr])
    assert_equal(all(k1 == k2), True)

def test_ab_block_tables_prof():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    cProfile.run('ab.block_tables(A, B, l_block_attr, r_block_attr, l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix)')
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    s1 += [l_output_prefix + x for x in l_output_attrs]
    s1 += [r_output_prefix + x for x in r_output_attrs]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    k1 = np.array(C[l_output_prefix + l_block_attr])
    k2 = np.array(C[r_output_prefix + r_block_attr])
    assert_equal(all(k1 == k2), True)







def test_ab_block_tables_skd():
    start_time = time.time()
    #A = mg.load_dataset('bikedekho_clean', 'ID')
    A = mg.load_dataset('bowker', 'ID')
    a_load_time = time.time()
    print("Loading table A --- %s seconds ---" % (a_load_time - start_time))
    #B = mg.load_dataset('bikewale_clean', 'ID')
    B = mg.load_dataset('walmart', 'ID')
    b_load_time = time.time()
    print("Loading table B --- %s seconds ---" % (b_load_time - a_load_time))
    ab = mg.AttrEquivalenceBlocker()
    ab_time = time.time()
    print("Created an AE blocker --- %s seconds ---" % (ab_time - b_load_time))
    #C = ab.block_tables_skd(A, B, 'city_posted', 'city_posted', 'city_posted', 'city_posted')
    C = ab.block_tables_joblib(A, B, 'pubYear', 'pubYear', 'pubYear', 'pubYear')
    #C = ab.block_tables_skd(A, B, 'isbn', 'isbn', 'isbn', 'isbn')
    print("Size of candset C: %d" % (len(C)))
    c_time = time.time()
    print("Block tables --- %s seconds ---" % (c_time - ab_time))

    #s1 = sorted(['_id', 'ltable.ID', 'rtable.ID', 'ltable.city_posted', 'rtable.city_posted'])
    s1 = sorted(['_id', 'ltable.ID', 'rtable.ID', 'ltable.pubYear', 'rtable.pubYear'])
    #s1 = sorted(['_id', 'ltable.ID', 'rtable.ID', 'ltable.isbn', 'rtable.isbn'])
    assert_equal(s1, sorted(C.columns))
    assert_equal(C.get_key(), '_id')
    assert_equal(C.get_property('foreign_key_ltable'), 'ltable.ID')
    assert_equal(C.get_property('foreign_key_rtable'), 'rtable.ID')
    #k1 = np.array(C[['ltable.city_posted']])
    k1 = np.array(C[['ltable.pubYear']])
    #k1 = np.array(C[['ltable.isbn']])
    #k2 = np.array(C[['rtable.city_posted']])
    k2 = np.array(C[['rtable.pubYear']])
    #k2 = np.array(C[['rtable.isbn']])
    assert_equal(all(k1 == k2), True)

def test_ab_block_candset_skd():
    #A = mg.load_dataset('table_A')
    A = mg.load_dataset('bikedekho_clean', 'ID')
    #B = mg.load_dataset('table_B')
    B = mg.load_dataset('bikewale_clean', 'ID')
    ab = mg.AttrEquivalenceBlocker()
    #C = ab.block_tables(A, B, 'zipcode', 'zipcode', ['zipcode', 'birth_year'], ['zipcode', 'birth_year'])
    C = ab.block_tables_opt(A, B, 'city_posted', 'city_posted',
	['bike_name', 'city_posted', 'km_driven', 'price', 'color', 'model_year'],
	['bike_name', 'city_posted', 'km_driven', 'price', 'color', 'model_year'])
    print "Size of C: ", len(C)
    #D = ab.block_candset_skd(C, 'birth_year', 'birth_year')
    D = ab.block_candset_joblib(C, 'model_year', 'model_year')
    print "Size of D: ", len(D)
    #s1 = sorted(['_id', 'ltable.ID', 'rtable.ID', 'ltable.zipcode', 'ltable.birth_year', 'rtable.zipcode',
    #             'rtable.birth_year'])
    s1 = sorted(['_id', 'ltable.ID', 'rtable.ID', 'ltable.bike_name', 'ltable.city_posted',
	'ltable.km_driven', 'ltable.price', 'ltable.color', 'ltable.model_year',
	'rtable.bike_name', 'rtable.city_posted', 'rtable.km_driven', 'rtable.price',
	'rtable.color', 'rtable.model_year'])
    assert_equal(s1, sorted(D.columns))
    assert_equal(D.get_key(), '_id')
    assert_equal(D.get_property('foreign_key_ltable'), 'ltable.ID')
    assert_equal(D.get_property('foreign_key_rtable'), 'rtable.ID')
    #k1 = np.array(D[['ltable.birth_year']])
    k1 = np.array(D[['ltable.model_year']])
    #k2 = np.array(D[['rtable.birth_year']])
    k2 = np.array(D[['rtable.model_year']])
    assert_equal(all(k1 == k2), True)
"""
