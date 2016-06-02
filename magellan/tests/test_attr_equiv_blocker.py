import os
from nose.tools import *
import pandas as pd
#import numpy as np

import magellan as mg

p = mg.get_install_path()
path_for_A = os.sep.join([p, 'datasets', 'table_A.csv'])
path_for_B = os.sep.join([p, 'datasets', 'table_B.csv'])
l_key = 'ID'
r_key = 'ID'
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

def test_ab_block_tables():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, l_output_attrs,
			r_output_attrs, l_output_prefix, r_output_prefix)
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
    s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    k1 = pd.np.array(C[l_output_prefix + l_block_attr_1])
    k2 = pd.np.array(C[r_output_prefix + r_block_attr_1])
    assert_equal(all(k1 == k2), True)

def test_ab_block_tables_wi_no_output_tuples():
    A = mg.read_csv_metadata(path_for_A)
    mg.set_key(A, l_key)
    B = mg.read_csv_metadata(path_for_B)
    mg.set_key(B, r_key)
    ab = mg.AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, l_block_attr_3, r_block_attr_3)
    assert_equal(len(C),  0)
    assert_equal(sorted(C.columns), sorted(['_id', 'ltable_' + l_key,
		 'rtable_' + r_key]))
    assert_equal(mg.get_key(C), '_id')
    assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
    assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)

def test_ab_block_tables_wi_null_l_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, None, r_output_attrs)       
    s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]              
    s1 += ['rtable_' + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)                                                             
    assert_equal(s1, sorted(C.columns))                                         
    assert_equal(mg.get_key(C), '_id')                                          
    assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)      
    assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)

def test_ab_block_tables_wi_null_r_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, l_output_attrs, None)       
    s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]              
    s1 += ['ltable_' + x for x in l_output_attrs if x != l_key]
    s1 = sorted(s1)                                                             
    assert_equal(s1, sorted(C.columns))                                         
    assert_equal(mg.get_key(C), '_id')                                          
    assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)      
    assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)

def test_ab_block_tables_wi_empty_l_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, [], r_output_attrs)       
    s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]              
    s1 += ['rtable_' + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)                                                             
    assert_equal(s1, sorted(C.columns))                                         
    assert_equal(mg.get_key(C), '_id')                                          
    assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)      
    assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)

def test_ab_block_tables_wi_empty_r_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, l_output_attrs, [])       
    s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]              
    s1 += ['ltable_' + x for x in l_output_attrs if x != l_key]
    s1 = sorted(s1)                                                             
    assert_equal(s1, sorted(C.columns))                                         
    assert_equal(mg.get_key(C), '_id')                                          
    assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)      
    assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)

@raises(AssertionError)
def test_ab_block_tables_bogus_l_block_attr():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, bogus_attr, r_block_attr_1)       

@raises(AssertionError)
def test_ab_block_tables_bogus_r_block_attr():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, bogus_attr)       

@raises(AssertionError)
def test_ab_block_tables_multi_l_block_attr():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, block_attr_multi, r_block_attr_1)       

@raises(AssertionError)
def test_ab_block_tables_multi_r_block_attr():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, block_attr_multi)       

@raises(AssertionError)
def test_ab_block_tables_bogus_l_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, [bogus_attr])       

@raises(AssertionError)
def test_ab_block_tables_bogus_r_output_attrs():                                                     
    A = mg.read_csv_metadata(path_for_A)                                        
    mg.set_key(A, l_key)                                                        
    B = mg.read_csv_metadata(path_for_B)                                        
    mg.set_key(B, r_key)                                                        
    ab = mg.AttrEquivalenceBlocker()                                            
    C = ab.block_tables(A, B, l_block_attr_1, r_block_attr_1, None, [bogus_attr])       

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
    k1 = pd.np.array(D[l_output_prefix + l_block_attr_2])
    k2 = pd.np.array(D[r_output_prefix + r_block_attr_2])
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
