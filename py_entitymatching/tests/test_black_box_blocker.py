import os
from nose.tools import *
import pandas as pd
import unittest

import py_entitymatching as em
import py_entitymatching.feature.simfunctions as sim

p = em.get_install_path()
path_a = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_b = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
l_output_attrs = ['name', 'address']
r_output_attrs = ['name', 'address']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

def _block_fn(x, y):
    if (sim.monge_elkan(x['name'], y['name']) < 0.6):
        return True
    else:
        return False

# block tables using _block_fn
expected_ids_1 = [('a2', 'b1'), ('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'),
                ('a3', 'b6'), ('a4', 'b2'), ('a5', 'b5')]

# attribute equivalence on 'zipcode'
expected_ids_zip = [('a1', 'b1'), ('a1', 'b2'), ('a1', 'b6'),
                    ('a2', 'b3'), ('a2', 'b4'), ('a2', 'b5'),
                    ('a3', 'b1'), ('a3', 'b2'), ('a3', 'b6'),
                    ('a4', 'b3'), ('a4', 'b4'), ('a4', 'b5'),
                    ('a5', 'b3'), ('a5', 'b4'), ('a5', 'b5')]

# block tables using attr equiv on zipcode, then block candset using _block_fn
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a3', 'b6'), ('a5', 'b5')]

# drops all the tuple pairs
def _evil_block_fn(x, y):
    return True

class BlackBoxBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.bb = em.BlackBoxBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.bb

    @raises(AssertionError)
    def test_bb_block_tables_invalid_ltable_1(self):
        self.bb.block_tables(None, self.B)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_ltable_2(self):
        self.bb.block_tables([10, 10], self.B)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_ltable_3(self):
        self.bb.block_tables(pd.DataFrame(), self.B)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_rtable_1(self):
        self.bb.block_tables(self.A, None)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_rtable_2(self):
        self.bb.block_tables(self.A, [10, 10])

    @raises(AssertionError)
    def test_bb_block_tables_invalid_rtable_3(self):
        self.bb.block_tables(self.A, pd.DataFrame())

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_attrs_1(self):
        self.bb.block_tables(self.A, self.B, l_output_attrs=1)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_attrs_2(self):
        self.bb.block_tables(self.A, self.B, l_output_attrs='name')

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_attrs_3(self):
        self.bb.block_tables(self.A, self.B, l_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_bb_block_tables_bogus_l_output_attrs(self):
        self.bb.block_tables(self.A, self.B, l_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_attrs_1(self):
        self.bb.block_tables(self.A, self.B, r_output_attrs=1)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_attrs_2(self):
        self.bb.block_tables(self.A, self.B, r_output_attrs='name')

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_attrs_3(self):
        self.bb.block_tables(self.A, self.B, r_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_bb_block_tables_bogus_r_output_attrs(self):
        self.bb.block_tables(self.A, self.B, r_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_prefix_1(self):
        self.bb.block_tables(self.A, self.B, l_output_prefix=None)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_prefix_2(self):
        self.bb.block_tables(self.A, self.B, l_output_prefix=1)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_l_output_prefix_3(self):
        self.bb.block_tables(self.A, self.B, l_output_prefix=True)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_prefix_1(self):
        self.bb.block_tables(self.A, self.B, r_output_prefix=None)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_prefix_2(self):
        self.bb.block_tables(self.A, self.B, r_output_prefix=1)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_r_output_prefix_3(self):
        self.bb.block_tables(self.A, self.B, r_output_prefix=True)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_verbose_1(self):
        self.bb.block_tables(self.A, self.B, verbose=None)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_verbose_2(self):
        self.bb.block_tables(self.A, self.B, verbose=1)

    @raises(AssertionError)
    def test_bb_block_tables_invalid_verbose_3(self):
        self.bb.block_tables(self.A, self.B, verbose='yes')

    def test_bb_block_tables(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_empty_ltable(self):
        empty_A = pd.DataFrame(columns=self.A.columns)
        em.set_key(empty_A, 'ID')
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(empty_A, self.B)
        validate_metadata(C)
        validate_data(C)

    def test_bb_block_tables_empty_rtable(self):
        empty_B = pd.DataFrame(columns=self.B.columns)
        em.set_key(empty_B, 'ID')
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, empty_B)
        validate_metadata(C)
        validate_data(C)

    def test_bb_block_tables_wi_no_output_tuples(self):
        self.bb.set_black_box_function(_evil_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C)

    def test_bb_block_tables_wi_null_l_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, r_output_attrs=r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_wi_null_r_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_wi_empty_l_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, [], r_output_attrs)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_wi_empty_r_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs, [])
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_candset_1(self):
        self.bb.block_candset(None)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_candset_2(self):
        self.bb.block_candset([10, 10])

    @raises(KeyError)
    def test_bb_block_candset_invalid_candset_3(self):
        self.bb.set_black_box_function(_block_fn)
        self.bb.block_candset(pd.DataFrame())

    @raises(AssertionError)
    def test_bb_block_candset_invalid_verbose_1(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, verbose=None)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_verbose_2(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, verbose=1)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_verbose_3(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, verbose='yes')

    @raises(AssertionError)
    def test_bb_block_candset_invalid_show_progress_1(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, show_progress=None)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_show_progress_2(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, show_progress=1)

    @raises(AssertionError)
    def test_bb_block_candset_invalid_show_progress_3(self):
        C = self.bb.block_tables(self.A, self.B)
        self.bb.block_candset(C, show_progress='yes')

    def test_bb_block_candset(self):
        ab = em.AttrEquivalenceBlocker()
        C = ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                            l_output_attrs, r_output_attrs,
                            l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_zip)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_bb_block_candset_empty_input(self):
        self.bb.set_black_box_function(_evil_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_bb_block_candset_empty_output(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        self.bb.set_black_box_function(_evil_block_fn)
        D = self.bb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

    def test_bb_block_tuples(self):
        self.bb.set_black_box_function(_block_fn)
        assert_equal(self.bb.block_tuples(self.A.loc[1], self.B.loc[2]),
                     False)
        assert_equal(self.bb.block_tuples(self.A.loc[2], self.B.loc[2]),
                     True)


class BlackBoxBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_a)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_b)
        em.set_key(self.B, 'ID')
        self.bb = em.BlackBoxBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.bb

    def test_bb_block_tables_wi_global_black_box_fn_njobs_2(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_wi_local_black_box_fn_njobs_2(self):
        def block_fn(x, y):                                                           
            if (sim.monge_elkan(x['name'], y['name']) < 0.6):                           
                return True                                                             
            else:                                                                       
                return False  
        self.bb.set_black_box_function(block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_wi_class_black_box_fn_njobs_2(self):
        class dummy:
            def block_fn(self, x, y):                                                           
                if (sim.monge_elkan(x['name'], y['name']) < 0.6):                           
                    return True                                                             
                else:                                                                       
                    return False  
        self.bb.set_black_box_function(dummy().block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_bb_block_tables_njobs_all(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix,
                                 r_output_prefix=r_output_prefix, n_jobs=-1)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_bb_block_candset_njobs_2(self):
        ab = em.AttrEquivalenceBlocker()
        C = ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                            l_output_attrs, r_output_attrs,
                            l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_zip)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)

    def test_bb_block_candset_njobs_all(self):
        ab = em.AttrEquivalenceBlocker()
        C = ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_zip)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C, n_jobs=-1)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_2)



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
