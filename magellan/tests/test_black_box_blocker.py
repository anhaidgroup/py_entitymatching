import os
from nose.tools import *
import pandas as pd
import unittest

import magellan as mg
import magellan.feature.simfunctions as sim

p = mg.get_install_path()
path_for_A = os.sep.join([p, 'datasets', 'table_A.csv'])
path_for_B = os.sep.join([p, 'datasets', 'table_B.csv'])
l_key = 'ID'
r_key = 'ID'
l_output_attrs = ['name', 'address']
r_output_attrs = ['name', 'address']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

expected_ids = [('a2', 'b1'), ('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'),
                ('a3', 'b6'), ('a4', 'b2'), ('a5', 'b5')]
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a3', 'b6'), ('a5', 'b5')]

def _block_fn(x, y):
    if (sim.monge_elkan(x['name'], y['name']) < 0.6):
        return True
    else:
        return False

def _evil_block_fn(x, y):
    return True

class BlackBoxBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, l_key)
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, r_key)
        self.bb = mg.BlackBoxBlocker()
        self.ab = mg.AttrEquivalenceBlocker()
        
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
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix)
        s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
        C_ids = C[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(C_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.values.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_njobs_2(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix, n_jobs=2)
        s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
        C_ids = C[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(C_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.values.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_njobs_all(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                                 l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix, n_jobs=-1)
        s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
        C_ids = C[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(C_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.values.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_empty_ltable(self):
        empty_A = pd.DataFrame(columns=self.A.columns)
        mg.set_key(empty_A, l_key)
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(empty_A, self.B)
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        assert_equal(len(C), 0)

    def test_bb_block_tables_empty_rtable(self):
        empty_B = pd.DataFrame(columns=self.B.columns)
        mg.set_key(empty_B, r_key)
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, empty_B)
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        assert_equal(len(C), 0)

    def test_bb_block_tables_wi_no_output_tuples(self):
        self.bb.set_black_box_function(_evil_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        assert_equal(sorted(C.columns), sorted(['_id', 'ltable_' + l_key,
                                                'rtable_' + r_key]))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        assert_equal(len(C),  0)

    def test_bb_block_tables_wi_null_l_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=None,
                                 r_output_attrs=r_output_attrs)
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 += ['rtable_' + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        C_ids = C[['ltable_' + l_key, 'rtable_' + r_key]]
        actual_ids = sorted(C_ids.set_index(['ltable_' + l_key, 'rtable_' + r_key]).index.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_wi_null_r_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=None)
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 += ['ltable_' + x for x in l_output_attrs if x != l_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        C_ids = C[['ltable_' + l_key, 'rtable_' + r_key]]
        actual_ids = sorted(C_ids.set_index(['ltable_' + l_key, 'rtable_' + r_key]).index.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_wi_empty_l_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=[], r_output_attrs=r_output_attrs)
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 += ['rtable_' + x for x in r_output_attrs if x != r_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        C_ids = C[['ltable_' + l_key, 'rtable_' + r_key]]
        actual_ids = sorted(C_ids.set_index(['ltable_' + l_key, 'rtable_' + r_key]).index.tolist())
        assert_equal(expected_ids, actual_ids)

    def test_bb_block_tables_wi_empty_r_output_attrs(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B,
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=[])
        s1 = ['_id', 'ltable_' + l_key, 'rtable_' + r_key]
        s1 += ['ltable_' + x for x in l_output_attrs if x != l_key]
        s1 = sorted(s1)
        assert_equal(s1, sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(C, 'fk_ltable'), 'ltable_' + l_key)
        assert_equal(mg.get_property(C, 'fk_rtable'), 'rtable_' + r_key)
        C_ids = C[['ltable_' + l_key, 'rtable_' + r_key]]
        actual_ids = sorted(C_ids.set_index(['ltable_' + l_key, 'rtable_' + r_key]).index.tolist())
        assert_equal(expected_ids, actual_ids)

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
        C = self.ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                            l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                            l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C)
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        D_ids = D[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(D_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.tolist())
        assert_equal(expected_ids_2, actual_ids)

    def test_bb_block_candset_njobs_2(self):
        C = self.ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                            l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                            l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C, n_jobs=2)
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        D_ids = D[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(D_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.tolist())
        assert_equal(expected_ids_2, actual_ids)

    def test_bb_block_candset_njobs_all(self):
        C = self.ab.block_tables(self.A, self.B, 'zipcode', 'zipcode',
                            l_output_attrs=l_output_attrs, r_output_attrs=r_output_attrs,
                            l_output_prefix=l_output_prefix, r_output_prefix=r_output_prefix)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C, n_jobs=-1)
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        D_ids = D[[l_output_prefix + l_key, r_output_prefix + r_key]]
        actual_ids = sorted(D_ids.set_index([l_output_prefix + l_key, r_output_prefix + r_key]).index.tolist())
        assert_equal(expected_ids_2, actual_ids)

    def test_bb_block_candset_empty_input(self):
        self.bb.set_black_box_function(_evil_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        assert_equal(len(C), 0)
        self.bb.set_black_box_function(_block_fn)
        D = self.bb.block_candset(C)
        assert_equal(sorted(D.columns), sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        assert_equal(len(D), 0)

    def test_bb_block_candset_empty_output(self):
        self.bb.set_black_box_function(_block_fn)
        C = self.bb.block_tables(self.A, self.B)
        self.bb.set_black_box_function(_evil_block_fn)
        D = self.bb.block_candset(C)
        assert_equal(sorted(D.columns), sorted(C.columns))
        assert_equal(mg.get_key(D), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        assert_equal(len(D), 0)
