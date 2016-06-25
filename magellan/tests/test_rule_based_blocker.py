import os
from nose.tools import *
import pandas as pd
import unittest

import magellan as mg

p = mg.get_install_path()
path_for_A = os.sep.join([p, 'datasets', 'table_A.csv'])
path_for_B = os.sep.join([p, 'datasets', 'table_B.csv'])
l_output_attrs = ['zipcode', 'birth_year']
r_output_attrs = ['zipcode', 'birth_year']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

# Jaccard 3gram name  < 0.3
rule_1 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3']
expected_ids_1 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# Levenshtein distance birth_year > 0 -- currently a non-filterable rule 
rule_2 = ['birth_year_birth_year_lev(ltuple, rtuple) > 0']
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                  ('a5', 'b5')]

# Jaccard whitespace name < 0.5 -- should return an empty candset
rule_3 = ['name_name_jac_dlm_dc0_dlm_dc0(ltuple,rtuple) < 0.5']

class RuleBasedBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')
        self.feature_table = mg.get_features_for_blocking(self.A, self.B)
        self.rb = mg.RuleBasedBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.rb

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
        #print('Expected ids: ', expected_ids)
        if expected_ids:
            lid = mg.get_property(C, 'fk_ltable')
            rid = mg.get_property(C, 'fk_rtable')
            C_ids = C[[lid, rid]].set_index([lid, rid])
            actual_ids = sorted(C_ids.index.values.tolist())
            #print('Actual ids: ', actual_ids)
            assert_equal(expected_ids, actual_ids)
        else:
            assert_equal(len(C), 0)
     
    @raises(AssertionError)
    def test_rb_block_tables_invalid_ltable_1(self):
        self.rb.block_tables(None, self.B)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_ltable_2(self):
        self.rb.block_tables([10, 10], self.B)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_ltable_3(self):
        self.rb.block_tables(pd.DataFrame(), self.B)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_rtable_1(self):
        self.rb.block_tables(self.A, None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_rtable_2(self):
        self.rb.block_tables(self.A, [10, 10])

    @raises(AssertionError)
    def test_rb_block_tables_invalid_rtable_3(self):
        self.rb.block_tables(self.A, pd.DataFrame())

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_attrs_1(self):
        self.rb.block_tables(self.A, self.B, 1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_attrs_2(self):
        self.rb.block_tables(self.A, self.B, 'name')

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_attrs_3(self):
        self.rb.block_tables(self.A, self.B, [1, 2])

    @raises(AssertionError)
    def test_rb_block_tables_bogus_l_output_attrs(self):
        self.rb.block_tables(self.A, self.B, ['bogus'])

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_attrs_1(self):
        self.rb.block_tables(self.A, self.B, r_output_attrs=1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_attrs_2(self):
        self.rb.block_tables(self.A, self.B, r_output_attrs='name')

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_attrs_3(self):
        self.rb.block_tables(self.A, self.B, r_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_rb_block_tables_bogus_r_output_attrs(self):
        self.rb.block_tables(self.A, self.B, r_output_attrs=['bogus'])

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_prefix_1(self):
        self.rb.block_tables(self.A, self.B, l_output_prefix=None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_prefix_2(self):
        self.rb.block_tables(self.A, self.B, l_output_prefix=1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_l_output_prefix_3(self):
        self.rb.block_tables(self.A, self.B, l_output_prefix=True)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_prefix_1(self):
        self.rb.block_tables(self.A, self.B, r_output_prefix=None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_prefix_2(self):
        self.rb.block_tables(self.A, self.B, r_output_prefix=1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_r_output_prefix_3(self):
        self.rb.block_tables(self.A, self.B, r_output_prefix=True)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_verbose_1(self):
        self.rb.block_tables(self.A, self.B, verbose=None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_verbose_2(self):
        self.rb.block_tables(self.A, self.B, verbose=1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_verbose_3(self):
        self.rb.block_tables(self.A, self.B, verbose='yes')

    @raises(AssertionError)
    def test_rb_block_tables_invalid_show_progress_1(self):
        self.rb.block_tables(self.A, self.B, show_progress=None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_show_progress_2(self):
        self.rb.block_tables(self.A, self.B, show_progress=1)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_show_progress_3(self):
        self.rb.block_tables(self.A, self.B, show_progress='yes')
    
    def test_rb_block_tables_filterable_rule_single_conjunct(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_non_filterable_rule_single_conjunct(self):
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_2)
    
    def test_rb_block_tables_wi_no_output_tuples(self):
        self.rb.add_rule(rule_3, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.validate_metadata(C)
        self.validate_data(C)
    
    def test_rb_block_tables_wi_null_l_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, None, r_output_attrs, show_progress=False)
        self.validate_metadata(C, None, r_output_attrs)
        self.validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_null_r_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs, None, show_progress=False)
        self.validate_metadata(C, l_output_attrs, None)
        self.validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_empty_l_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, [], r_output_attrs, show_progress=False)
        self.validate_metadata(C, [], r_output_attrs)
        self.validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_empty_r_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs, [], show_progress=False)
        self.validate_metadata(C, l_output_attrs, [])
        self.validate_data(C, expected_ids_1)
    """
    @raises(AssertionError)
    def test_rb_block_candset_invalid_candset_1(self):
        self.rb.block_candset(None, l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_candset_2(self):
        self.rb.block_candset([10, 10], l_block_attr_1, r_block_attr_1)

    @raises(KeyError)
    def test_rb_block_candset_invalid_candset_3(self):
        self.rb.block_candset(pd.DataFrame(), l_block_attr_1, r_block_attr_1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_l_block_attr_1(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, None, r_block_attr_2)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_l_block_attr_2(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, 10, r_block_attr_2)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_l_block_attr_3(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, True, r_block_attr_2)

    @raises(AssertionError)
    def test_rb_block_candset_bogus_l_block_attr(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, bogus_attr, r_block_attr_2)

    @raises(AssertionError)
    def test_rb_block_candset_multi_l_block_attr(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, block_attr_multi, r_block_attr_2)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_r_block_attr_1(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, None)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_r_block_attr_2(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, 10)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_r_block_attr_3(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, True)

    @raises(AssertionError)
    def test_rb_block_candset_bogus_r_block_attr(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, bogus_attr)

    @raises(AssertionError)
    def test_rb_block_candset_multi_r_block_attr(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, block_attr_multi)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_1(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, None)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_2(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, 1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_3(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, 'yes')

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_1(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, show_progress=None)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_2(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, show_progress=1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_3(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        self.rb.block_candset(C, l_block_attr_2, r_block_attr_2, show_progress='yes')

    def test_rb_block_candset(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        D = self.rb.block_candset(C, l_block_attr_2, r_block_attr_2)
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))
        k1 = pd.np.array(D[l_output_prefix + l_block_attr_2])
        k2 = pd.np.array(D[r_output_prefix + r_block_attr_2])
        assert_equal(all(k1 == k2), True)

    def test_rb_block_candset_empty_input(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_3, r_block_attr_3)
        assert_equal(len(C),  0)
        D = self.rb.block_candset(C, l_block_attr_2, r_block_attr_2)
        assert_equal(len(D),  0)
        assert_equal(sorted(D.columns), sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

    def test_rb_block_candset_empty_output(self):
        C = self.rb.block_tables(self.A, self.B, l_block_attr_1, r_block_attr_1)
        D = self.rb.block_candset(C, l_block_attr_3, r_block_attr_3)
        assert_equal(len(D),  0)
        assert_equal(sorted(D.columns), sorted(C.columns))
        assert_equal(mg.get_key(C), '_id')
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

    def test_rb_block_tuples(self):
        assert_equal(self.rb.block_tuples(self.A.ix[1], self.B.ix[2], l_block_attr_1,
                                     r_block_attr_1), False)
        assert_equal(self.rb.block_tuples(self.A.ix[2], self.B.ix[2], l_block_attr_1,
                                     r_block_attr_1), True)
     
    def test_block_tables_name_cos(self):
        path_a = os.sep.join([p, 'datasets', 'example_datasets', 'electronics', 'A.csv'])
        path_b = os.sep.join([p, 'datasets', 'example_datasets', 'electronics', 'B.csv'])
        A = mg.read_csv_metadata(path_a)
        mg.set_key(A, 'ID')
        B = mg.read_csv_metadata(path_b)
        mg.set_key(B, 'ID')
        rb = mg.RuleBasedBlocker()
        feature_table = mg.get_features_for_blocking(A, B)
        rb.add_rule(['Name_Name_cos_dlm_dc0_dlm_dc0(ltuple,rtuple) < 0.3'],
                    feature_table)
        C = rb.block_tables_skd(A, B, ['Name'], ['Name'], show_progress=True)
        C.to_csv('elec_name_cos.csv', index=False)
        print('size of C:', len(C))
   """
