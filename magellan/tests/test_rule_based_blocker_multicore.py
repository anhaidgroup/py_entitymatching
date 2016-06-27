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

# Jaccard 3gram name  < 0.3 - filterable rule with single conjunct
rule_1 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3']
expected_ids_1 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# Levenshtein distance birth_year > 0 - non-filterable rule with single conjunct
rule_2 = ['birth_year_birth_year_lev(ltuple, rtuple) > 0']
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                  ('a5', 'b5')]

expected_ids_1_and_2 = [('a2', 'b3'), ('a3', 'b2'), ('a5', 'b5')]

# non-filterable rule with multiple conjuncts
rule_3 = ['name_name_jac_qgm_3_qgm_3(ltuple, rtuple) < 0.3',
          'birth_year_birth_year_lev(ltuple, rtuple) > 0']
expected_ids_3 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a4', 'b1'),
                  ('a4', 'b6'), ('a5', 'b5')]

expected_ids_2_and_3 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                        ('a5', 'b5')]

# filterable rule with multiple conjuncts
rule_4 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3',
          'name_name_cos_dlm_dc0_dlm_dc0(ltuple, rtuple) < 0.25']
expected_ids_4 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# rule returning an empty candset
rule_5 = ['name_name_jac_dlm_dc0_dlm_dc0(ltuple,rtuple) < 0.5']

class RuleBasedBlockerMulticoreTestCases(unittest.TestCase):

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
     
    def validate_metadata_two_candsets(self, C, D): 
        assert_equal(sorted(C.columns), sorted(D.columns))
        assert_equal(mg.get_key(D), mg.get_key(C))
        assert_equal(mg.get_property(D, 'fk_ltable'), mg.get_property(C, 'fk_ltable'))
        assert_equal(mg.get_property(D, 'fk_rtable'), mg.get_property(C, 'fk_rtable'))

    def test_rb_block_tables_filterable_rule_single_conjunct_njobs_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
     
    def test_rb_block_tables_filterable_rule_multiple_conjuncts_njobs_2(self):
        self.rb.add_rule(rule_4, self.feature_table)
        #print('feature_table:', self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_4)
    
    def test_rb_block_tables_rule_sequence_with_one_filterable_rule_njobs_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1_and_2)

    def test_rb_block_tables_wi_no_output_tuples_njobs_2(self):
        self.rb.add_rule(rule_5, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False, n_jobs=2)
        self.validate_metadata(C)
        self.validate_data(C)
   
    def test_rb_block_tables_non_filterable_rule_single_conjunct_njobs_2(self):
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_2)

    
    def test_rb_block_tables_non_filterable_rule_multiple_conjuncts_njobs_2(self):
        self.rb.add_rule(rule_3, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_3)

    def test_rb_block_tables_rule_sequence_with_no_filterable_rule_njobs_2(self):
        self.rb.add_rule(rule_2, self.feature_table)
        self.rb.add_rule(rule_3, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, n_jobs=2)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_2_and_3)
    """
    def test_rb_block_candset(self):
        rb = mg.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        self.validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        self.validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_2, self.feature_table)
        D = self.rb.block_candset(C)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D, expected_ids_1_and_2)
    
    def test_rb_block_candset_empty_input(self):
        rb = mg.RuleBasedBlocker()
        rb.add_rule(rule_5, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        self.validate_metadata(C)
        self.validate_data(C)
        self.rb.add_rule(rule_1, self.feature_table)
        D = self.rb.block_candset(C)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)
    
    def test_rb_block_candset_empty_output(self):
        rb = mg.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        self.validate_metadata(C)
        self.validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_5, self.feature_table)
        D = self.rb.block_candset(C)
        self.validate_metadata_two_candsets(C, D)
        self.validate_data(D)
    
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
