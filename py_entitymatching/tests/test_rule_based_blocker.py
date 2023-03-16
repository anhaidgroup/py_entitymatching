import os
# from nose.tools import *
import pandas as pd
import unittest
from .utils import raises

import py_entitymatching as em
from py_entitymatching.feature.simfunctions import get_sim_funs_for_blocking
from py_entitymatching.feature.tokenizers import get_tokenizers_for_blocking
from py_entitymatching.feature.addfeatures import add_feature, get_feature_fn

p = em.get_install_path()
path_for_A = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_for_B = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
l_output_attrs = ['zipcode', 'birth_year']
r_output_attrs = ['zipcode', 'birth_year']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

# filterable rule with single conjunct using Jaccard sim_fn, 3g tokenization
rule_1 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3']
expected_ids_1 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# filterable rule with single conjunct using edit distance function
rule_2 = ['birth_year_birth_year_lev_dist(ltuple, rtuple) > 0']
expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                  ('a5', 'b5')]

expected_ids_1_and_2 = [('a2', 'b3'), ('a3', 'b2'), ('a5', 'b5')]

# filterable rule with multiple conjuncts - (Jaccard, 3g) & (edit dist)
rule_3 = ['name_name_jac_qgm_3_qgm_3(ltuple, rtuple) < 0.3',
          'birth_year_birth_year_lev_dist(ltuple, rtuple) > 0']
expected_ids_3 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a4', 'b1'),
                  ('a4', 'b6'), ('a5', 'b5')]

expected_ids_2_and_3 = [('a2', 'b3'), ('a3', 'b2'), ('a4', 'b1'), ('a4', 'b6'),
                        ('a5', 'b5')]

# filterable rule with multiple conjuncts - (Jaccard, 3g) & (cosine, ws)
rule_4 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3',
          'name_name_cos_dlm_dc0_dlm_dc0(ltuple, rtuple) < 0.25']
expected_ids_4 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# rule returning an empty candset
rule_5 = ['name_name_jac_dlm_dc0_dlm_dc0(ltuple,rtuple) < 0.5']

# non filterable rule with single conjunct
rule_6 = ['name_name_mel(ltuple,rtuple) < 0.6']
expected_ids_6 = [('a2', 'b1'), ('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'),
                  ('a3', 'b6'), ('a4', 'b2'), ('a5', 'b5')]

expected_ids_1_and_6 = [('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'), ('a5', 'b5')]

# non filterable rule with multiple conjuncts
rule_7 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) < 0.3',
          'name_name_mel(ltuple,rtuple) < 0.6']
expected_ids_7 = [('a2', 'b1'), ('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'),
                  ('a3', 'b6'), ('a4', 'b2'), ('a5', 'b5')]

expected_ids_6_and_7 = [('a2', 'b1'), ('a2', 'b3'), ('a2', 'b6'), ('a3', 'b2'),
                       ('a3', 'b6'), ('a4', 'b2'), ('a5', 'b5')]

# rule with supported sim_fn but unsupported op for filters (returns empty set)
rule_8 = ['name_name_jac_dlm_dc0_dlm_dc0(ltuple,rtuple) >= 0']

# rule with supported sim_fn but unsupported op (returns empty set)
rule_9 = ['zipcode_zipcode_lev_dist(ltuple,rtuple) <= 2']

class RuleBasedBlockerTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_for_A)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_for_B)
        em.set_key(self.B, 'ID')
        self.feature_table = em.get_features_for_blocking(self.A, self.B, validate_inferred_attr_types=False)
        self.rb = em.RuleBasedBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.rb

    @raises(AssertionError)
    def test_rb_block_tables_invalid_ltable_1(self):
        self.rb.block_tables(None, self.B)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_ltable_2(self):
        self.rb.block_tables([10, 10], self.B)

    @raises(KeyError)
    def test_rb_block_tables_invalid_ltable_3(self):
        self.rb.block_tables(pd.DataFrame(), self.B)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_rtable_1(self):
        self.rb.block_tables(self.A, None)

    @raises(AssertionError)
    def test_rb_block_tables_invalid_rtable_2(self):
        self.rb.block_tables(self.A, [10, 10])

    @raises(KeyError)
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

    @raises(AssertionError)
    def test_rb_block_tables_no_rules(self):
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
     
    def test_rb_block_tables_filterable_rule_single_conjunct_1(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_filterable_rule_single_conjunct_2(self):
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_2)
    
    def test_rb_block_tables_filterable_rule_multiple_conjuncts_1(self):
        self.rb.add_rule(rule_3, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_3)
     
    def test_rb_block_tables_filterable_rule_multiple_conjuncts_2(self):
        self.rb.add_rule(rule_4, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_4)
    
    def test_rb_block_tables_non_filterable_rule_single_conjunct(self):
        self.rb.add_rule(rule_6, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_6)

    def test_rb_block_tables_non_filterable_rule_multiple_conjuncts(self):
        self.rb.add_rule(rule_7, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_7)

    def test_rb_block_tables_rule_sequence_with_two_filterable_rules(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1_and_2)
    
    def test_rb_block_tables_rule_sequence_with_one_filterable_rule(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.rb.add_rule(rule_6, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1_and_6)
    
    def test_rb_block_tables_rule_sequence_with_no_filterable_rule(self):
        self.rb.add_rule(rule_6, self.feature_table)
        self.rb.add_rule(rule_7, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_6_and_7)
    
    def test_rb_block_tables_wi_no_output_tuples(self):
        self.rb.add_rule(rule_5, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
    
    def test_rb_block_tables_wi_null_l_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, None, r_output_attrs, show_progress=False)
        validate_metadata(C, None, r_output_attrs)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_null_r_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs, None, show_progress=False)
        validate_metadata(C, l_output_attrs, None)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_empty_l_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, [], r_output_attrs, show_progress=False)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_wi_empty_r_output_attrs(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs, [], show_progress=False)
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    def test_rb_block_tables_supported_sim_fn_unsupported_op_for_filters_1(self):
        self.rb.set_feature_table(self.feature_table)
        self.rb.add_rule(rule_8, None)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
   
    def test_rb_block_tables_supported_sim_fn_unsupported_op_for_filters_2(self):
        self.rb.set_feature_table(self.feature_table)
        self.rb.add_rule(rule_9, None)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
   
    def test_rb_block_tables_set_feature_table(self):
        self.rb.set_feature_table(self.feature_table)
        self.rb.add_rule(rule_1, None)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_set_feature_table_twice(self):
        self.rb.set_feature_table(self.feature_table)
        self.rb.set_feature_table(self.feature_table)
        self.rb.add_rule(rule_1, None)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    @raises(AssertionError)
    def test_rb_block_tables_no_feature_table(self):
        self.rb.add_rule(rule_1, None)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)
    
    @raises(AssertionError)
    def test_rb_block_tables_rule_with_bogus_feature(self):
        self.rb.add_rule(['bogus_feature(ltuple, rtuple) < 0.5'], self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False)

    def test_rb_block_tables_rule_wi_no_auto_gen_feature(self):
        feature_string = "jaccard(qgm_3(ltuple['name']), qgm_3(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) < 0.3'] # same as rule_1
        self.rb.add_rule(test_rule, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
    
    def test_rb_block_tables_rule_wi_diff_tokenizers(self):
        feature_string = "jaccard(qgm_3(ltuple['address']), dlm_dc0(rtuple['address']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) <= 1'] # should return an empty set
        self.rb.add_rule(test_rule, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
    
    def test_rb_block_tables_rule_wi_dice_sim_fn(self):
        feature_string = "dice(dlm_dc0(ltuple['name']), dlm_dc0(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) <= 1'] # should return an empty set
        self.rb.add_rule(test_rule, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
    
    def test_rb_block_tables_rule_wi_overlap_coeff_sim_fn(self):
        feature_string = "overlap_coeff(dlm_dc0(ltuple['name']), dlm_dc0(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) <= 1'] # should return an empty set
        self.rb.add_rule(test_rule, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)
    
    def test_rb_block_tables_rule_wi_lev_dist_geq_op(self):
        # rule should return an empty set
        test_rule = ['birth_year_birth_year_lev_dist(ltuple, rtuple) >= 0']
        self.rb.add_rule(test_rule, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        validate_metadata(C)
        validate_data(C)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_candset_1(self):
        self.rb.block_candset(None)
    
    @raises(AssertionError)
    def test_rb_block_candset_invalid_candset_2(self):
        self.rb.block_candset([10, 10])

    @raises(KeyError)
    def test_rb_block_candset_invalid_candset_3(self):
        self.rb.block_candset(pd.DataFrame())

    @raises(AssertionError)
    def test_rb_block_candset_no_rules(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_1(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, None)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, 1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_verbose_3(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, 'yes')

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_1(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, show_progress=None)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, show_progress=1)

    @raises(AssertionError)
    def test_rb_block_candset_invalid_show_progress_3(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False)
        self.rb.block_candset(C, show_progress='yes')
     
    def test_rb_block_candset(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_2, self.feature_table)
        D = self.rb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_1_and_2)
    
    def test_rb_block_candset_empty_input(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_5, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C)
        self.rb.add_rule(rule_1, self.feature_table)
        D = self.rb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D)
    
    def test_rb_block_candset_empty_output(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_5, self.feature_table)
        D = self.rb.block_candset(C)
        validate_metadata_two_candsets(C, D)
        validate_data(D)
     
    def test_rb_block_tuples(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.assertEqual(self.rb.block_tuples(self.A.loc[1], self.B.loc[2]), False)
        self.assertEqual(self.rb.block_tuples(self.A.loc[2], self.B.loc[2]), True)

    def test_rb_add_rule_user_supplied_rule_name(self):
        rule_name = self.rb.add_rule(rule_1, self.feature_table, 'myrule')
        self.assertEqual(rule_name, 'myrule')
        # view rule source
        self.rb.view_rule(rule_name)
        # get rule fn
        self.rb.get_rule(rule_name)
        # see if rule exists in the set of rules
        rule_names = self.rb.get_rule_names()
        self.assertEqual(rule_name in rule_names, True)
        
    @raises(AssertionError)
    def test_rb_add_rule_twice(self):
        rule_name = self.rb.add_rule(rule_1, self.feature_table, 'myrule')
        self.assertEqual(rule_name, 'myrule')
        # see if rule exists in the set of rules
        rule_names = self.rb.get_rule_names()
        self.assertEqual(rule_name in rule_names, True)
        rule_name = self.rb.add_rule(rule_1, self.feature_table, 'myrule')
        
    def test_rb_delete_rule(self):
        rule_name = self.rb.add_rule(rule_1, self.feature_table)
        rule_names = self.rb.get_rule_names()
        self.assertEqual(rule_name in rule_names, True)
        self.rb.delete_rule(rule_name)
        rule_names = self.rb.get_rule_names()
        self.assertEqual(rule_name in rule_names, False)
        
    @raises(AssertionError)
    def test_rb_delete_non_existing_rule(self):
        self.rb.delete_rule('bogus_rule')
        

class RuleBasedBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        self.A = em.read_csv_metadata(path_for_A)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_for_B)
        em.set_key(self.B, 'ID')
        self.feature_table = em.get_features_for_blocking(self.A, self.B, validate_inferred_attr_types=False)
        self.rb = em.RuleBasedBlocker()
        
    def tearDown(self):
        del self.A
        del self.B
        del self.rb

    def test_rb_block_tables_filterable_rule_single_conjunct_njobs_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
     
    def test_rb_block_tables_filterable_rule_multiple_conjuncts_njobs_2(self):
        self.rb.add_rule(rule_4, self.feature_table)
        #print('feature_table:', self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_4)
    
    def test_rb_block_tables_wi_no_output_tuples_njobs_2(self):
        self.rb.add_rule(rule_5, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, show_progress=False, n_jobs=2)
        validate_metadata(C)
        validate_data(C)
   
    def test_rb_block_tables_non_filterable_rule_single_conjunct_njobs_2(self):
        self.rb.add_rule(rule_6, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs, l_output_prefix,
                          r_output_prefix)
        validate_data(C, expected_ids_6)

     
    def test_rb_block_tables_non_filterable_rule_multiple_conjuncts_njobs_2(self):
        self.rb.add_rule(rule_7, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_7)
    
    def test_rb_block_tables_rule_sequence_with_two_filterable_rules_njobs_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.rb.add_rule(rule_2, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1_and_2)

    def test_rb_block_tables_rule_sequence_with_one_filterable_rules_njobs_2(self):
        self.rb.add_rule(rule_1, self.feature_table)
        self.rb.add_rule(rule_6, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, show_progress=False, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1_and_6)

    def test_rb_block_tables_rule_sequence_with_no_filterable_rule_njobs_2(self):
        self.rb.add_rule(rule_6, self.feature_table)
        self.rb.add_rule(rule_7, self.feature_table)
        C = self.rb.block_tables(self.A, self.B, l_output_attrs,
                                 r_output_attrs, l_output_prefix,
                                 r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_6_and_7)
    
    def test_rb_block_candset_njobs_2(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B, l_output_attrs,
                            r_output_attrs, l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_2, self.feature_table)
        D = self.rb.block_candset(C, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D, expected_ids_1_and_2)
    
    def test_rb_block_candset_empty_input_njobs_2(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_5, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C)
        self.rb.add_rule(rule_1, self.feature_table)
        D = self.rb.block_candset(C, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)
    
    def test_rb_block_candset_empty_output_njobs_2(self):
        rb = em.RuleBasedBlocker()
        rb.add_rule(rule_1, self.feature_table)
        C = rb.block_tables(self.A, self.B)
        validate_metadata(C)
        validate_data(C, expected_ids_1)
        self.rb.add_rule(rule_5, self.feature_table)
        D = self.rb.block_candset(C, n_jobs=2)
        validate_metadata_two_candsets(C, D)
        validate_data(D)

# helper functions for validating output

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
    
