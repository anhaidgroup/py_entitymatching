import os
from nose.tools import *
import pandas as pd
import unittest

import py_entitymatching as em
from py_entitymatching.feature.simfunctions import get_sim_funs_for_blocking
from py_entitymatching.feature.tokenizers import get_tokenizers_for_blocking
from py_entitymatching.feature.addfeatures import add_feature, get_feature_fn

p = em.get_install_path()
path_for_A = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_for_B = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
path_for_C = os.sep.join([p, 'tests', 'test_datasets', 'C.csv'])
l_output_attrs = ['zipcode', 'birth_year']
r_output_attrs = ['zipcode', 'birth_year']
l_output_prefix = 'l_'
r_output_prefix = 'r_'

# rule with single conjunct using Jaccard sim_fn, 3g tokenization
rule_1 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) > 0.4']
expected_labels_1 = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

# rule with single conjunct using edit distance function
rule_2 = ['birth_year_birth_year_lev_dist(ltuple, rtuple) < 1']
expected_labels_2 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

expected_labels_1_and_2 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

# rule with multiple conjuncts - (Jaccard, 3g) & (edit dist)
rule_3 = ['name_name_jac_qgm_3_qgm_3(ltuple, rtuple) > 0.4',
          'birth_year_birth_year_lev_dist(ltuple, rtuple) < 1']
expected_labels_3 = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

expected_labels_2_and_3 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

# rule with multiple conjuncts - (Jaccard, 3g) & (cosine, ws)
rule_4 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) > 0.4',
          'name_name_cos_dlm_dc0_dlm_dc0(ltuple, rtuple) > 0.25']
expected_labels_4 = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

# rule returning all 0 labels
rule_5 = ['name_name_jac_dlm_dc0_dlm_dc0(ltuple,rtuple) > 0.5']
expected_labels_all_zeroes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

# rule with single conjunct
rule_6 = ['name_name_mel(ltuple,rtuple) > 0.6']
expected_labels_6 = [0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1]

expected_labels_1_and_6 = [0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1]

# rule with multiple conjuncts
rule_7 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) > 0.3',
          'name_name_mel(ltuple,rtuple) > 0.6']
expected_labels_7 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

expected_labels_6_and_7 = [0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1]

class RuleBasedMatcherTestCases(unittest.TestCase):
    def setUp(self):
        self.A = em.read_csv_metadata(path_for_A)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_for_B)
        em.set_key(self.B, 'ID')
        self.C = em.read_csv_metadata(path_for_C, ltable=self.A, rtable=self.B)
        self.feature_table = em.get_features_for_matching(self.A, self.B, validate_inferred_attr_types=False)
        self.brm = em.BooleanRuleMatcher()

    def tearDown(self):
        del self.A
        del self.B
        del self.C
        del self.brm

    @raises(AssertionError)
    def test_rulebased_matcher_null_table(self):
        self.brm.predict(table=None)

    @raises(AssertionError)
    def test_rulebased_matcher_invalid_table_1(self):
        self.brm.predict(table={})

    @raises(AssertionError)
    def test_rulebased_matcher_invalid_table_2(self):
        self.brm.predict(table=[10, 10])

    @raises(KeyError)
    def test_rulebased_matcher_invalid_table_3(self):
        self.brm.predict(table=pd.DataFrame())

    @raises(AssertionError)
    def test_rulebased_matcher_invalid_target_attr(self):
        self.brm.predict(table=self.C, target_attr=pd.DataFrame())

    @raises(AssertionError)
    def test_rulebased_matcher_invalid_append(self):
        self.brm.predict(table=self.C, append=None)

    @raises(AssertionError)
    def test_rulebased_matcher_invalid_inplace(self):
        self.brm.predict(table=self.C, inplace=None)

    @raises(AssertionError)
    def test_rulebased_matcher_no_rules(self):
        self.brm.predict(self.C)

    @raises(AssertionError)
    def test_rulebased_matcher_no_feature_table(self):
        self.brm.add_rule(rule_1, feature_table=None)
        self.brm.predict(self.C)

    @raises(AssertionError)
    def test_rulebased_matcher_rule_with_bogus_feature(self):
        self.brm.add_rule(['bogus_feature(ltuple, rtuple) < 0.5'], self.feature_table)
        self.brm.predict(self.C)

    @raises(AssertionError)
    def test_rulebased_matcher_delete_nonexisting_rule(self):
        self.brm.delete_rule('bogus_rule')

    @raises(AssertionError)
    def test_rulebased_matcher_add_rule_twice(self):
        rule_name = self.brm.add_rule(rule_1, self.feature_table, 'myrule')
        assert_equal(rule_name, 'myrule')
        # see if rule exists in the set of rules
        rule_names = self.brm.get_rule_names()
        assert_equal(rule_name in rule_names, True)
        rule_name = self.brm.add_rule(rule_1, self.feature_table, 'myrule')

    def test_rulebased_matcher_filterable_rule_single_conjunct_1(self):
        self.brm.add_rule(rule_1, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_1, predictions)

    def test_rulebased_matcher_filterable_rule_single_conjunct_2(self):
        self.brm.add_rule(rule_2, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_2, predictions)

    def test_rulebased_matcher_filterable_rule_multiple_conjuncts_1(self):
        self.brm.add_rule(rule_3, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_3, predictions)

    def test_rulebased_matcher_filterable_rule_multiple_conjuncts_2(self):
        self.brm.add_rule(rule_4, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_4, predictions)

    def test_rulebased_matcher_non_filterable_rule_single_conjunct(self):
        self.brm.add_rule(rule_6, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_6, predictions)

    def test_rulebased_matcher_non_filterable_rule_multiple_conjuncts(self):
        self.brm.add_rule(rule_7, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_7, predictions)

    def test_rulebased_matcher_rule_sequence_with_two_filterable_rules(self):
        self.brm.add_rule(rule_1, self.feature_table)
        self.brm.add_rule(rule_2, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_1_and_2, predictions)

    def test_rulebased_matcher_rule_sequence_with_one_filterable_rule(self):
        self.brm.add_rule(rule_1, self.feature_table)
        self.brm.add_rule(rule_6, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_1_and_6, predictions)

    def test_rulebased_matcher_rule_sequence_with_no_filterable_rule(self):
        self.brm.add_rule(rule_6, self.feature_table)
        self.brm.add_rule(rule_7, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_6_and_7, predictions)

    def test_rulebased_matcher_rule_wi_no_output_tuples(self):
        self.brm.add_rule(rule_5, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_all_zeroes, predictions)

    def test_rulebased_matcher_rule_wi_no_auto_gen_feature(self):
        feature_string = "jaccard(qgm_3(ltuple['name']), qgm_3(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 0.4']  # same as rule_1
        self.brm.add_rule(test_rule, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_1, predictions)

    def test_rulebased_matcher_rule_wi_diff_tokenizers(self):
        feature_string = "jaccard(qgm_3(ltuple['address']), dlm_dc0(rtuple['address']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 1']  # should return an empty set
        self.brm.add_rule(test_rule, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_all_zeroes, predictions)

    def test_rulebased_matcher_rule_wi_dice_sim_fn(self):
        feature_string = "dice(dlm_dc0(ltuple['name']), dlm_dc0(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 1']  # should return an empty set
        self.brm.add_rule(test_rule, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_all_zeroes, predictions)

    def test_rulebased_matcher_rule_wi_overlap_coeff_sim_fn(self):
        feature_string = "overlap_coeff(dlm_dc0(ltuple['name']), dlm_dc0(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 1']  # should return an empty set
        self.brm.add_rule(test_rule, self.feature_table)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_all_zeroes, predictions)

    def test_rulebased_matcher_delete_rule(self):
        rule_name = self.brm.add_rule(rule_1, self.feature_table)
        rule_names = self.brm.get_rule_names()
        assert_equal(rule_name in rule_names, True)
        self.brm.delete_rule(rule_name)
        rule_names = self.brm.get_rule_names()
        assert_equal(rule_name in rule_names, False)

    def test_rulebased_matcher_add_rule_user_supplied_rule_name(self):
        rule_name = self.brm.add_rule(rule_1, self.feature_table, 'myrule')
        assert_equal(rule_name, 'myrule')
        # view rule source
        self.brm.view_rule(rule_name)
        # get rule fn
        self.brm.get_rule(rule_name)
        # see if rule exists in the set of rules
        rule_names = self.brm.get_rule_names()
        assert_equal(rule_name in rule_names, True)

    def test_rulebased_matcher_set_feature_table_then_add_rule(self):
        self.brm.set_feature_table(self.feature_table)
        self.brm.add_rule(rule_1)
        predictions = self.brm.predict(table=self.C)
        assert_equal(expected_labels_1, predictions)
