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
rule_2 = ['birth_year_birth_year_lev_dist(ltuple, rtuple) == 0']
expected_labels_2 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

expected_labels_1_and_2 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

# rule with multiple conjuncts - (Jaccard, 3g) & (edit dist)
rule_3 = ['name_name_jac_qgm_3_qgm_3(ltuple, rtuple) > 0.4',
          'birth_year_birth_year_lev_dist(ltuple, rtuple) == 0']
expected_labels_3 = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

expected_labels_2_and_3 = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]

# rule with multiple conjuncts - (Jaccard, 3g) & (cosine, ws)
rule_4 = ['name_name_jac_qgm_3_qgm_3(ltuple,rtuple) > 0.4',
          'name_name_cos_dlm_dc0_dlm_dc0(ltuple, rtuple) > 0.25']
expected_labels_4 = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

expected_labels_zeros = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


class RuleBasedMatcherTestCases(unittest.TestCase):
    def setUp(self):
        self.A = em.read_csv_metadata(path_for_A)
        em.set_key(self.A, 'ID')
        self.B = em.read_csv_metadata(path_for_B)
        em.set_key(self.B, 'ID')
        self.C = em.read_csv_metadata(path_for_C, ltable=self.A, rtable=self.B)
        self.feature_table = em.get_features_for_matching(self.A, self.B, validate_inferred_attr_types=False)
        self.C['neg_trig_labels'] = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        self.C['pos_trig_labels'] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.mt = em.MatchTrigger()

    def tearDown(self):
        del self.A
        del self.B
        del self.C
        del self.mt

    @raises(AssertionError)
    def test_trigger_null_table(self):
        self.mt.execute(None, 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_trigger_invalid_table_1(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.execute([10, 10], 'neg_trig_labels', inplace=False)

    @raises(KeyError)
    def test_trigger_invalid_table_2(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.execute(pd.DataFrame(), 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_trigger_invalid_label(self):
        self.mt.execute(self.C, None, inplace=False)

    @raises(AssertionError)
    def test_trigger_invalid_label(self):
        self.mt.execute(self.C, pd.DataFrame(), inplace=False)

    @raises(AssertionError)
    def test_trigger_invalid_inplace(self):
        self.mt.execute(self.C, 'neg_trig_labels', inplace=None)

    @raises(AssertionError)
    def test_trigger_invalid_verbose(self):
        self.mt.execute(self.C, 'neg_trig_labels', inplace=False, verbose=None)

    @raises(AssertionError)
    def test_trigger_no_rules(self):
        self.mt.execute(self.C, 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_trigger_no_rules(self):
        self.mt.execute(self.C, 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_trigger_invalid_status(self):
        self.mt.add_cond_status(None)

    @raises(AssertionError)
    def test_trigger_invalid_action(self):
        self.mt.add_action(None)

    @raises(AssertionError)
    def test_trigger_no_feature_table(self):
        self.mt.add_cond_rule(rule_1, feature_table=None)
        self.mt.execute(self.C, 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_trigger_rule_with_bogus_feature(self):
        self.mt.add_cond_rule(['bogus_feature(ltuple, rtuple) < 0.5'], self.feature_table)
        self.mt.execute(self.C, 'neg_trig_labels', inplace=False)

    @raises(AssertionError)
    def test_rulebased_matcher_delete_nonexisting_rule(self):
        self.mt.delete_rule('bogus_rule')

    @raises(AssertionError)
    def test_rulebased_matcher_get_nonexisting_rule(self):
        self.mt.get_rule('bogus_rule')

    @raises(AssertionError)
    def test_rulebased_matcher_view_nonexisting_rule(self):
        self.mt.view_rule('bogus_rule')

    @raises(AssertionError)
    def test_trigger_add_rule_twice(self):
        rule_name = self.mt.add_cond_rule(rule_1, self.feature_table, 'myrule')
        assert_equal(rule_name, 'myrule')
        # see if rule exists in the set of rules
        rule_names = self.mt.get_rule_names()
        assert_equal(rule_name in rule_names, True)
        rule_name = self.mt.add_cond_rule(rule_1, self.feature_table, 'myrule')

    def test_neg_trigger_single_conjunct_1(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_1, predictions)

    def test_neg_trigger_single_conjunct_2(self):
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_2, predictions)

    def test_neg_trigger_multiple_conjuncts_1(self):
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_3, predictions)

    def test_neg_trigger_multiple_conjuncts_2(self):
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_4, predictions)

    def test_neg_trigger_rule_sequence_single_conjuncts(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_1_and_2, predictions)

    def test_neg_trigger_rule_sequence_single_multiple_conjunct(self):
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_2_and_3, predictions)

    def test_pos_trigger_single_conjunct_1(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_1, predictions)

    def test_pos_trigger_single_conjunct_2(self):
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_2, predictions)

    def test_pos_trigger_multiple_conjuncts_1(self):
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_3, predictions)

    def test_pos_trigger_multiple_conjuncts_2(self):
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_4, predictions)

    def test_pos_trigger_rule_sequence_single_conjuncts(self):
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_1_and_2, predictions)

    def test_pos_trigger_rule_sequence_single_multiple_conjunct(self):
        self.mt.add_cond_rule(rule_2, self.feature_table)
        self.mt.add_cond_rule(rule_3, self.feature_table)
        self.mt.add_cond_status(True)
        self.mt.add_action(1)
        preds = self.mt.execute(self.C, 'pos_trig_labels', inplace=False)
        predictions = preds['pos_trig_labels'].tolist()
        assert_equal(expected_labels_2_and_3, predictions)

    def test_trigger_rule_wi_no_auto_gen_feature(self):
        feature_string = "jaccard(qgm_3(ltuple['name']), qgm_3(rtuple['name']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 0.4']  # same as rule_1

        self.mt.add_cond_rule(test_rule, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_1, predictions)

    def test_trigger_rule_wi_diff_tokenizers(self):
        feature_string = "jaccard(qgm_3(ltuple['address']), dlm_dc0(rtuple['address']))"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_blocking(),
                                get_sim_funs_for_blocking())
        f_dict['is_auto_generated'] = True
        add_feature(self.feature_table, 'test', f_dict)
        test_rule = ['test(ltuple, rtuple) > 1']  # should return an empty set

        self.mt.add_cond_rule(test_rule, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_zeros, predictions)

    def test_rulebased_matcher_delete_rule(self):
        rule_name = self.mt.add_cond_rule(rule_1, self.feature_table)
        rule_names = self.mt.get_rule_names()
        assert_equal(rule_name in rule_names, True)
        self.mt.delete_rule(rule_name)
        rule_names = self.mt.get_rule_names()
        assert_equal(rule_name in rule_names, False)

    def test_trigger_add_rule_user_supplied_rule_name(self):
        rule_name = self.mt.add_cond_rule(rule_1, self.feature_table, 'myrule')
        assert_equal(rule_name, 'myrule')
        # view rule source
        self.mt.view_rule(rule_name)
        # get rule fn
        self.mt.get_rule(rule_name)
        # see if rule exists in the set of rules
        rule_names = self.mt.get_rule_names()
        assert_equal(rule_name in rule_names, True)

    def test_rulebased_matcher_set_feature_table_then_add_rule(self):
        self.mt.set_feature_table(self.feature_table)
        self.mt.add_cond_rule(rule_1, self.feature_table)
        self.mt.add_cond_status(False)
        self.mt.add_action(0)
        preds = self.mt.execute(self.C, 'neg_trig_labels', inplace=False)
        predictions = preds['neg_trig_labels'].tolist()
        assert_equal(expected_labels_1, predictions)
