# coding=utf-8
import os

from nose.tools import *
import unittest

import pandas as pd

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.evaluation.evaluation import eval_matches
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.utils.generic_helper import get_install_path

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class EvaluationTestCases(unittest.TestCase):
    def test_eval_matches_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C1 = C[['_id', 'ltable_ID', 'rtable_ID']]
        num_ones = 1
        num_zeros = len(C1) - num_ones
        gold = [0] * num_ones
        gold.extend([1] * num_zeros)
        predicted = [1] * (num_zeros + num_ones)

        ln = len(C1.columns)
        C1.insert(ln, 'gold', gold)
        C1.insert(ln + 1, 'predicted', predicted)
        cm.copy_properties(C, C1)

        result = eval_matches(C1, 'gold', 'predicted')
        self.assertEqual(isinstance(result, dict), True)
        self.assertEqual(result['prec_numerator'], 14)
        self.assertEqual(result['prec_denominator'], 15)
        self.assertAlmostEqual(result['precision'], 0.9333333333333333)
        self.assertEqual(result['recall_numerator'], 14)
        self.assertEqual(result['recall_denominator'], 14)
        self.assertEqual(result['recall'], 1.0)
        self.assertEqual(result['f1'], 0.9655172413793104)
        self.assertEqual(result['pred_pos_num'], 15)
        self.assertEqual(result['false_pos_num'], 1.0)
        self.assertEqual(len(result['false_pos_ls']), 1)
        t = result['false_pos_ls'][0]
        self.assertEqual(t[0], 'a1')
        self.assertEqual(t[1], 'b1')
        self.assertEqual(result['pred_neg_num'], 0)
        self.assertEqual(result['false_neg_num'], 0.0)
        self.assertEqual(len(result['false_neg_ls']), 0)

    def test_eval_matches_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C1 = C[['_id', 'ltable_ID', 'rtable_ID']]
        num_ones = 1
        num_zeros = len(C1) - num_ones
        gold = [0] * num_ones
        gold.extend([1] * num_zeros)
        predicted = [1] * (num_zeros + num_ones)

        ln = len(C1.columns)
        C1.insert(ln, 'gold', gold)
        C1.insert(ln + 1, 'predicted', predicted)
        cm.copy_properties(C, C1)

        result = eval_matches(C1, 'predicted', 'gold')
        self.assertEqual(isinstance(result, dict), True)
        self.assertEqual(result['prec_numerator'], 14)
        self.assertEqual(result['prec_denominator'], 14)
        self.assertAlmostEqual(result['precision'], 1)
        self.assertEqual(result['recall_numerator'], 14)
        self.assertEqual(result['recall_denominator'], 15)
        self.assertEqual(result['recall'], 0.9333333333333333)
        self.assertEqual(result['f1'], 0.9655172413793104)
        self.assertEqual(result['pred_pos_num'], 14)
        self.assertEqual(result['false_pos_num'], 0.0)
        self.assertEqual(len(result['false_pos_ls']), 0)
        self.assertEqual(result['pred_neg_num'], 1)
        self.assertEqual(result['false_neg_num'], 1.0)
        self.assertEqual(len(result['false_neg_ls']), 1)
        t = result['false_neg_ls'][0]
        self.assertEqual(t[0], 'a1')
        self.assertEqual(t[1], 'b1')

    def test_eval_matches_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C1 = C[['_id', 'ltable_ID', 'rtable_ID']]
        num_ones = len(C1)
        num_zeros = len(C1) - num_ones
        gold = [0]*num_ones
        # gold.extend([1]*num_zeros)
        predicted = [1]* (num_zeros + num_ones)

        ln = len(C1.columns)
        C1.insert(ln, 'gold', gold)
        C1.insert(ln+1, 'predicted', predicted)
        D = pd.DataFrame(columns=C1.columns)
        cm.copy_properties(C, D)
        result = eval_matches(D, 'gold', 'predicted')

        self.assertEqual(isinstance(result, dict), True)
        self.assertEqual(result['prec_numerator'], 0)
        self.assertEqual(result['prec_denominator'], 0)
        self.assertAlmostEqual(result['precision'], 0)
        self.assertEqual(result['recall_numerator'], 0)
        self.assertEqual(result['recall_denominator'], 0)
        self.assertEqual(result['recall'], 0)
        self.assertEqual(result['f1'], 0)
        self.assertEqual(result['pred_pos_num'], 0)
        self.assertEqual(result['false_pos_num'], 0.0)
        self.assertEqual(len(result['false_pos_ls']), 0)
        self.assertEqual(result['pred_neg_num'], 0)
        self.assertEqual(result['false_neg_num'], 0.0)
        self.assertEqual(len(result['false_neg_ls']), 0)

    @raises(AssertionError)
    def test_eval_matches_invalid_df(self):
        eval_matches(None, "", "")

    @raises(AssertionError)
    def test_eval_matches_invalid_gold_attr(self):
        eval_matches(pd.DataFrame(), None, "")

    @raises(AssertionError)
    def test_eval_matches_invalid_predicted_attr(self):
        eval_matches(pd.DataFrame(), "", None)

    @raises(AssertionError)
    def test_eval_matches_gold_attr_not_in_df(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C1 = C[['_id', 'ltable_ID', 'rtable_ID']]
        num_ones = 1
        num_zeros = len(C1) - num_ones
        gold = [0] * num_ones
        gold.extend([1] * num_zeros)
        predicted = [1] * (num_zeros + num_ones)

        ln = len(C1.columns)
        C1.insert(ln, 'gold', gold)
        C1.insert(ln + 1, 'predicted', predicted)
        cm.copy_properties(C, C1)

        result = eval_matches(C1, 'gold1', 'predicted')

    @raises(AssertionError)
    def test_eval_matches_predicted_attr_not_in_df(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        C1 = C[['_id', 'ltable_ID', 'rtable_ID']]
        num_ones = 1
        num_zeros = len(C1) - num_ones
        gold = [0] * num_ones
        gold.extend([1] * num_zeros)
        predicted = [1] * (num_zeros + num_ones)

        ln = len(C1.columns)
        C1.insert(ln, 'gold', gold)
        C1.insert(ln + 1, 'predicted', predicted)
        cm.copy_properties(C, C1)

        result = eval_matches(C1, 'gold', 'predicted1')
