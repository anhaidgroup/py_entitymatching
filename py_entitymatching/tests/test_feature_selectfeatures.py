import os
from nose.tools import *
import unittest
import pandas as pd

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching import impute_table
from py_entitymatching.feature.extractfeatures import extract_feature_vecs
from py_entitymatching.feature.autofeaturegen import get_features_for_matching
from py_entitymatching.feature.scalers import scale_features
from py_entitymatching.feature.selectfeatures import select_features_univariate

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'matcherselector'])
path_a = os.sep.join([datasets_path, 'DBLP_demo.csv'])
path_b = os.sep.join([datasets_path, 'ACM_demo.csv'])
path_c = os.sep.join([datasets_path, 'feat_vecs.csv'])
A = read_csv_metadata(path_a, key='id')
B = read_csv_metadata(path_b, key='id')
C = read_csv_metadata(path_c, ltable=A, rtable=B)
feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
F = extract_feature_vecs(C,
                         attrs_before=['_id', 'ltable.id', 'rtable.id'],
                         attrs_after=['gold'],
                         feature_table=feature_table)

H = impute_table(F,
                 exclude_attrs=['_id', 'ltable.id', 'rtable.id', 'gold'],
                 strategy='mean')

x, scaler = scale_features(H,
                           exclude_attrs=['_id', 'ltable.id', 'rtable.id'],
                           scaling_method='MinMax')


class SelectFeaturesTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_select_features_univariate_invalid_input(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x.values)

    @raises(AssertionError)
    def test_select_features_univariate_invalid_input2(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x,
                                   score=1)

    @raises(AssertionError)
    def test_select_features_univariate_invalid_input3(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x,
                                   parameter='weird_parameter')

    @raises(AssertionError)
    def test_select_features_univariate_invalid_input4(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x,
                                   target_attr=None, exclude_attrs=None,
                                   score='weird_score', mode='k_best', parameter=2)

    @raises(AssertionError)
    def test_select_features_univariate_invalid_input5(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x,
                                   target_attr=None, exclude_attrs=None,
                                   score='f_score', mode='weird_mode', parameter=2)

    @raises(AssertionError)
    def test_select_features_univariate_invalid_input6(self):
        select_features_univariate(feature_table=feature_table,
                                   table=x,
                                   target_attr=None, exclude_attrs=None,
                                   score='f_score', mode='k_best', parameter=2)

    def test_select_features_univariate_valid_input(self):
        feature_table_selected = select_features_univariate(
            feature_table=feature_table, table=x,
            target_attr='gold', exclude_attrs=['_id', 'ltable.id', 'rtable.id'],
            score='f_score', mode='k_best', parameter=2)
        self.assertEqual(isinstance(feature_table_selected, pd.DataFrame), True)
