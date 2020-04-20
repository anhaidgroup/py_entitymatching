# coding=utf-8
import os
import unittest
from nose.tools import *

import pandas as pd

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.matcher.matcherutils as mu
from py_entitymatching.debugmatcher.debug_gui_randomforest_matcher import _vis_debug_rf, \
    vis_tuple_debug_rf_matcher
from py_entitymatching.debugmatcher.debug_randomforest_matcher import debug_randomforest_matcher

from py_entitymatching.feature.autofeaturegen import get_features_for_matching
from py_entitymatching.feature.extractfeatures import extract_feature_vecs
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.utils.generic_helper import get_install_path

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class VisRFDebugMatcherTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_vis_debug_matcher_rf_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']

        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
                      target_attr='labels', show_window=False)

    def test_vis_tuple_debug_rf_matcher_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')
        s = pd.DataFrame(feature_vectors.loc[0])
        s1 = s.T
        vis_tuple_debug_rf_matcher(rf.clf, s1,
                                   exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'])

    def test_vis_tuple_debug_rf_matcher_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')
        s = pd.DataFrame(feature_vectors.loc[0])
        s1 = s.T
        vis_tuple_debug_rf_matcher(rf, s1,
                                   exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'])

    def test_vis_tuple_debug_rf_matcher_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')
        feature_vectors.drop(['_id', 'ltable_ID', 'rtable_ID', 'labels'], axis=1, inplace=True)
        s = pd.DataFrame(feature_vectors.loc[0])
        s1 = s.T

        vis_tuple_debug_rf_matcher(rf.clf, s1, exclude_attrs=None)

    @raises(AssertionError)
    def test_vis_debug_matcher_rf_invalid_df(self):
        _vis_debug_rf(None, pd.DataFrame(), pd.DataFrame(),
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
                      target_attr='labels', show_window=False)

    @raises(AssertionError)
    def test_vis_debug_matcher_rf_invalid_tar_attr(self):
        _vis_debug_rf(RFMatcher(), pd.DataFrame(), pd.DataFrame(),
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
                      target_attr=None, show_window=False)

    @raises(AssertionError)
    def test_vis_debug_matcher_rf_ex_attrs_notin_train(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']
        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID1', 'rtable_ID', 'labels'],
                      target_attr='labels', show_window=False)

    @raises(AssertionError)
    def test_vis_debug_matcher_rf_tar_attr_notin_train(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']
        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
                      target_attr='labels1', show_window=False)

    @raises(AssertionError)
    def test_vis_debug_matcher_rf_ex_attrs_notin_test(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']
        test.drop('_id', inplace=True, axis=1)
        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
                      target_attr='labels', show_window=False)

#
    def test_vis_debug_matcher_rf_tar_attrs_notin_exattrs(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']
        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                      target_attr='labels', show_window=False)


    def test_vis_debug_matcher_rf_label_col_wi_sp_name(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['_predicted'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='_predicted')

        rf = RFMatcher()
        train_test = mu.split_train_test(feature_vectors)

        train = train_test['train']
        test = train_test['test']
        _vis_debug_rf(rf, train, test,
                      exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                      target_attr='_predicted', show_window=False)

class RFDebugMatcherTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()


    # def test_debug_rf_matcher_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     B = read_csv_metadata(path_b, key='ID')
    #     C = read_csv_metadata(path_c, ltable=A, rtable=B)
    #     labels = [0] * 7
    #     labels.extend([1] * 8)
    #     C['labels'] = labels
    #
    #     feature_table = get_features_for_matching(A, B)
    #     feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
    #                                            attrs_after='labels')
    #     rf = RFMatcher()
    #     rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
    #            target_attr='labels')
    #     debug_randomforest_matcher(rf, A.loc[1], B.loc[2], feat_table=feature_table,
    #                           fv_columns=feature_vectors.columns,
    #                           exclude_attrs=['ltable_ID', 'rtable_ID', '_id', 'labels'])

    @raises(AssertionError)
    def test_debug_rf_matcher_invalid_feat_table(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')
        rf = RFMatcher()
        rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')

        debug_randomforest_matcher(rf, A.loc[1], B.loc[2], feature_table=None,
                                   table_columns=feature_vectors.columns,
                                   exclude_attrs=['ltable_ID', 'rtable_ID', '_id', 'labels'])


    def test_debug_rf_matcher_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels

        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table,
                                               attrs_after='labels')
        rf = RFMatcher()
        rf.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')

        debug_randomforest_matcher(rf.clf, A.loc[1], B.loc[2], feature_table=feature_table,
                                   table_columns=feature_vectors.columns,
                                   exclude_attrs=['ltable_ID', 'rtable_ID', '_id', 'labels'])
