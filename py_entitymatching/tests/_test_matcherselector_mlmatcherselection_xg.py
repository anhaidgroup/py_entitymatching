import os
from nose.tools import *
import unittest
import pandas as pd
import numpy as np
import six

from py_entitymatching.utils.generic_helper import get_install_path, list_diff
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.matcherselector.mlmatcherselection import select_matcher
from py_entitymatching.matcher.dtmatcher import DTMatcher
from py_entitymatching.matcher.linregmatcher import LinRegMatcher
from py_entitymatching.matcher.logregmatcher import LogRegMatcher
from py_entitymatching.matcher.nbmatcher import NBMatcher
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.matcher.svmmatcher import SVMMatcher
from py_entitymatching.matcher.xgboostmatcher import XGBoostMatcher

import py_entitymatching.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets',
                             'matcherselector'])
path_a = os.sep.join([datasets_path, 'DBLP_demo.csv'])
path_b = os.sep.join([datasets_path, 'ACM_demo.csv'])
path_c = os.sep.join([datasets_path, 'dblp_acm_demo_labels.csv'])
path_f = os.sep.join([datasets_path, 'feat_vecs.csv'])


class MLMatcherSelectionTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    # @nottest
    def test_select_matcher_valid_1(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # C['labels'] = labels
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                target_attr='gold', k=7)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_2(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_3(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric='recall')
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_4(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric='f1')
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_5(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric='f1', k=4)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)


    def test_select_matcher_valid_6(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    def test_select_matcher_valid_7(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors)
                                 ])
        feature_vectors = feature_vectors[l]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs='_id',
                                target_attr='gold', k=2)

        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)


    @raises(AssertionError)
    def test_select_matcher_invalid_df(self):
        select_matcher(matchers=[], table="", exclude_attrs=[], target_attr="")

    @raises(SyntaxError)
    def test_select_matcher_invalid_args(self):
        select_matcher(matchers=[], table="", exclude_attrs=[])

    @raises(AssertionError)
    def test_select_matcher_target_attr_not_series(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors[['gold']]
        result = select_matcher(matchers, x=X, y=Y)

    @raises(AssertionError)
    def test_select_matcher_ex_attrs_not_present(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors)
                                 ])
        feature_vectors = feature_vectors[l]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs='_id1',
                                target_attr='gold', k=2)

    @raises(AssertionError)
    def test_select_matcher_target_attr_not_present(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        # C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable.id',
        #                       fk_rtable='rtable.id', key='_id')
        # labels = [0] * 7
        # labels.extend([1] * 8)
        # C['labels'] = labels
        # feature_table = get_features_for_matching(A, B)
        # feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold')
        # feature_vectors.fillna(0, inplace=True)
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher, xgmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors)
                                 ])
        feature_vectors = feature_vectors[l]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs='_id',
                                target_attr='labels1', k=2)
