import os
# from nose.tools import *
import unittest
import pandas as pd
import numpy as np
import six
from .utils import raises

from py_entitymatching.utils.generic_helper import get_install_path, list_diff
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.matcherselector.mlmatcherselection import select_matcher
from py_entitymatching.matcher.dtmatcher import DTMatcher
from py_entitymatching.matcher.linregmatcher import LinRegMatcher
from py_entitymatching.matcher.logregmatcher import LogRegMatcher
from py_entitymatching.matcher.nbmatcher import NBMatcher
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.matcher.svmmatcher import SVMMatcher


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

    # @unittest.skip("Not a test")
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
        # xgmatcher = XGBoostMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher,
                    logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                target_attr='gold', k=7)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['precision']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @unittest.skip("Not a test")
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['precision']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @unittest.skip("Not a test")
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric_to_select_matcher='recall', metrics_to_display=['recall'])
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['recall']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @unittest.skip("Not a test")
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric_to_select_matcher='f1', metrics_to_display=['f1'])
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['f1']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @unittest.skip("Not a test")
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y, metric_to_select_matcher='f1', metrics_to_display=['f1'], k=4)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['f1']
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = feature_vectors[l]
        Y = feature_vectors['gold']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['precision']
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors)
                                 ])
        feature_vectors = feature_vectors[l]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs='_id',
                                target_attr='gold', k=2)

        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['drill_down_cv_stats']['precision']
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]
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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

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
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors)
                                 ])
        feature_vectors = feature_vectors[l]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs='_id',
                                target_attr='labels1', k=2)

    def test_select_matcher_valid_multiple_metrics(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                target_attr='gold', k=7)
        header = ['Name', 'Matcher', 'Num folds']
        result_df_p = result['drill_down_cv_stats']['precision']
        result_df_f = result['drill_down_cv_stats']['f1']
        result_df_r = result['drill_down_cv_stats']['recall']
        # Check header of precision dataframe
        self.assertEqual(set(header) == set(list(result_df_p.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df_p.columns[len(result_df_p.columns) - 1])
        # Check header of f1 dataframe
        self.assertEqual(set(header) == set(list(result_df_f.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df_f.columns[len(result_df_f.columns) - 1])
        # Check header of recall dataframe
        self.assertEqual(set(header) == set(list(result_df_r.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df_p.columns[len(result_df_r.columns) - 1])
        d = result_df_p.set_index('Name')
        p_max = d.loc[result['selected_matcher'].name, 'Mean score']
        a_max = np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    def test_select_matcher_valid_cv_stats(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                target_attr='gold', k=7)
        header = ['Matcher', 'Average precision', 'Average recall', 'Average f1']
        result_df = result['cv_stats']
        result_df_p = result['drill_down_cv_stats']['precision']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2, 3]])), True)
        d = result_df.set_index('Matcher')
        p_max = d.loc[result['selected_matcher'].name, 'Average precision']
        a_max = np.max(result_df_p['Mean score'])
        self.assertEqual(p_max, a_max)

    def test_select_matcher_valid_cv_stats_2(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metric_to_select_matcher='recall',
                                metrics_to_display=['recall', 'f1'],
                                target_attr='gold', k=7)
        header = ['Matcher', 'Average recall', 'Average f1']
        result_df = result['cv_stats']
        result_df_r = result['drill_down_cv_stats']['recall']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        d = result_df.set_index('Matcher')
        p_max = d.loc[result['selected_matcher'].name, 'Average recall']
        a_max = np.max(result_df_r['Mean score'])
        self.assertEqual(p_max, a_max)

    def test_select_matcher_valid_cv_stats_3(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metric_to_select_matcher='recall',
                                metrics_to_display='recall',
                                target_attr='gold', k=7)
        header = ['Matcher', 'Average recall']
        result_df = result['cv_stats']
        result_df_r = result['drill_down_cv_stats']['recall']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1]])), True)
        d = result_df.set_index('Matcher')
        p_max = d.loc[result['selected_matcher'].name, 'Average recall']
        a_max = np.max(result_df_r['Mean score'])
        self.assertEqual(p_max, a_max)

    @raises(KeyError)
    def test_select_matcher_invalid_no_display_drill_down(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metrics_to_display=['precision'],
                                target_attr='gold', k=7)
        result_df_p = result['drill_down_cv_stats']['recall']

    @raises(AssertionError)
    def test_select_matcher_invalid_metrics_to_display_1(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metrics_to_display=None,
                                target_attr='gold', k=7)

    @raises(AssertionError)
    def test_select_matcher_invalid_metrics_to_display_2(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metrics_to_display=['test'],
                                target_attr='gold', k=7)

    @raises(AssertionError)
    def test_select_matcher_invalid_metric_to_select_matcher(self):
        A = read_csv_metadata(path_a, key='id')
        B = read_csv_metadata(path_b, key='id')
        feature_vectors = read_csv_metadata(path_f, ltable=A, rtable=B)
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                metric_to_select_matcher='test',
                                target_attr='gold', k=7)
