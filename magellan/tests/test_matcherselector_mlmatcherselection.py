import os
from nose.tools import *
import unittest
import pandas as pd
import six

from magellan.utils.generic_helper import get_install_path, list_diff
from magellan.io.parsers import read_csv_metadata
from magellan.feature.autofeaturegen import get_features_for_matching
from magellan.feature.extractfeatures import extract_feature_vecs
from magellan.matcherselector.mlmatcherselection import select_matcher
from magellan.matcher.dtmatcher import DTMatcher
from magellan.matcher.linregmatcher import LinRegMatcher
from magellan.matcher.logregmatcher import LogRegMatcher
from magellan.matcher.nbmatcher import NBMatcher
from magellan.matcher.rfmatcher import RFMatcher
from magellan.matcher.svmmatcher import SVMMatcher

import magellan.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class MLMatcherSelectionTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    # @nottest
    def test_select_matcher_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
        dtmatcher = DTMatcher()
        nbmatcher = NBMatcher()
        rfmatcher = RFMatcher()
        svmmatcher = SVMMatcher()
        linregmatcher = LinRegMatcher()
        logregmatcher = LogRegMatcher()
        matchers = [dtmatcher, nbmatcher, rfmatcher, svmmatcher, linregmatcher, logregmatcher]

        result = select_matcher(matchers, x=None, y=None, table=feature_vectors,
                                exclude_attrs=['ltable_ID', 'rtable_ID', '_id', 'labels'],
                                target_attr='labels', k=2)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors['labels']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors['labels']
        result = select_matcher(matchers, x=X, y=Y, metric='recall')
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_4(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors['labels']
        result = select_matcher(matchers, x=X, y=Y, metric='f1')
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    # @nottest
    def test_select_matcher_valid_5(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors['labels']
        result = select_matcher(matchers, x=X, y=Y, metric='f1', k=4)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)


    def test_select_matcher_valid_6(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors['labels']
        result = select_matcher(matchers, x=X, y=Y)
        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)

    def test_select_matcher_valid_7(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                target_attr='labels', k=2)

        header = ['Name', 'Matcher', 'Num folds']
        result_df = result['cv_stats']
        self.assertEqual(set(header) == set(list(result_df.columns[[0, 1, 2]])), True)
        self.assertEqual('Mean score', result_df.columns[len(result_df.columns) - 1])
        d = result_df.set_index('Name')
        p_max = d.ix[result['selected_matcher'].name, 'Mean score']
        a_max = pd.np.max(d['Mean score'])
        self.assertEqual(p_max, a_max)


    @raises(AssertionError)
    def test_select_matcher_invalid_df(self):
        select_matcher(matchers=[], table="", exclude_attrs=[], target_attr="")

    @raises(SyntaxError)
    def test_select_matcher_invalid_args(self):
        select_matcher(matchers=[], table="", exclude_attrs=[])

    @raises(AssertionError)
    def test_select_matcher_target_attr_not_series(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                 'labels'])
        X = feature_vectors[l]
        Y = feature_vectors[['labels']]
        result = select_matcher(matchers, x=X, y=Y)

    @raises(AssertionError)
    def test_select_matcher_ex_attrs_not_present(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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
                                target_attr='labels', k=2)

    @raises(AssertionError)
    def test_select_matcher_target_attr_not_present(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        labels = [0] * 7
        labels.extend([1] * 8)
        C['labels'] = labels
        feature_table = get_features_for_matching(A, B)
        feature_vectors = extract_feature_vecs(C, feature_table=feature_table, attrs_after='labels')
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