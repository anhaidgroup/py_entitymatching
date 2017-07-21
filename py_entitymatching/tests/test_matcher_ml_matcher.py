# coding=utf-8
import os
import unittest
from nose.tools import *

import six

from py_entitymatching.matcher.dtmatcher import DTMatcher
from py_entitymatching.matcher.linregmatcher import LinRegMatcher
from py_entitymatching.matcher.logregmatcher import LogRegMatcher
from py_entitymatching.matcher.nbmatcher import NBMatcher
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.matcher.svmmatcher import SVMMatcher

from py_entitymatching.io.parsers import read_csv_metadata
import py_entitymatching.matcher.matcherutils as mu
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.generic_helper import get_install_path, list_diff

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

feat_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets',
                                  'matcherselector'])
fpath_a = os.sep.join([feat_datasets_path, 'DBLP_demo.csv'])
fpath_b = os.sep.join([feat_datasets_path, 'ACM_demo.csv'])
fpath_c = os.sep.join([feat_datasets_path, 'dblp_acm_demo_labels.csv'])
fpath_f = os.sep.join([feat_datasets_path, 'feat_vecs.csv'])


class MLMatcherTestCases(unittest.TestCase):
    def test_valid_names_for_matchers(self):
        matchers1 = {"DT": DTMatcher(), "LinReg": LinRegMatcher(), "LogReg": LogRegMatcher(),
                    "NB": NBMatcher(), "RF": RFMatcher(), "SVM": SVMMatcher()}

        matchers2 = {"DT": DTMatcher(name='temp'), "LinReg": LinRegMatcher(name='temp'),
                     "LogReg": LogRegMatcher(name='temp'),
                    "NB": NBMatcher(name='temp'), "RF": RFMatcher(name='temp'), "SVM": SVMMatcher(name='temp')}

        for m_name, matcher in six.iteritems(matchers1):
                self.assertEqual(isinstance(matcher.name, six.string_types), True)

        for m_name, matcher in six.iteritems(matchers2):
                self.assertEqual(matcher.name, 'temp')


    def test_train_test_split_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        result = mu.split_train_test(C)
        train = result['train']
        test = result['test']
        self.assertEqual(len(train)+len(test), len(C))
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(train)
        p3 = cm.get_all_properties(test)
        # d = {}
        # d['ltable'] = A
        # d['rtable'] = A
        # d['key'] = '_id'
        # d['fk_ltable'] = 'ltable_ID'
        # d['fk_rtable'] = 'rtable_ID'
        self.assertEqual(p1 == p2, True)
        self.assertEqual(p1 == p3, True)
        # self.assertEqual(p1 == d, True)

    @raises(AssertionError)
    def test_train_test_split_invalid_df(self):
        mu.split_train_test(None)

    def test_ml_matcher_valid_1(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        dt.fit(table=train, exclude_attrs=['ltable.id', 'rtable.id', '_id'], target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                 target_attr='predicted',
                                 append=True)

        self.assertEqual(len(predictions), len(test))
        self.assertEqual(set(list(predictions.columns)).issubset(list(test.columns)), True)
        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    def test_ml_matcher_valid_2(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = train[l]
        Y = train['gold']

        dt.fit(x=X, y=Y)
        predictions = dt.predict(test[l])
        self.assertEqual(len(predictions), len(test))

    # return probs
    def test_ml_matcher_valid_3(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_key(feature_vectors), cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = train[l]
        Y = train['gold']

        dt.fit(x=X, y=Y)
        predictions, probs = dt.predict(test[l], return_probs=True)
        self.assertEqual(len(predictions), len(test))
        self.assertEqual(len(probs), len(test))


    @raises(AssertionError)
    def test_ml_matcher_invalid_df(self):
        dt = DTMatcher(name='DecisionTree')
        dt.fit(table="", exclude_attrs=['ltable.id', 'rtable.id', '_id'], target_attr='gold')

    def test_ml_matcher_ex_attrs_not_list(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs=['_id', 'gold'],
                                 target_attr='predicted',
                                 append=True)

        self.assertEqual(len(predictions), len(test))
        l = len(set(list(predictions.columns)).difference(list(test.columns)))
        self.assertEqual(l, 0)
        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    @raises(AssertionError)
    def test_ml_matcher_ex_attrs_not_present_fit(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id1', target_attr='gold')


    @raises(AssertionError)
    def test_ml_matcher_target_attr_not_present_fit(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold1')

    def test_ml_matcher_target_attr_present_in_ex_attrs(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        dt.fit(table=train, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'], target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                 target_attr='predicted',
                                 append=True)

        self.assertEqual(len(predictions), len(test))
        l = len(set(list(predictions.columns)).difference(list(test.columns)))
        self.assertEqual(l, 0)
        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    @raises(SyntaxError)
    def test_ml_matcher_invalid_input_combn_fit(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        dt.fit(x=train, table=train)


    @raises(AssertionError)
    def test_ml_matcher_invalid_df_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        dt.fit(table=train, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'], target_attr='gold')
        _ = dt.predict(table="", exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                                 target_attr='predicted',
                                 append=True)

    def test_ml_matcher_ex_attrs_not_list_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id',
                                 target_attr='predicted',
                                 append=True)

        self.assertEqual(len(predictions), len(test))
        l = len(set(list(predictions.columns)).difference(list(test.columns)))
        self.assertEqual(l, 0)
        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    @raises(AssertionError)
    def test_ml_matcher_ex_attrs_not_in_df_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id1',
                                 target_attr='predicted',
                                 append=True)

        self.assertEqual(len(predictions), len(test))
        l = len(set(list(predictions.columns)).difference(list(test.columns)))
        self.assertEqual(l, 0)

        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    @raises(SyntaxError)
    def test_ml_invalid_predict_sign(self):
        dt = DTMatcher(name='DecisionTree')
        dt.predict()

    def test_ml_matcher_append_false_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id',
                                 target_attr='predicted',
                                 append=False)

        self.assertEqual(len(predictions), len(test))
        # self.assertEqual(set(list(predictions.columns)).issubset(list(test.columns)), True)
        # p_col = predictions.columns[len(predictions.columns)-1]
        # self.assertEqual(p_col, 'predicted')

    def test_ml_matcher_inplace_false_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id',
                                 target_attr='predicted',
                                 inplace=False, append=True)

        self.assertNotEqual(id(predictions), id(test))
        self.assertEqual(len(predictions), len(test))
        self.assertEqual(set(list(test.columns)).issubset(list(predictions.columns)), True)
        p_col = predictions.columns[len(predictions.columns)-1]
        self.assertEqual(p_col, 'predicted')

    def test_ml_matcher_return_probs_true_predict(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id',
                                 target_attr='predicted', probs_attr='proba',
                                 inplace=False, append=True, return_probs=True)

        self.assertNotEqual(id(predictions), id(test))
        self.assertEqual(len(predictions), len(test))
        self.assertEqual(set(list(test.columns)).issubset(list(predictions.columns)), True)

        p_col = predictions.columns[len(predictions.columns)-2]
        self.assertEqual(p_col, 'predicted')

        r_col = predictions.columns[len(predictions.columns) - 1]
        self.assertEqual(r_col, 'proba')

        self.assertEqual(sum((predictions[r_col] >= 0.0) & (predictions[r_col] <= 1.0)),
                         len(predictions))

    def test_ml_matcher_return_probs_true_predict_diff_colname(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')
        train.drop('ltable.id', axis=1, inplace=True)
        train.drop('rtable.id', axis=1, inplace=True)
        test.drop('ltable.id', axis=1, inplace=True)
        test.drop('rtable.id', axis=1, inplace=True)
        test.drop('gold', axis=1, inplace=True)
        dt.fit(table=train, exclude_attrs='_id', target_attr='gold')
        predictions = dt.predict(table=test, exclude_attrs='_id',
                                 target_attr='predicted', probs_attr='probas',
                                 inplace=False, append=True, return_probs=True)

        self.assertNotEqual(id(predictions), id(test))
        self.assertEqual(len(predictions), len(test))
        self.assertEqual(set(list(test.columns)).issubset(list(predictions.columns)), True)

        p_col = predictions.columns[len(predictions.columns)-2]
        self.assertEqual(p_col, 'predicted')

        r_col = predictions.columns[len(predictions.columns) - 1]
        self.assertEqual(r_col, 'probas')

        self.assertEqual(sum((predictions[r_col] >= 0.0) & (predictions[r_col] <= 1.0)),
                         len(predictions))

    def test_ml_matcher_set_name(self):
        dt = DTMatcher()
        dt.set_name('Decision Tree')
        self.assertEqual(dt.get_name(), 'Decision Tree')

    @raises(AssertionError)
    def test_ml_matcher_invalid_df_1(self):
        dt = DTMatcher(name='DecisionTree')
        dt.fit(x="", y="")

    def test_ml_matcher_valid_with_id_in_x(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = train[l]
        Y = train['gold']

        dt.fit(x=X, y=Y)
        predictions = dt.predict(test[l])
        self.assertEqual(len(predictions), len(test))

    def test_ml_matcher_valid_with_id_in_y(self):
        A = read_csv_metadata(fpath_a, key='id')
        B = read_csv_metadata(fpath_b, key='id')
        feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
        train_test = mu.split_train_test(feature_vectors)
        train, test = train_test['train'], train_test['test']
        dt = DTMatcher(name='DecisionTree')

        col_list = list(feature_vectors.columns)
        l = list_diff(col_list, [cm.get_fk_ltable(feature_vectors),
                                 cm.get_fk_rtable(feature_vectors),
                                 'gold'])
        X = train[l]
        Y = train[['_id', 'gold']]

        dt.fit(x=X, y=Y)
        predictions = dt.predict(test[l])
        self.assertEqual(len(predictions), len(test))
