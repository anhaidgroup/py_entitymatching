import os
from nose.tools import *
import unittest
import pandas as pd
import six

from magellan.utils.generic_helper import get_install_path
from magellan.io.parsers import read_csv_metadata
from magellan.feature.simfunctions import get_sim_funs_for_matching
from magellan.feature.tokenizers import get_tokenizers_for_matching
from magellan.feature.autofeaturegen import get_features_for_matching
from magellan.feature.addfeatures import add_feature, add_blackbox_feature, get_feature_fn, _parse_feat_str, create_feature_table


import magellan.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])


class AddFeaturesTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_add_features_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_matching(A, B)
        len1 = len(feature_table)
        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        add_feature(feature_table, 'test', f_dict)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.ix[len(feature_table)-1, 'function'](A.ix[1], B.ix[2]), 1.0)


    def test_feature_fn_valid_nosim_tok(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_matching(A, B)
        len1 = len(feature_table)
        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, dict(), dict())


    @raises(AssertionError)
    def test_get_feature_fn_invalid_feat_str(self):
        get_feature_fn(None, dict(), dict())

    @raises(AssertionError)
    def test_get_feature_fn_invalid_sim(self):
        get_feature_fn("", None, dict())

    @raises(AssertionError)
    def test_get_feature_fn_invalid_tok(self):
        get_feature_fn("", dict(), None)


    @raises(AssertionError)
    def test_parse_feat_str_invalid(self):
        _parse_feat_str(None, dict(), dict())

    @raises(AssertionError)
    def test_parse_feat_str_invalid_sim(self):
        _parse_feat_str("", None, dict())

    @raises(AssertionError)
    def test_parse_feat_str_invalid_tok(self):
        _parse_feat_str("", dict(), None)


    def test_parse_feat_str_parse_exp(self):
        feature_string = "jaccard~(qgm_3(ltuple[['zipcode']), qgm_3(rtuple['zipcode']))"
        p_dict = _parse_feat_str(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        for k,v in six.iteritems(p_dict):
            if k != 'is_auto_generated':
                self.assertEqual(v, 'PARSE_EXP')

    def test_parse_feat_str_parse_valid_1(self):
        feature_string = "jaccard(qgm_3(ltuple['zipcode']), qgm_3(rtuple['zipcode']))"
        p_dict = _parse_feat_str(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        self.assertEqual(p_dict['left_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['right_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['simfunction'], 'jaccard')
        self.assertEqual(p_dict['left_attribute'], 'zipcode')
        self.assertEqual(p_dict['right_attribute'], 'zipcode')

    def test_parse_feat_str_parse_valid_2(self):
        feature_string = "jaccard(qgm_3(ltuple['zipcode']), qgm_3(ltuple['zipcode']))"
        p_dict = _parse_feat_str(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        self.assertEqual(p_dict['left_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['right_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['simfunction'], 'jaccard')
        # self.assertEqual(p_dict['left_attribute'], 'zipcode')
        # self.assertEqual(p_dict['right_attribute'], 'zipcode')


    def test_parse_feat_str_parse_valid_3(self):
        feature_string = "jaccard(qgm_3(rtuple['zipcode']), qgm_3(rtuple['zipcode']))"
        p_dict = _parse_feat_str(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        self.assertEqual(p_dict['left_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['right_attr_tokenizer'], 'qgm_3')
        self.assertEqual(p_dict['simfunction'], 'jaccard')
        # self.assertEqual(p_dict['left_attribute'], 'zipcode')
        # self.assertEqual(p_dict['right_attribute'], 'zipcode')

    def test_add_feature_empty_df(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = create_feature_table()
        len1 = len(feature_table)
        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        add_feature(feature_table, 'test', f_dict)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.ix[len(feature_table)-1, 'function'](A.ix[1], B.ix[2]), 1.0)

    @raises(AssertionError)
    def test_add_feature_invalid_df(self):
        add_feature(None, 'test', dict())

    @raises(AssertionError)
    def test_add_feature_invalid_feat_name_type(self):
        add_feature(pd.DataFrame(), None, dict())

    @raises(AssertionError)
    def test_add_feature_invalid_feature_dict_type(self):
        add_feature(pd.DataFrame(), "", None)

    @raises(AssertionError)
    def test_add_feature_invalid_df_columns(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')

        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        add_feature(pd.DataFrame(), 'test', f_dict)

    @raises(AssertionError)
    def test_add_feature_name_already_present(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = create_feature_table()
        len1 = len(feature_table)
        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        add_feature(feature_table, 'test', f_dict)
        add_feature(feature_table, 'test', f_dict)


class AddBlackBoxFeatureTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_add_bb_feature_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_matching(A, B)
        def bb_fn(ltuple, rtuple):
            return 1.0
        len1 = len(feature_table)
        add_blackbox_feature(feature_table, 'test', bb_fn)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.ix[len(feature_table)-1, 'function'](A.ix[1], B.ix[2]), 1.0)

    def test_add_bb_feature_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = create_feature_table()
        def bb_fn(ltuple, rtuple):
            return 1.0
        len1 = len(feature_table)
        add_blackbox_feature(feature_table, 'test', bb_fn)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.ix[len(feature_table)-1, 'function'](A.ix[1], B.ix[2]), 1.0)

    @raises(AssertionError)
    def test_add_bb_feature_invalid_df(self):
        add_blackbox_feature(None, 'test', dict())

    @raises(AssertionError)
    def test_add_bb_feature_invalid_feat_name_type(self):
        add_blackbox_feature(pd.DataFrame(), None, dict())

    @raises(AssertionError)
    def test_add_bb_feature_invalid_feature_dict_type(self):
        add_blackbox_feature(pd.DataFrame(), "", None)

    @raises(AssertionError)
    def test_add_bb_feature_invalid_df_columns(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')

        def bb_fn(ltuple, rtuple):
            return 1.0


        add_blackbox_feature(pd.DataFrame(), 'test', bb_fn)

    @raises(AssertionError)
    def test_add_bb_feature_name_already_present(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = create_feature_table()
        len1 = len(feature_table)
        def bb_fn(ltuple, rtuple):
            return 1.0

        add_blackbox_feature(feature_table, 'test', bb_fn)
        add_blackbox_feature(feature_table, 'test', bb_fn)
