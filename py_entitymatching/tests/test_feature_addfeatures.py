import os
# from nose.tools import *
import unittest
import pandas as pd
import six
from .utils import raises

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.feature.simfunctions import get_sim_funs_for_matching
from py_entitymatching.feature.tokenizers import get_tokenizers_for_matching
from py_entitymatching.feature.autofeaturegen import get_features_for_matching
from py_entitymatching.feature.addfeatures import add_feature, add_blackbox_feature, get_feature_fn, _parse_feat_str, \
    create_feature_table

import py_entitymatching.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
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
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        len1 = len(feature_table)
        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())
        add_feature(feature_table, 'test', f_dict)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.loc[len(feature_table)-1, 'function'](A.loc[1], B.loc[2]), 1.0)


    def test_feature_fn_valid_nosim_tok(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
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
        self.assertEqual(feature_table.loc[len(feature_table)-1, 'function'](A.loc[1], B.loc[2]), 1.0)

    @raises(AssertionError)
    def test_add_feature_invalid_df(self):
        add_feature(None, 'test', dict())

    @raises(AssertionError)
    def test_add_feature_invalid_feat_name_type(self):
        add_feature(pd.DataFrame(), None, dict())

    @raises(AssertionError)
    def test_add_feature_invalid_feature_dict_type(self):
        add_feature(pd.DataFrame(), "", None)

    def test_add_feature_invalid_df_columns(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')

        feature_string = "exact_match(ltuple['zipcode'], rtuple['zipcode'])"
        f_dict = get_feature_fn(feature_string, get_tokenizers_for_matching(), get_sim_funs_for_matching())

        with self.assertRaises(AssertionError) as ctx:
            add_feature(pd.DataFrame(), 'test', f_dict)

        actual = str(ctx.exception)
        print(actual)
        expected = 'Feature table does not have all required columns\n ' \
                   'The following columns are missing: feature_name, left_attribute, right_attribute, ' \
                   'left_attr_tokenizer,' \
                   ' right_attr_tokenizer, simfunction, function, function_source, is_auto_generated'
        self.assertEqual(actual, expected)

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
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        def bb_fn(ltuple, rtuple):
            return 1.0
        len1 = len(feature_table)
        add_blackbox_feature(feature_table, 'test', bb_fn)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        self.assertEqual(feature_table.loc[len(feature_table)-1, 'function'](A.loc[1], B.loc[2]), 1.0)

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
        self.assertEqual(feature_table.loc[len(feature_table)-1, 'function'](A.loc[1], B.loc[2]), 1.0)
    
    def test_add_bb_feature_with_attrs(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        def bb_fn(ltuple, rtuple):
            return min(len(ltuple['name']), len(rtuple['name']))
        len1 = len(feature_table)
        attrs = {
            'left_attribute': 'name',
            'right_attribute': 'name'
        }
        add_blackbox_feature(feature_table, 'bb_attr_test', bb_fn, **attrs)
        len2 = len(feature_table)
        self.assertEqual(len1+1, len2)
        added_feature = feature_table.iloc[len(feature_table)-1]
        self.assertEqual(added_feature.feature_name, 'bb_attr_test')
        self.assertEqual(added_feature.left_attribute, 'name')
        self.assertEqual(added_feature.right_attribute, 'name')
        self.assertEqual(added_feature.simfunction, None)

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
