import os
from nose.tools import *
import unittest
import pandas as pd

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.feature.simfunctions import get_sim_funs_for_matching
from py_entitymatching.feature.tokenizers import get_tokenizers_for_matching

import py_entitymatching.feature.autofeaturegen as afg
import py_entitymatching.feature.attributeutils as au
import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.feature.simfunctions as simfuncs
import py_entitymatching.feature.tokenizers as toks

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
bc_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'blockercombiner'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

class AutoFeatureGenerationTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_get_features_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        tok = get_tokenizers_for_matching()
        sim = get_sim_funs_for_matching()
        feat_table = afg.get_features(A, B, l_attr_types, r_attr_types, attr_corres, tok, sim)
        self.assertEqual(isinstance(feat_table, pd.DataFrame), True)
        functions = feat_table['function']
        for f in functions:
            x = f(A.ix[1], B.ix[2])
            self.assertEqual(x >= 0, True)

    @raises(AssertionError)
    def test_get_features_invalid_df1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        tok = get_tokenizers_for_matching()
        sim = get_sim_funs_for_matching()
        feat_table = afg.get_features(None, B, l_attr_types, r_attr_types, attr_corres, tok, sim)

    @raises(AssertionError)
    def test_get_features_invalid_df2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        tok = get_tokenizers_for_matching()
        sim = get_sim_funs_for_matching()
        feat_table = afg.get_features(A, None, l_attr_types, r_attr_types, attr_corres, tok, sim)

    @raises(AssertionError)
    def test_get_features_invalid_ltable_rtable_switch(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(B, A)
        tok = get_tokenizers_for_matching()
        sim = get_sim_funs_for_matching()
        feat_table = afg.get_features(A, B, l_attr_types, r_attr_types, attr_corres, tok, sim)


    def test_get_features_for_blocking_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_blocking(A, B)
        self.assertEqual(isinstance(feat_table, pd.DataFrame), True)
        functions = feat_table['function']
        for f in functions:
            x = f(A.ix[1], B.ix[2])
            self.assertEqual(x >= 0, True)

    @raises(AssertionError)
    def test_get_features_for_blocking_invalid_df1(self):
        # A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_blocking(None, B)

    @raises(AssertionError)
    def test_get_features_for_blocking_invalid_df2(self):
        A = read_csv_metadata(path_a)
        # B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_blocking(A, None)

    def test_get_features_for_matching_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_matching(A, B)
        self.assertEqual(isinstance(feat_table, pd.DataFrame), True)
        functions = feat_table['function']
        for f in functions:
            x = f(A.ix[1], B.ix[2])
            self.assertEqual(x >= 0, True)

    @raises(AssertionError)
    def test_get_features_for_matching_invalid_df1(self):
        # A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_matching(None, B)

    @raises(AssertionError)
    def test_get_features_for_matching_invalid_df2(self):
        A = read_csv_metadata(path_a)
        # B = read_csv_metadata(path_b, key='ID')
        feat_table = afg.get_features_for_matching(A, None)


    def test_check_table_order_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        status = afg._check_table_order(A, B, l_attr_types, r_attr_types, attr_corres)
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_check_table_order_invalid_df1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        status = afg._check_table_order(None, B, l_attr_types, r_attr_types, attr_corres)

    @raises(AssertionError)
    def test_check_table_order_invalid_df2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        status = afg._check_table_order(A, None, l_attr_types, r_attr_types, attr_corres)


    def test_check_table_order_invalid_l_attrtype_table(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        l_attr_types['_table'] = pd.DataFrame()
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        status = afg._check_table_order(A, B, l_attr_types, r_attr_types, attr_corres)
        self.assertEqual(status, False)

    def test_check_table_order_invalid_r_attrtype_table(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        r_attr_types['_table'] = pd.DataFrame()
        attr_corres = au.get_attr_corres(A, B)
        status = afg._check_table_order(A, B, l_attr_types, r_attr_types, attr_corres)
        self.assertEqual(status, False)

    def test_check_table_order_invalid_attrcorres_rtable(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        attr_corres['rtable'] = pd.DataFrame()
        status = afg._check_table_order(A, B, l_attr_types, r_attr_types, attr_corres)
        self.assertEqual(status, False)

    def test_check_table_order_invalid_attrcorres_ltable(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_attr_types = au.get_attr_types(A)
        r_attr_types = au.get_attr_types(B)
        attr_corres = au.get_attr_corres(A, B)
        attr_corres['ltable'] = pd.DataFrame()
        status = afg._check_table_order(A, B, l_attr_types, r_attr_types, attr_corres)
        self.assertEqual(status, False)

    def test_check_get_lkp_tbl_valid(self):
        x = afg._get_feat_lkp_tbl()
        self.assertEqual(isinstance(x, dict), True)

    def test_get_features_for_type_valid_1(self):
        x = afg._get_features_for_type('str_eq_1w')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    def test_get_features_for_type_valid_2(self):
        x = afg._get_features_for_type('str_bt_1w_5w')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    def test_get_features_for_type_valid_3(self):
        x = afg._get_features_for_type('str_bt_5w_10w')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    def test_get_features_for_type_valid_4(self):
        x = afg._get_features_for_type('str_gt_10w')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    def test_get_features_for_type_valid_5(self):
        x = afg._get_features_for_type('numeric')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    def test_get_features_for_type_valid_6(self):
        x = afg._get_features_for_type('boolean')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x) >= 0, True)

    @raises(TypeError)
    def test_get_features_for_type_invlaid(self):
        x = afg._get_features_for_type('unknown')

    def test_get_magellan_str_types(self):
        x = afg.get_magellan_str_types()

    def test_valid_tok_sim_valid(self):
        sim = simfuncs.get_sim_funs_for_blocking()
        tok = toks.get_tokenizers_for_blocking()
        status = afg.check_valid_tok_sim(('lev1', 'tok', 'tok'), sim, tok)
        self.assertEqual(status, None)

    def test_get_fn_str_valid1(self):
        status = afg.get_fn_str(None, None)
        self.assertEqual(status, None)

    def test_get_fn_str_valid2(self):
        status = afg.get_fn_str('lev', ('year', 'year'))
        self.assertNotEqual(status, None)
