import os
from nose.tools import *
import unittest
import pandas as pd
import numpy as np

import py_entitymatching as em
from py_entitymatching.utils.generic_helper import get_install_path
import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.io.parsers import read_csv_metadata

#import sys
#sys.path.insert(0, '../debugblocker')
#import debugblocker as db
import py_entitymatching.debugblocker.debugblocker as db

from operator import itemgetter
from array import array


datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
catalog_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'catalog'])
debugblocker_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'debugblocker'])

path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

class DebugblockerTestCases(unittest.TestCase):
    def test_validate_types_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_ID',
                fk_rtable='rtable_ID', key = '_id')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        attr_corres = None
        db._validate_types(A, B, C, 100, attr_corres, False)

    def test_validate_types_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_' +
                A_key, fk_rtable='rtable_' + B_key, key = '_id')
        attr_corres  = [('ID', 'ID'), ('name', 'name'),
                         ('birth_year', 'birth_year'),
                         ('hourly_wage', 'hourly_wage'),
                         ('address', 'address'),
                         ('zipcode', 'zipcode')]
        db._validate_types(A, B, C, 100, attr_corres, False)

    def test_check_input_field_correspondence_list_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = None
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    def test_check_input_field_correspondence_list_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = []
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    @raises(AssertionError)
    def test_check_input_field_correspondence_list_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = [('adsf', 'fdsa'), 'asdf']
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    @raises(AssertionError)
    def test_check_input_field_correspondence_list_4(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = [('asdf', 'fdsa')]
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    @raises(AssertionError)
    def test_check_input_field_correspondence_list_5(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = [('address', 'fdsa')]
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    def test_check_input_field_correspondence_list_7(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        field_corres_list = [('zipcode', 'zipcode'),
                             ('birth_year', 'birth_year')]
        db._check_input_field_correspondence_list(A, B, field_corres_list)

    def test_get_field_correspondence_list_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        expected_list = [('ID', 'ID'), ('name', 'name'),
                         ('birth_year', 'birth_year'),
                         ('hourly_wage', 'hourly_wage'),
                         ('address', 'address'),
                         ('zipcode', 'zipcode')]
        attr_corres = None
        corres_list = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)
        self.assertEqual(corres_list, expected_list)

        attr_corres = []
        corres_list = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)
        self.assertEqual(corres_list, expected_list)

    def test_get_field_correspondence_list_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        expected_list = [('ID', 'ID'), ('name', 'name'),
                         ('address', 'address'),
                         ('zipcode', 'zipcode')]
        attr_corres = [('ID', 'ID'), ('name', 'name'),
                         ('address', 'address'),
                         ('zipcode', 'zipcode')]
        corres_list = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)
        self.assertEqual(corres_list, expected_list)

    def test_get_field_correspondence_list_3(self):
        data = [[1, 'asdf', 'a0001']]
        A = pd.DataFrame(data)
        A.columns = ['Id', 'Title', 'ISBN']
        A_key = 'Id'
        B = pd.DataFrame(data)
        B.columns = ['Id', 'title', 'ISBN']
        B_key = 'Id'
        attr_corres = []

        corres_list = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)
        expected_list = [('Id', 'Id'), ('ISBN', 'ISBN')]
        self.assertEqual(corres_list, expected_list)

    @raises(AssertionError)
    def test_get_field_correspondence_list_4(self):
        data = [[1, 'asdf', 'a0001']]
        A = pd.DataFrame(data)
        A.columns = ['ID', 'Title', 'isbn']
        A_key = 'ID'
        B = pd.DataFrame(data)
        B.columns = ['Id', 'title', 'ISBN']
        B_key = 'Id'
        attr_corres = []
        db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)

    def test_get_field_correspondence_list_5(self):
        A = pd.DataFrame([[0, 'A', 0.11, 'ASDF']])
        A.columns = ['ID', 'name', 'price', 'desc']
        em.set_key(A, 'ID')
        A_key = em.get_key(A)
        B = pd.DataFrame([['B', 'B001', 'ASDF', 0.111]])
        B.columns = ['item_name', 'item_id', 'item_desc', 'item_price']
        em.set_key(B, 'item_id')
        B_key = em.get_key(B)
        attr_corres = [('name', 'item_name'),
                       ('price', 'item_price')]
        actual_attr_corres = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)

        expected_attr_corres = [('name', 'item_name'),
                                ('price', 'item_price'),
                                ('ID', 'item_id')]
        self.assertEqual(expected_attr_corres, actual_attr_corres)

    def test_build_col_name_index_dict_1(self):
        A = pd.DataFrame([[]])
        A.columns = []
        col_index = db._build_col_name_index_dict(A)

    def test_build_col_name_index_dict_2(self):
        A = pd.DataFrame([[0, 'A', 0.11, 'ASDF']])
        A.columns = ['ID', 'name', 'price', 'desc']
        em.set_key(A, 'ID')
        col_index = db._build_col_name_index_dict(A)
        self.assertEqual(col_index['ID'], 0)
        self.assertEqual(col_index['name'], 1)
        self.assertEqual(col_index['price'], 2)
        self.assertEqual(col_index['desc'], 3)

    @raises(AssertionError)
    def test_filter_corres_list_1(self):
        A = pd.DataFrame([[0, 20, 0.11, 4576]])
        A.columns = ['ID', 'age', 'price', 'zip code']
        em.set_key(A, 'ID')
        B = pd.DataFrame([[0, 240, 0.311, 4474]])
        B.columns = ['ID', 'age', 'price', 'zip code']
        em.set_key(A, 'ID')
        A_key = 'ID'
        B_key = 'ID'
        ltable_col_dict = db._build_col_name_index_dict(A)
        rtable_col_dict = db._build_col_name_index_dict(B)
        attr_corres = [('ID', 'ID'), ('age', 'age'),
                         ('price', 'price'),
                         ('zip code', 'zip code')]
        db._filter_corres_list(A, B, A_key, B_key, ltable_col_dict,
                rtable_col_dict, attr_corres)

    def test_filter_corres_list_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        ltable_col_dict = db._build_col_name_index_dict(A)
        rtable_col_dict = db._build_col_name_index_dict(B)
        attr_corres = [('ID', 'ID'), ('name', 'name'),
                         ('birth_year', 'birth_year'),
                         ('hourly_wage', 'hourly_wage'),
                         ('address', 'address'),
                         ('zipcode', 'zipcode')]
        expected_filtered_attr = [('ID', 'ID'), ('name', 'name'),
                         ('address', 'address')]
        db._filter_corres_list(A, B, A_key, B_key, ltable_col_dict,
                rtable_col_dict, attr_corres)
        self.assertEqual(expected_filtered_attr, attr_corres)

    def test_get_filtered_table(self):
        A = pd.DataFrame([['a1', 'A', 0.11, 53704]])
        A.columns = ['ID', 'name', 'price', 'zip code']
        em.set_key(A, 'ID')
        B = pd.DataFrame([['b1', 'A', 0.11, 54321]])
        B.columns = ['ID', 'name', 'price', 'zip code']
        em.set_key(B, 'ID')
        A_key = 'ID'
        B_key = 'ID'
        ltable_col_dict = db._build_col_name_index_dict(A)
        rtable_col_dict = db._build_col_name_index_dict(B)
        attr_corres = [('ID', 'ID'), ('name', 'name'),
                         ('price', 'price'),
                         ('zip code', 'zip code')]
        db._filter_corres_list(A, B, A_key, B_key, ltable_col_dict,
                rtable_col_dict, attr_corres)

        filtered_A, filtered_B = db._get_filtered_table(A, B, attr_corres)

        expected_filtered_A = pd.DataFrame([['a1', 'A']])
        expected_filtered_A.columns = ['ID', 'name']
        em.set_key(expected_filtered_A, 'ID')
        expected_filtered_B = pd.DataFrame([['b1', 'A']])
        expected_filtered_B.columns = ['ID', 'name']
        em.set_key(expected_filtered_B, 'ID')

        self.assertEqual(expected_filtered_A.equals(filtered_A), True)
        self.assertEqual(expected_filtered_B.equals(filtered_B), True)

    @raises(AssertionError)
    def test_get_feature_weight_1(self):
        A = []
        dataframe = pd.DataFrame(A)
        db._get_feature_weight(dataframe)

    def test_get_feature_weight_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        cols_A = list(A.columns)
        cols_B = list(B.columns)
        corres_list = [(cols_A[0], cols_B[0]), (cols_A[1], cols_B[1]), (cols_A[4],
                                                                        cols_B[4]),
                       (cols_A[5], cols_B[5])]
        A_filtered, B_filtered = db._get_filtered_table(
            A, B, corres_list)
        A_wlist = db._get_feature_weight(A_filtered)
        expected_A_wlist = [2.0, 2.0, 2.0, 1.4]
        self.assertEqual(A_wlist, expected_A_wlist)

        B_wlist = db._get_feature_weight(B_filtered)
        expected_B_wlist = [2.0, 2.0, 2.0, 1.3333333333333333]
        self.assertEqual(B_wlist, expected_B_wlist)

    def test_get_feature_weight_3(self):
        table = [[''], [np.nan]]
        dataframe = pd.DataFrame(table)
        weight_list = db._get_feature_weight(dataframe)
        self.assertEqual(weight_list, [0.0])

    def test_select_features_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        actual_selected_features = db._select_features(A, B, A_key, B_key)
        expected_selected_features = [1, 3, 4, 2, 5]
        self.assertEqual(actual_selected_features, expected_selected_features)

    def test_select_features_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        cols_A = list(A.columns)
        cols_B = list(B.columns)
        corres_list = [(cols_A[0], cols_B[0]), (cols_A[1], cols_B[1]), (cols_A[4],
                                                                        cols_B[4])]
        A_filtered, B_filtered = db._get_filtered_table(A, B, corres_list)
        actual_selected_features = db._select_features(
            A_filtered, B_filtered, A_key, B_key)
        expected_selected_features = [1, 2]
        self.assertEqual(actual_selected_features, expected_selected_features)

    def test_select_features_3(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        cols_A = list(A.columns)
        cols_B = list(B.columns)

        corres_list = [(cols_A[0], cols_B[0])]
        A_filtered, B_filtered = db._get_filtered_table(A, B, corres_list)
        actual_selected_features = db._select_features(
            A_filtered, B_filtered, A_key, B_key)
        expected_selected_features = []
        self.assertEqual(actual_selected_features, expected_selected_features)

    @raises(AssertionError)
    def test_select_features_4(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        cols_A = list(A.columns)
        cols_B = list(B.columns)

        A_field_set = [0, 1, 2]
        B_field_set = [0, 1, 2, 3]

        A_field_set = list(itemgetter(*A_field_set)(cols_A))
        B_field_set = list(itemgetter(*B_field_set)(cols_B))

        A_filtered = A[A_field_set]
        B_filtered = B[B_field_set]
        db._select_features(
            A_filtered, B_filtered, A_key, B_key)

    @raises(AssertionError)
    def test_select_features_5(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        cols_A = list(A.columns)
        cols_B = list(B.columns)

        A_field_set = [0, 1, 2, 3]
        B_field_set = [0, 1, 2]

        A_field_set = list(itemgetter(*A_field_set)(cols_A))
        B_field_set = list(itemgetter(*B_field_set)(cols_B))


        A_filtered = A[A_field_set]
        B_filtered = B[B_field_set]
        db._select_features(
            A_filtered, B_filtered, A_key, B_key)

    def test_build_id_to_index_map_1(self):
        A = read_csv_metadata(path_a, key='ID')
        key = em.get_key(A)
        actual_rec_id_to_idx = db._build_id_to_index_map(A, key)
        expected_rec_id_to_idx = {'a1': 0, 'a3': 2, 'a2': 1, 'a5': 4, 'a4': 3}
        self.assertEqual(actual_rec_id_to_idx, expected_rec_id_to_idx)

    @raises(AssertionError)
    def test_build_id_to_index_map_2(self):
        table = [['a1', 'hello'], ['a1', 'world']]
        key = 'ID'
        dataframe = pd.DataFrame(table)
        dataframe.columns = ['ID', 'title']
        em.set_key(dataframe, key)
        db._build_id_to_index_map(dataframe, key)

    def test_replace_nan_to_empty_1(self):
        field = np.nan
        self.assertEqual(db._replace_nan_to_empty(field), '')

    def test_replace_nan_to_empty_2(self):
        field = ''
        self.assertEqual(db._replace_nan_to_empty(field), '')
        field = 'string'
        self.assertEqual(db._replace_nan_to_empty(field), 'string')

    def test_replace_nan_to_empty_3(self):
        field = 1
        self.assertEqual(db._replace_nan_to_empty(field), '1')
        field = 3.57
        self.assertEqual(db._replace_nan_to_empty(field), '4')
        field = 1234.5678e5
        self.assertEqual(db._replace_nan_to_empty(field), '123456780')

    def test_get_tokenized_column_1(self):
        column = []
        actual_ret_column = db._get_tokenized_column(column)
        expected_ret_column = []
        self.assertEqual(actual_ret_column, expected_ret_column)

    def test_get_tokenized_column_2(self):
        column = ['hello world', np.nan, 'how are you',
                  '', 'this is a blocking debugger']
        actual_ret_column = db._get_tokenized_column(column)
        expected_ret_column = [['hello', 'world'], [''],
                               ['how', 'are', 'you'], [''],
                                ['this', 'is', 'a', 'blocking', 'debugger']]
        self.assertEqual(actual_ret_column, expected_ret_column)

    def test_get_tokenized_table_1(self):
        A = read_csv_metadata(path_a, key='ID')
        A_key = em.get_key(A)
        feature_list = range(len(A.columns))
        actual_record_list = db._get_tokenized_table(A, A_key, feature_list)

        expected_record_list = [[('a1', 0), ('kevin', 1), ('smith', 1), ('1989', 2), ('30', 3), 
            ('607', 4), ('from', 4), ('st,', 4), ('san', 4), ('francisco', 4), ('94107',5)], 
            [('a2', 0), ('michael', 1), ('franklin', 1), ('1988', 2), ('28', 3), ('1652', 4),
            ('stockton', 4), ('st,', 4), ('san', 4), ('francisco', 4), ('94122', 5)], [('a3', 0),
            ('william', 1), ('bridge', 1), ('1986', 2), ('32', 3), ('3131', 4), ('webster', 4), 
            ('st,', 4), ('san', 4), ('francisco', 4), ('94107', 5)], [('a4', 0), ('binto', 1), 
            ('george', 1), ('1987', 2), ('32', 3), ('423', 4), ('powell', 4), ('st,', 4), 
            ('san', 4), ('francisco', 4), ('94122', 5)], [('a5', 0), ('alphonse', 1), ('kemper', 1),
            ('1984', 2), ('35', 3), ('1702', 4), ('post', 4), ('street,', 4), ('san', 4), 
            ('francisco', 4), ('94122', 5)]]

        self.assertEqual(actual_record_list, expected_record_list)

    def test_get_tokenized_table_2(self):
        B = read_csv_metadata(path_b, key='ID')
        B_key = em.get_key(B)
        feature_list = [0, 1, 3]
        actual_record_list = db._get_tokenized_table(B, B_key, feature_list)
        expected_record_list = [[('b1', 0), ('mark', 1), ('levene', 1), ('30', 2)], 
                                [('b2', 0), ('bill', 1), ('bridge', 1), ('32', 2)], 
                                [('b3', 0), ('mike', 1), ('franklin', 1), ('28', 2)], 
                                [('b4', 0), ('joseph', 1), ('kuan', 1), ('26', 2)], 
                                [('b5', 0), ('alfons', 1), ('kemper', 1), ('35', 2)], 
                                [('b6', 0), ('michael',1), ('brodie', 1), ('32', 2)]]
        self.assertEqual(actual_record_list, expected_record_list)

    def test_get_tokenized_table_3(self):
        table = [[1, 'abc abc asdf', '123-3456-7890', np.nan, '',
                  '135 east  abc  st'],
                 [2, 'aaa bbb', '000-111-2222', '', '', '246  west abc st'],
                 [3, 'cc dd', '123-123-1231', 'cc', 'unknown', ' 246 west def st']]
        dataframe = pd.DataFrame(table)
        dataframe.columns = ['ID', 'name', 'phone', 'department', 'school', 'address']
        key = 'ID'
        em.set_key(dataframe, key)
        feature_list = [1, 3, 4, 5]
        actual_record_list = db._get_tokenized_table(dataframe, key, feature_list)
        expected_record_list = [[('abc', 0), ('abc_1', 0), ('asdf', 0), ('135', 3), ('east', 3),
                                ('abc_2', 3), ('st', 3)], [('aaa', 0), ('bbb', 0), ('246', 3),
                                ('west', 3), ('abc', 3), ('st', 3)], [('cc', 0), ('dd', 0),
                                ('cc_1', 1), ('unknown', 2), ('246', 3), ('west', 3),
                                ('def', 3), ('st', 3)]]
        self.assertEqual(actual_record_list, expected_record_list)

    def test_build_global_token_order_impl_1(self):
        record_list = []
        actual_dict = {}
        expected_dict = {}
        db._build_global_token_order_impl(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

        record_list = [[], [], []]
        actual_dict = {}
        expected_dict = {}
        db._build_global_token_order_impl(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

    def test_build_global_token_order_impl_2(self):
        record_list = [['c', 'b', 'a'], [], ['b', 'c'], ['c', 'c']]
        actual_dict = {}
        expected_dict = {'a': 1, 'c': 4, 'b': 2}
        db._build_global_token_order_impl(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

    def test_build_global_token_order_1(self):
        l_record_list = []
        r_record_list = []
        expected_order_dict = {}
        expected_token_index_dict = {}
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        self.assertEqual(order_dict, expected_order_dict)
        self.assertEqual(token_index_dict, expected_token_index_dict)

    def test_build_global_token_order_2(self):
        l_record_list = [[], [], []]
        r_record_list = [[]]
        expected_order_dict = {}
        expected_token_index_dict = {}
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        self.assertEqual(order_dict, expected_order_dict)
        self.assertEqual(token_index_dict, expected_token_index_dict)

    def test_build_global_token_order_3(self):
        l_record_list = [['c', 'b', 'a'], [], ['b', 'c'], ['c', 'c']]
        r_record_list = [['e'], ['b', 'a']]
        expected_token_index_dict = {0: 'e', 1: 'a', 2: 'b', 3: 'c'}
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        self.assertEqual(order_dict['e'], 0)
        self.assertEqual(order_dict['a'], 1)
        self.assertEqual(order_dict['b'], 2)
        self.assertEqual(order_dict['c'], 3)
        self.assertEqual(token_index_dict, expected_token_index_dict)
    
    def test_replace_token_with_numeric_index_1(self):
        l_record_list = []
        r_record_list = []
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        expected_l_record_list = []
        expected_r_record_list = [] 
        db._replace_token_with_numeric_index(l_record_list, order_dict)
        db._replace_token_with_numeric_index(r_record_list, order_dict)
        self.assertEqual(l_record_list, expected_l_record_list)
        self.assertEqual(r_record_list, expected_r_record_list)

    def test_replace_token_with_numeric_index_2(self):
        l_record_list = [[], []]
        r_record_list = [[]]
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        expected_l_record_list = [[], []]
        expected_r_record_list = [[]] 
        db._replace_token_with_numeric_index(l_record_list, order_dict)
        db._replace_token_with_numeric_index(r_record_list, order_dict)
        self.assertEqual(l_record_list, expected_l_record_list)
        self.assertEqual(r_record_list, expected_r_record_list)

    def test_replace_token_with_numeric_index_3(self):
        l_record_list = [[('c', 0), ('b', 0), ('a', 1)], [('b', 0), ('c', 1)]]
        r_record_list = [[('e', 0), ('b', 0)], [('b', 0), ('a', 1)]]
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        expected_l_record_list = [[(2, 0), (3, 0), (1, 1)], [(3, 0), (2, 1)]]
        expected_r_record_list = [[(0, 0), (3, 0)], [(3, 0), (1, 1)]]
        db._replace_token_with_numeric_index(l_record_list, order_dict)
        db._replace_token_with_numeric_index(r_record_list, order_dict)
        self.assertEqual(l_record_list, expected_l_record_list)
        self.assertEqual(r_record_list, expected_r_record_list)

    def test_sort_record_tokens_by_global_order_1(self):
        record_list = []
        expected_record_list = []
        db._sort_record_tokens_by_global_order(record_list)
        self.assertEqual(record_list, expected_record_list)

    def test_sort_record_tokens_by_global_order_2(self):
        record_list = [[], []]
        expected_record_list = [[], []]
        db._sort_record_tokens_by_global_order(record_list)
        self.assertEqual(record_list, expected_record_list)

    def test_sort_record_tokens_by_global_order_3(self):
        record_list = [[(3, 1), (4, 2), (100, 0), (1, 2)], [(2, 1), (0, 1), (10, 3)]]
        expected_record_list = [[(1, 2), (3, 1), (4, 2), (100, 0)], [(0, 1),
            (2,1), (10, 3)]]
        db._sort_record_tokens_by_global_order(record_list)
        self.assertEqual(record_list, expected_record_list)

    def test_sort_record_tokens_by_global_order_4(self):
        l_record_list = [[('c', 0), ('b', 0), ('a', 1)], [('b', 0), ('c', 1)]]
        r_record_list = [[('e', 0), ('b', 0)], [('b', 0), ('a', 1)]]
        order_dict, token_index_dict = db._build_global_token_order(l_record_list, r_record_list)
        expected_l_record_list = [[(1, 1), (2, 0), (3, 0)], [(2, 1), (3, 0)]]
        expected_r_record_list = [[(0, 0), (3, 0)], [(1, 1), (3, 0)]]
        db._replace_token_with_numeric_index(l_record_list, order_dict)
        db._replace_token_with_numeric_index(r_record_list, order_dict)
        db._sort_record_tokens_by_global_order(l_record_list)
        db._sort_record_tokens_by_global_order(r_record_list)
        self.assertEqual(l_record_list, expected_l_record_list)
        self.assertEqual(r_record_list, expected_r_record_list)

    def test_split_record_token_and_index_1(self):
        record_list = []
        record_token_list, record_index_list =\
            db._split_record_token_and_index(record_list) 
        expected_record_token_list = []
        expected_record_index_list = []
        self.assertEqual(record_token_list, expected_record_token_list)
        self.assertEqual(record_index_list, expected_record_index_list)

    def test_split_record_token_and_index_2(self):
        record_list = [[], []]
        record_token_list, record_index_list =\
            db._split_record_token_and_index(record_list) 
        expected_record_token_list = [array('I'), array('I')]
        expected_record_index_list = [array('I'), array('I')]
        self.assertEqual(record_token_list, expected_record_token_list)
        self.assertEqual(record_index_list, expected_record_index_list)

    def test_split_record_token_and_index_3(self):
        record_list = [[(1, 2), (3, 1), (4, 2), (100, 0)], [(0, 1), (2, 1), (10, 3)]]
        record_token_list, record_index_list =\
            db._split_record_token_and_index(record_list) 
        expected_record_token_list = [array('I', [1, 3, 4, 100]), array('I', [0, 2, 10])]
        expected_record_index_list = [array('I', [2, 1, 2, 0]), array('I', [1, 1, 3])]
        self.assertEqual(record_token_list, expected_record_token_list)
        self.assertEqual(record_index_list, expected_record_index_list)

    def test_index_candidate_set_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_' +
                l_key, fk_rtable='rtable_' + r_key, key = '_id')

        lrecord_id_to_index_map = db._build_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._build_id_to_index_map(B, r_key)

        expected_cand_set = {0: set([0, 1, 5]), 1: set([2, 3, 4]), 2: set([0, 1,
            5]), 3: set([2, 3, 4]), 4: set([2, 3, 4])}
        actual_cand_set = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(expected_cand_set, actual_cand_set)

    @raises(AssertionError)
    def test_index_candidate_set_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_' +
                l_key, fk_rtable='rtable_' + r_key, key = '_id')
        C.loc[0, 'ltable_ID'] = 'aaaa'

        lrecord_id_to_index_map = db._build_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._build_id_to_index_map(B, r_key)

        db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)

    @raises(AssertionError)
    def test_index_candidate_set_3(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_' +
                l_key, fk_rtable='rtable_' + r_key, key = '_id')
        C.loc[0, 'rtable_ID'] = 'bbbb'

        lrecord_id_to_index_map = db._build_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._build_id_to_index_map(B, r_key)

        db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)

    def test_index_candidate_set_4(self):
        A_list = [[1, 'asdf', 'fdas'], [2, 'fdsa', 'asdf']]
        B_list = [['B002', 'qqqq', 'wwww'], ['B003', 'rrrr', 'fdsa']]
        A = pd.DataFrame(A_list)
        A.columns = ['ID', 'f1', 'f2']
        em.set_key(A, 'ID')
        B = pd.DataFrame(B_list)
        B.columns = ['ID', 'f1', 'f2']
        em.set_key(B, 'ID')
        C_list = [[0, 1, 'B003'], [1, 2, 'B002']]
        C = pd.DataFrame(C_list)
        C.columns = ['_id', 'ltable_ID', 'rtable_ID']
        cm.set_candset_properties(C, '_id', 'ltable_ID',
                                  'rtable_ID', A, B)
        lrecord_id_to_index_map = db._build_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._build_id_to_index_map(B, 'ID')
        expected_cand_set = {0: set([1]), 1: set([0])}
        actual_cand_set = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(expected_cand_set, actual_cand_set)

    @raises(AssertionError)
    def test_index_candidate_set_5(self):
        A_list = [[1, 'asdf', 'fdas'], [2, 'fdsa', 'asdf']]
        B_list = [['B002', 'qqqq', 'wwww'], ['B003', 'rrrr', 'fdsa']]
        A = pd.DataFrame(A_list)
        A.columns = ['ID', 'f1', 'f2']
        em.set_key(A, 'ID')
        B = pd.DataFrame(B_list)
        B.columns = ['ID', 'f1', 'f2']
        em.set_key(B, 'ID')
        C_list = [[0, 1, 'B001'], [1, 2, 'B002']]
        C = pd.DataFrame(C_list)
        C.columns = ['_id', 'ltable_ID', 'rtable_ID']
        cm.set_candset_properties(C, '_id', 'ltable_ID',
                                  'rtable_ID', A, B)
        lrecord_id_to_index_map = db._build_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._build_id_to_index_map(B, 'ID')
        db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)

    def test_index_candidate_set_6(self):
        A_list = [[1, 'asdf', 'fdas'], [2, 'fdsa', 'asdf']]
        B_list = [['B002', 'qqqq', 'wwww'], ['B003', 'rrrr', 'fdsa']]
        A = pd.DataFrame(A_list)
        A.columns = ['ID', 'f1', 'f2']
        em.set_key(A, 'ID')
        B = pd.DataFrame(B_list)
        B.columns = ['ID', 'f1', 'f2']
        em.set_key(B, 'ID')
        C = pd.DataFrame()
        lrecord_id_to_index_map = db._build_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._build_id_to_index_map(B, 'ID')
        new_C = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(new_C, {})

    def test_calc_table_field_length_1(self):
        record_index_list = []
        field_length_list = db._calc_table_field_length(record_index_list, 4)
        expected_field_length_list = []
        self.assertEqual(field_length_list, expected_field_length_list)

    def test_calc_table_field_length_2(self):
        record_index_list = [array('I', [2, 1, 2, 0]), array('I', [1, 1, 3])]
        field_length_list = db._calc_table_field_length(record_index_list, 4)
        expected_field_length_list = [array('I', [1, 1, 2, 0]), array('I', [0,
            2, 0, 1])]
        self.assertEqual(field_length_list, expected_field_length_list)

    @raises(AssertionError)
    def test_calc_table_field_length_3(self):
        record_index_list = [array('I', [2, 1, 2, 0]), array('I', [1, 1, 3])]
        field_length_list = db._calc_table_field_length(record_index_list, 3)
        expected_field_length_list = [array('I', [1, 1, 2, 0]), array('I', [0,
            2, 0, 1])]
        self.assertEqual(field_length_list, expected_field_length_list)

    def test_calc_table_field_token_sum_1(self):
        field_length_list = []
        field_token_sum = db._calc_table_field_token_sum(field_length_list, 4)
        expected_field_token_sum = [0, 0, 0, 0]
        self.assertEqual(field_token_sum, expected_field_token_sum)

    def test_calc_table_field_token_sum_2(self):
        field_length_list = [array('I', [1, 1, 2, 0]), array('I', [0,
            2, 0, 1])]
        field_token_sum = db._calc_table_field_token_sum(field_length_list, 4)
        expected_field_token_sum = [1, 3, 2, 1]
        self.assertEqual(field_token_sum, expected_field_token_sum)

    def test_assemble_topk_table_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        topk_heap = []
        ret_dataframe = db._assemble_topk_table(topk_heap, A, B, A_key, B_key)
        self.assertEqual(len(ret_dataframe), 0)
        self.assertEqual(list(ret_dataframe.columns), [])

    def test_assemble_topk_table_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        topk_heap = [(0.2727272727272727, 1, 0), (0.23076923076923078, 0, 4),
                     (0.16666666666666666, 0, 3)]
        ret_dataframe = db._assemble_topk_table(topk_heap, A, B, A_key, B_key)
        expected_columns = ['_id', 'ltable_ID', 'rtable_ID',
                            'ltable_name', 'ltable_birth_year',
                            'ltable_hourly_wage',
                            'ltable_address', 'ltable_zipcode', 'rtable_name',
                            'rtable_birth_year', 'rtable_hourly_wage',
                            'rtable_address', 'rtable_zipcode']
        self.assertEqual(len(ret_dataframe), 3)
        self.assertEqual(list(ret_dataframe.columns), expected_columns)

        expected_recs = [[0, 'a2', 'b1', 'Michael Franklin',
                          1988, 27.5, '1652 Stockton St, San Francisco',
                          94122, 'Mark Levene', 1987, 29.5,
                          '108 Clement St, San Francisco', 94107],
                         [1, 'a1', 'b5', 'Kevin Smith',
                          1989, 30.0, '607 From St, San Francisco', 94107,
                          'Alfons Kemper', 1984, 35.0,
                          '170 Post St, Apt 4,  San Francisco', 94122],
                         [2, 'a1', 'b4', 'Kevin Smith',
                          1989, 30.0, '607 From St, San Francisco', 94107,
                          'Joseph Kuan', 1982, 26.0,
                          '108 South Park, San Francisco', 94122]]
        self.assertEqual(list(ret_dataframe.loc[0]), expected_recs[0])
        self.assertEqual(list(ret_dataframe.loc[1]), expected_recs[1])
        self.assertEqual(list(ret_dataframe.loc[2]), expected_recs[2])
    
    def test_debugblocker_config_cython_1(self):
        ltable_field_token_sum = {1}
        rtable_field_token_sum = {1}
        py_num_fields = 1
        config_list = db.debugblocker_config_cython(ltable_field_token_sum, rtable_field_token_sum, 
                                    py_num_fields, 2, 2)
        expected_config_list = [[0]]
        self.assertEqual(config_list, expected_config_list)

    def test_debugblocker_config_cython_2(self):
        ltable_field_token_sum = {4, 3, 2, 1}
        rtable_field_token_sum = {4, 3, 2, 1}
        py_num_fields = 4
        config_list = db.debugblocker_config_cython(ltable_field_token_sum, rtable_field_token_sum, 
                                    py_num_fields, 2, 2)
        expected_config_list = [[0, 1, 2, 3], [0, 1, 2], [0, 1], [0], [1, 2, 3], [0, 2, 3], [0, 1, 3],
                [1, 2], [0, 2], [1]]
        self.assertEqual(config_list, expected_config_list)

    def test_debugblocker_topk_cython_1(self):
        py_config = []
        lrecord_token_list = [[]]
        rrecord_token_list = [[]]
        lrecord_index_list = [[]]
        rrecord_index_list = [[]]
        py_cand_set = []
        py_output_size = 100
        rec_list = db.debugblocker_topk_cython(py_config, lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list, py_cand_set, py_output_size)
        expected_rec_list = []
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_topk_cython_2(self):
        py_config = []
        lrecord_token_list = [[]]
        rrecord_token_list = [[]]
        lrecord_index_list = [[]]
        rrecord_index_list = [[]]
        py_cand_set = None
        py_output_size = 100
        rec_list = db.debugblocker_topk_cython(py_config, lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list, py_cand_set, py_output_size)
        expected_rec_list = []
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_topk_cython_3(self):
        py_config = [0, 1]
        lrecord_token_list = [[1, 2]]
        rrecord_token_list = [[0, 1]]
        lrecord_index_list = [[1, 2]]
        rrecord_index_list = [[0, 1]]
        py_cand_set = None
        py_output_size = 100
        rec_list = db.debugblocker_topk_cython(py_config, lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list, py_cand_set, py_output_size)
        expected_rec_list = [[0, 0, 1]]
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_topk_cython_4(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_ID',
                fk_rtable='rtable_ID', key = '_id')
        py_config = [0, 1]
        lrecord_token_list = [array('I', [5, 12, 15, 22, 37, 38, 39]), 
                              array('I', [26, 30, 32, 34, 37, 38, 39]), 
                              array('I', [24, 27, 28, 36, 37, 38, 39]), 
                              array('I', [4, 10, 13, 21, 37, 38, 39]), 
                              array('I', [2, 7, 31, 33, 35, 38, 39])]
        rrecord_token_list = [array('I', [17, 18, 25, 29, 37, 38, 39]), 
                              array('I', [9, 27, 28, 36, 37, 38, 39]), 
                              array('I', [19, 26, 30, 34, 37, 38, 39]), 
                              array('I', [14, 16, 20, 23, 25, 38, 39]), 
                              array('I', [1, 3, 6, 8, 31, 33, 37, 38, 39]), 
                              array('I', [0, 11, 29, 32, 35, 38, 39])]
        lrecord_index_list = [array('I', [1, 1, 0, 0, 1, 1, 1]), 
                              array('I', [1, 0, 0, 1, 1, 1, 1]), 
                              array('I', [0, 1, 0, 1, 1, 1, 1]), 
                              array('I', [1, 0, 0, 1, 1, 1, 1]), 
                              array('I', [1, 0, 0, 1, 1, 1, 1])]
        rrecord_index_list = [array('I', [0, 0, 1, 1, 1, 1, 1]), 
                              array('I', [0, 1, 0, 1, 1, 1, 1]), 
                              array('I', [0, 1, 0, 1, 1, 1, 1]), 
                              array('I', [0, 0, 1, 1, 1, 1, 1]), 
                              array('I', [1, 1, 0, 1, 0, 1, 1, 1, 1]), 
                              array('I', [1, 0, 1, 0, 1, 1, 1])]
        py_cand_set = {0: set([0, 1, 5]), 1: set([2, 3, 4]), 2: set([0, 1, 5]), 3: set([2, 3, 4]), 4: set([2, 3, 4])}
        py_output_size = 100

        rec_list = db.debugblocker_topk_cython(py_config, lrecord_token_list, rrecord_token_list,
                        lrecord_index_list, rrecord_index_list, py_cand_set, py_output_size)

        expected_rec_list = [[0, 2, 13], [0, 3, 3], [0, 4, 6], [1, 0, 12], [1, 1, 11], 
                            [1, 5, 10], [2, 2, 9], [2, 3, 2], [2, 4, 7], [3, 0, 14], 
                            [3, 1, 15], [3, 5, 5], [4, 0, 1], [4, 1, 4], [4, 5, 8]]

        self.assertEqual(len(rec_list), len(expected_rec_list))

    def test_debugblocker_merge_topk_cython_1(self):
        rec_lists = []
        rec_list = db.debugblocker_merge_topk_cython(rec_lists);
        expected_rec_list = []
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_merge_topk_cython_2(self):
        rec_lists = [[[1, 2, 1]], [[1, 2, 2]], [[1, 2, 3]]]
        rec_list = db.debugblocker_merge_topk_cython(rec_lists);
        expected_rec_list = [(2, 1, 2)]
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_merge_topk_cython_3(self):
        rec_lists = [[[1, 2, 1], [2, 3, 2]], [[1, 2, 2], [2, 3, 3]], [[1, 2, 3],
            [2, 3, 4]]]
        rec_list = db.debugblocker_merge_topk_cython(rec_lists);
        expected_rec_list = [(2, 1, 2), (3, 2, 3)]
        self.assertEqual(rec_list, expected_rec_list)

    def test_debugblocker_merge_topk_cython_4(self):
        rec_lists = [[(1, 2, 1)], [(1, 2, 2)], [(1, 2, 3)]]
        rec_list = db.debugblocker_merge_topk_cython(rec_lists);
        expected_rec_list = [(2, 1, 2)]
        self.assertEqual(rec_list, expected_rec_list)

    @raises(AssertionError)
    def test_debugblocker_1(self):
        A = []
        B = []
        C = []
        db.debug_blocker(C, A, B)

    @raises(AssertionError)
    def test_debugblocker_2(self):
        A = read_csv_metadata(path_a)
        B = []
        C = []
        db.debug_blocker(C, A, B)

    @raises(AssertionError)
    def test_debugblocker_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = None
        db.debug_blocker(C, A, B)

    @raises(AssertionError)
    def test_debugblocker_4(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        output_size = '200'
        db.debug_blocker(C, A, B, output_size)

    @raises(AssertionError)
    def test_debugblocker_5(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        attr_corres = set()
        db.debug_blocker(C, A, B, 200, attr_corres)

    def test_debugblocker_6(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID',
                              fk_rtable='rtable_ID',
                              key='_id')
        attr_corres = []
        db.debug_blocker(C, A, B, 200, attr_corres)

    @raises(AssertionError)
    def test_debugblocker_7(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID',
                              fk_rtable='rtable_ID',
                              key='_id')
        attr_corres = [('ID', 'ID'), ['ID', 'ID']]
        db.debug_blocker(C, A, B, 200, attr_corres)

    @raises(AssertionError)
    def test_debugblocker_8(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID',
                              fk_rtable='rtable_ID',
                              key='_id')
        attr_corres = [('ID', 'ID')]
        verbose = 'true'
        db.debug_blocker(C, A, B, 200, attr_corres, verbose)

    @raises(AssertionError)
    def test_debugblocker_9(self):
        A = pd.DataFrame([])
        B = read_csv_metadata(path_b)
        C = pd.DataFrame([])
        db.debug_blocker(C, A, B)

    @raises(AssertionError)
    def test_debugblocker_10(self):
        A = read_csv_metadata(path_a)
        B = pd.DataFrame([])
        C = pd.DataFrame([])
        db.debug_blocker(C, A, B)

    @raises(AssertionError)
    def test_debugblocker_11(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = pd.DataFrame([])
        output_size = 0
        db.debug_blocker(C, A, B, output_size)

    @raises(AssertionError)
    def test_debugblocker_12(self):
        llist = [[0]]
        rlist = [[0]]
        ltable = pd.DataFrame(llist)
        rtable = pd.DataFrame(rlist)
        ltable.columns = ['ID']
        rtable.columns = ['ID']
        lkey = 'ID'
        rkey = 'ID'
        em.set_key(ltable, lkey)
        em.set_key(rtable, rkey)
        cand_set = pd.DataFrame([[0, 0, 0]])
        cand_set.columns = ['_id', 'ltable_ID', 'rtable_ID']
        cm.set_candset_properties(cand_set, '_id', 'ltable_ID',
                                  'rtable_ID', ltable, rtable)

        db.debug_blocker(cand_set, ltable, rtable)

    def test_debugblocker_13(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID', fk_rtable='rtable_ID',
                              key='_id')

        actual_ret_table = db.debug_blocker(C, A, B)
        test_file_path = os.sep.join(
            [debugblocker_datasets_path, 'test_debugblocker_13_out.csv'])
        expected_ret_table = read_csv_metadata(test_file_path,
                                               ltable=A, rtable=B,
                                               fk_ltable='ltable_ID',
                                               fk_rtable='rtable_ID',
                                               key='_id')
        self.assertEqual(len(expected_ret_table), len(actual_ret_table))

    def test_debugblocker_14(self):
        path_ltable = os.sep.join([debugblocker_datasets_path,
                                   'test_debugblocker_ltable.csv'])
        path_rtable = os.sep.join([debugblocker_datasets_path,
                                   'test_debugblocker_rtable.csv'])
        path_cand = os.sep.join([debugblocker_datasets_path,
                                   'test_debugblocker_cand.csv'])
        ltable = read_csv_metadata(path_ltable, key='ID')
        rtable = read_csv_metadata(path_rtable, key='book_id')
        cand_set = read_csv_metadata(path_cand, ltable=ltable, rtable=rtable,
                                      fk_ltable='ltable_ID',
                                      fk_rtable='rtable_book_id',
                                      key='_id')
        attr_corres = [('title', 'book_title'), ('price', 'price'),
                       ('desc', 'description'), ('genre', 'book_genre'),
                       ('year', 'pub_year'), ('lang', 'language'),
                       ('author', 'author'), ('publisher', 'publisher')]
        output_size = 1
        ret_dataframe = db.debug_blocker(cand_set, ltable, rtable,
                                         output_size, attr_corres)
        expected_columns = ['_id', 'ltable_ID', 'rtable_book_id',
                            'ltable_title', 'ltable_desc', 'ltable_year',
                            'ltable_lang', 'ltable_author', 'ltable_publisher',
                            'rtable_book_title', 'rtable_description',
                            'rtable_pub_year', 'rtable_language',
                            'rtable_author', 'rtable_publisher']
        self.assertEqual(list(ret_dataframe.columns), expected_columns)
        ret_record = list(ret_dataframe.loc[0])
        expected_record = [0, 1, 'B001', 'data analysis', 'introduction to data analysis',
            2015, 'ENG', 'Jane Doe', 'BCD publisher', 'introduction to data analysis', 
            float('nan'), 'English', 'introduction to data analysis', 'John Doe', 'ABC publisher10.00']
        print(ret_record)
        print(expected_record)
        self.assertEqual(expected_record[2], ret_record[2])
        self.assertEqual(expected_record[3], ret_record[3])

    @raises(AssertionError)
    def test_debugblocker_15(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID',
                              fk_rtable='rtable_ID',
                              key='_id')
        attr_corres = [('ID', 'ID'), ('birth_year', 'birth_year')]
        db.debug_blocker(C, A, B, 200, attr_corres)

    def test_debugblocker_16(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID', fk_rtable='rtable_ID',
                              key='_id')

        ret_table = db.debug_blocker(C, A, B, n_jobs = -1)

    def test_debugblocker_17(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID', fk_rtable='rtable_ID',
                              key='_id')

        ret_table = db.debug_blocker(C, A, B, n_jobs = -2)

    def test_debugblocker_18(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B,
                              fk_ltable='ltable_ID', fk_rtable='rtable_ID',
                              key='_id')

        ret_table = db.debug_blocker(C, A, B, n_jobs = 2)


def read_record_list(path):
    record_list = []
    f = open(path, 'r')
    for line in f:
        record_list.append(line.strip().split(' '))
    return record_list


def read_formatted_cand_set(path):
    cand_set = set()
    f = open(path, 'r')
    for line in f:
        pair = line.strip().split(' ')
        cand_set.add((int(pair[0]), int(pair[1])))
    return cand_set


def read_record_list(path):
    record_list = []
    f = open(path, 'r')
    for line in f:
        record_list.append(line.strip().split(' '))
    return record_list


def read_formatted_cand_set(path):
    cand_set = set()
    f = open(path, 'r')
    for line in f:
        pair = line.strip().split(' ')
        cand_set.add((int(pair[0]), int(pair[1])))
    return cand_set
