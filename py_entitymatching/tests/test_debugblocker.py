import os
from nose.tools import *
import unittest
# import numpy as np
import pandas as pd

import py_entitymatching as em
from py_entitymatching.utils.generic_helper import get_install_path
import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.io.parsers import read_csv_metadata
import py_entitymatching.debugblocker.debugblocker as db


datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
catalog_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'catalog'])
debugblocker_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets', 'debugblocker'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

class DebugblockerTestCases(unittest.TestCase):
    def test_calc_threshold_1(self):
        index = 0
        length = 5
        thres = db._calc_threshold(index, length)
        self.assertEqual(thres, 1.0)

    def test_calc_threshold_2(self):
        index = 5
        length = 5
        thres = db._calc_threshold(index, length)
        self.assertEqual(thres, 0.0)

    def test_jaccard_sim_1(self):
        lset = set()
        rset = set()
        self.assertEqual(db._jaccard_sim(lset, rset), 0.0)

    def test_jaccard_sim_2(self):
        lset = {'hello'}
        rset = set()
        self.assertEqual(db._jaccard_sim(lset, rset), 0.0)

    def test_jaccard_sim_3(self):
        lset = set([])
        rset = {'hello'}
        self.assertEqual(db._jaccard_sim(lset, rset), 0.0)

    def test_jaccard_sim_4(self):
        lset = {'hello', 'this'}
        rset = {'hello'}
        self.assertEqual(db._jaccard_sim(lset, rset), 0.5)

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

        corrres_list = db._get_field_correspondence_list(
            A, B, A_key, B_key, attr_corres)
        expected_list = [('Id', 'Id'), ('ISBN', 'ISBN')]
        self.assertEqual(corrres_list, expected_list)

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

    def test_assemble_topk_table_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        topk_heap = []
        ret_dataframe = db._assemble_topk_table(topk_heap, A, B)
        self.assertEqual(len(ret_dataframe), 0)
        self.assertEqual(list(ret_dataframe.columns), [])

    def test_assemble_topk_table_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        topk_heap = [(0.2727272727272727, 1, 0), (0.23076923076923078, 0, 4),
                     (0.16666666666666666, 0, 3)]
        ret_dataframe = db._assemble_topk_table(topk_heap, A, B)
        expected_columns = ['_id','similarity', 'ltable_ID', 'rtable_ID',
                            'ltable_name', 'ltable_birth_year',
                            'ltable_hourly_wage',
                            'ltable_address', 'ltable_zipcode', 'rtable_name',
                            'rtable_birth_year', 'rtable_hourly_wage',
                            'rtable_address', 'rtable_zipcode']
        self.assertEqual(len(ret_dataframe), 3)
        self.assertEqual(list(ret_dataframe.columns), expected_columns)

        expected_recs = [[0, 0.27272727272727271, 'a2', 'b1', 'Michael Franklin',
                          1988, 27.5, '1652 Stockton St, San Francisco',
                          94122, 'Mark Levene', 1987, 29.5,
                          '108 Clement St, San Francisco', 94107],
                         [1, 0.23076923076923078, 'a1', 'b5', 'Kevin Smith',
                          1989, 30.0, '607 From St, San Francisco', 94107,
                          'Alfons Kemper', 1984, 35.0,
                          '170 Post St, Apt 4,  San Francisco', 94122],
                         [2, 0.16666666666666666, 'a1', 'b4', 'Kevin Smith',
                          1989, 30.0, '607 From St, San Francisco', 94107,
                          'Joseph Kuan', 1982, 26.0,
                          '108 South Park, San Francisco', 94122]]
        self.assertEqual(list(ret_dataframe.ix[0]), expected_recs[0])
        self.assertEqual(list(ret_dataframe.ix[1]), expected_recs[1])
        self.assertEqual(list(ret_dataframe.ix[2]), expected_recs[2])

    def test_index_candidate_set_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)

        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, r_key)

        expected_cand_set = {(0, 1), (1, 2), (3, 2), (0, 0), (3, 3),
                             (4, 4), (1, 4), (2, 0), (1, 3), (0, 5),
                             (2, 1), (4, 3), (4, 2), (2, 5), (3, 4)}
        actual_cand_set = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(expected_cand_set, actual_cand_set)

    @raises(AssertionError)
    def test_index_candidate_set_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)
        C.ix[0, 'ltable_ID'] = 'aaaa'

        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, r_key)

        db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)

    @raises(AssertionError)
    def test_index_candidate_set_3(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        l_key = cm.get_key(A)
        r_key = cm.get_key(B)
        C.ix[0, 'rtable_ID'] = 'bbbb'

        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, l_key)
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, r_key)

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
        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, 'ID')
        expected_cand_set = {(0, 1), (1, 0)}
        actuacl_cand_set = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(expected_cand_set, actuacl_cand_set)

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
        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, 'ID')
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
        lrecord_id_to_index_map = db._get_record_id_to_index_map(A, 'ID')
        rrecord_id_to_index_map = db._get_record_id_to_index_map(B, 'ID')
        new_C = db._index_candidate_set(C,
                lrecord_id_to_index_map, rrecord_id_to_index_map, False)
        self.assertEqual(new_C, set())

    def test_generate_prefix_events_impl_1(self):
        record_list = []
        prefix_events = []
        db._generate_prefix_events_impl(record_list, prefix_events, 0)
        self.assertEqual(prefix_events, [])

        record_list = [[], [], []]
        prefix_events = []
        db._generate_prefix_events_impl(record_list, prefix_events, 0)
        self.assertEqual(prefix_events, [])

    def test_generate_prefix_events_impl_2(self):
        record_list = [['a1', 'a2'], [], ['c1']]
        table_indicator = 0
        actual_prefix_events = []
        db._generate_prefix_events_impl(
            record_list, actual_prefix_events, table_indicator)
        expected_prefix_events = [(-1.0, 0, 0, 0, 'a1'),
                                  (-0.5, 0, 0, 1, 'a2'), (-1.0, 0, 2, 0, 'c1')]
        self.assertEqual(actual_prefix_events, expected_prefix_events)

    def test_generate_prefix_events_1(self):
        lrecord_list = []
        rrecord_list = []
        prefix_events = db._generate_prefix_events(lrecord_list, rrecord_list)
        self.assertEqual(prefix_events, [])

        lrecord_list = [[], [], []]
        rrecord_list = [[], []]
        prefix_events = db._generate_prefix_events(lrecord_list, rrecord_list)
        self.assertEqual(prefix_events, [])

    def test_generate_prefix_events_2(self):
        lrecord_list = [['a1', 'a2'], [], ['c1', 'c2', 'c3']]
        rrecord_list = [[], ['b1', 'b2'], ['c1']]
        actual_prefix_events = db._generate_prefix_events(
            lrecord_list, rrecord_list)
        expected_prefix_events = [(-1.0, 0, 0, 0, 'a1'),
                                  (-1.0, 1, 2, 0, 'c1'),
                                  (-1.0, 0, 2, 0, 'c1'),
                                  (-0.6666666666666667, 0, 2, 1, 'c2'),
                                  (-0.33333333333333337, 0, 2, 2, 'c3'),
                                  (-1.0, 1, 1, 0, 'b1'),
                                  (-0.5, 1, 1, 1, 'b2'),
                                  (-0.5, 0, 0, 1, 'a2')]
        self.assertEqual(actual_prefix_events, expected_prefix_events)

    def test_replace_nan_to_empty_1(self):
        field = pd.np.nan
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

    def test_build_global_token_order_1(self):
        record_list = []
        actual_dict = {}
        expected_dict = {}
        db._build_global_token_order(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

        record_list = [[], [], []]
        actual_dict = {}
        expected_dict = {}
        db._build_global_token_order(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

    def test_build_global_token_order_2(self):
        record_list = [['c', 'b', 'a'], [], ['b', 'c'], ['c', 'c']]
        actual_dict = {}
        expected_dict = {'a': 1, 'c': 4, 'b': 2}
        db._build_global_token_order(record_list, actual_dict)
        self.assertEqual(actual_dict, expected_dict)

    def test_sort_record_tokens_by_global_order_1(self):
        record_list = []
        order_dict = {}
        db._build_global_token_order(record_list, order_dict)
        db._sort_record_tokens_by_global_order(record_list, order_dict)
        expected_out_list = []
        self.assertEqual(record_list, expected_out_list)

        record_list = [[], [], []]
        order_dict = {}
        db._build_global_token_order(record_list, order_dict)
        db._sort_record_tokens_by_global_order(record_list, order_dict)
        expected_out_list = [[], [], []]
        self.assertEqual(record_list, expected_out_list)

    def test_sort_record_tokens_by_global_order_2(self):
        record_list = [['c', 'b', 'a'], [], ['c', 'b'], ['c', 'c']]
        order_dict = {}
        db._build_global_token_order(record_list, order_dict)
        db._sort_record_tokens_by_global_order(record_list, order_dict)
        expected_out_list = [['a', 'b', 'c'], [], ['b', 'c'], ['c', 'c']]
        self.assertEqual(record_list, expected_out_list)

    def test_sort_record_tokens_by_global_order_3(self):
        record_list = [['c', 'b', 'a'], [], ['c', 'b'], ['c', 'c']]
        order_dict = {}
        db._build_global_token_order(record_list, order_dict)
        record_list[1] = ['d', 'e', 'c']
        db._sort_record_tokens_by_global_order(record_list, order_dict)
        expected_out_list = [['a', 'b', 'c'], ['c'], ['b', 'c'], ['c', 'c']]
        self.assertEqual(record_list, expected_out_list)

    def test_get_record_id_to_index_map_1(self):
        A = read_csv_metadata(path_a, key='ID')
        key = em.get_key(A)
        actual_rec_id_to_idx = db._get_record_id_to_index_map(A, key)
        expected_rec_id_to_idx = {'a1': 0, 'a3': 2, 'a2': 1, 'a5': 4, 'a4': 3}
        self.assertEqual(actual_rec_id_to_idx, expected_rec_id_to_idx)

    @raises(AssertionError)
    def test_get_record_id_to_index_map_2(self):
        table = [['a1', 'hello'], ['a1', 'world']]
        key = 'ID'
        dataframe = pd.DataFrame(table)
        dataframe.columns = ['ID', 'title']
        em.set_key(dataframe, key)
        db._get_record_id_to_index_map(dataframe, key)

    def test_get_tokenized_column_1(self):
        column = []
        actual_ret_column = db._get_tokenized_column(column)
        expected_ret_column = []
        self.assertEqual(actual_ret_column, expected_ret_column)

    def test_get_tokenized_column_2(self):
        column = ['hello world', pd.np.nan, 'how are you',
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

        expected_record_list = []
        test_file_path = os.sep.join([debugblocker_datasets_path, 'test_get_tokenized_table_1.txt'])
        f = open(test_file_path, 'r')
        for line in f:
            expected_record_list.append(line.strip().split(' '))

        self.assertEqual(actual_record_list, expected_record_list)

    def test_get_tokenized_table_2(self):
        B = read_csv_metadata(path_b, key='ID')
        B_key = em.get_key(B)
        feature_list = [0, 1, 3]
        actual_record_list = db._get_tokenized_table(B, B_key, feature_list)

        expected_record_list = []
        test_file_path = os.sep.join([debugblocker_datasets_path, 'test_get_tokenized_table_2.txt'])
        f = open(test_file_path, 'r')
        for line in f:
            expected_record_list.append(line.strip().split(' '))

        self.assertEqual(actual_record_list, expected_record_list)

    def test_get_tokenized_table_3(self):
        table = [[1, 'abc abc asdf', '123-3456-7890', pd.np.nan, '',
                  '135 east  abc  st'],
                 [2, 'aaa bbb', '000-111-2222', '', '', '246  west abc st'],
                 [3, 'cc dd', '123-123-1231', 'cc', 'unknown', ' 246 west def st']]
        dataframe = pd.DataFrame(table)
        dataframe.columns = ['ID', 'name', 'phone', 'department', 'school', 'address']
        key = 'ID'
        em.set_key(dataframe, key)
        feature_list = [1, 3, 4, 5]
        actual_record_list = db._get_tokenized_table(dataframe, key, feature_list)
        expected_record_list = [['abc', 'abc_1', 'asdf', '135', 'east', 'abc_2', 'st'],
                                ['aaa', 'bbb', '246', 'west', 'abc', 'st'],
                                ['cc', 'dd', 'cc_1', 'unknown', '246', 'west', 'def', 'st']]
        self.assertEqual(actual_record_list, expected_record_list)

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
        corres_list = [(0, 0), (1, 1), (4, 4), (5, 5)]
        A_filtered, B_filtered = db._get_filtered_table(
            A, B, A_key, B_key, corres_list)
        A_wlist = db._get_feature_weight(A_filtered)
        expected_A_wlist = [2.0, 2.0, 2.0, 1.4]
        self.assertEqual(A_wlist, expected_A_wlist)

        B_wlist = db._get_feature_weight(B_filtered)
        expected_B_wlist = [2.0, 2.0, 2.0, 1.3333333333333333]
        self.assertEqual(B_wlist, expected_B_wlist)

    def test_get_feature_weight_3(self):
        table = [[''], [pd.np.nan]]
        dataframe = pd.DataFrame(table)
        weight_list = db._get_feature_weight(dataframe)
        self.assertEqual(weight_list, [0.0])

    def test_select_features_1(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        actual_selected_features = db._select_features(A, B, A_key)
        expected_selected_features = [1, 3, 4]
        self.assertEqual(actual_selected_features, expected_selected_features)

    def test_select_features_2(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        corres_list = [(0, 0), (1, 1), (4, 4)]
        A_filtered, B_filtered = db._get_filtered_table(A, B, A_key, B_key, corres_list)
        actual_selected_features = db._select_features(
            A_filtered, B_filtered, A_key)
        expected_selected_features = [1, 2]
        self.assertEqual(actual_selected_features, expected_selected_features)

    def test_select_features_3(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        B_key = em.get_key(B)
        corres_list = [(0, 0)]
        A_filtered, B_filtered = db._get_filtered_table(A, B, A_key, B_key, corres_list)
        actual_selected_features = db._select_features(
            A_filtered, B_filtered, A_key)
        expected_selected_features = []
        self.assertEqual(actual_selected_features, expected_selected_features)

    @raises(AssertionError)
    def test_select_features_4(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        A_field_set = [0, 1, 2]
        B_field_set = [0, 1, 2, 3]

        A_filtered = A[A_field_set]
        B_filtered = B[B_field_set]
        db._select_features(
            A_filtered, B_filtered, A_key)

    @raises(AssertionError)
    def test_select_features_5(self):
        A = read_csv_metadata(path_a, key='ID')
        B = read_csv_metadata(path_b, key='ID')
        A_key = em.get_key(A)
        A_field_set = [0, 1, 2, 3]
        B_field_set = [0, 1, 2]

        A_filtered = A[A_field_set]
        B_filtered = B[B_field_set]
        db._select_features(
            A_filtered, B_filtered, A_key)

    def test_topk_sim_join_1(self):
        ltable_path = os.sep.join([debugblocker_datasets_path, 'test_topk_sim_join_1_A.txt'])
        lrecord_list = read_record_list(ltable_path)
        rtable_path = os.sep.join([debugblocker_datasets_path, 'test_topk_sim_join_1_B.txt'])
        rrecord_list = read_record_list(rtable_path)
        cand_path = os.sep.join([debugblocker_datasets_path, 'test_topk_sim_join_1_C.txt'])
        cand_set = read_formatted_cand_set(cand_path)

        actual_topk_heap = db._topk_sim_join(lrecord_list, rrecord_list, cand_set, 100)
        expected_topk_heap = [(0.1, 0, 3),(0.15789473684210525, 0, 2),(0.1, 2, 3),
                              (0.15789473684210525, 1, 1), (0.15789473684210525, 2, 2),
                              (0.14285714285714285, 0, 4),(0.1, 4, 0),
                              (0.2222222222222222, 3, 1),(0.15789473684210525, 1, 5),
                              (0.2222222222222222, 3, 0),(0.15789473684210525, 4, 5),
                              (0.2222222222222222, 3, 5),(0.14285714285714285, 2, 4),
                              (0.15789473684210525, 1, 0),(0.1, 4, 1)]
        self.assertEqual(actual_topk_heap, expected_topk_heap)

    def test_topk_sim_join_2(self):
        lrecord_list = [['asdf', 'fdsa']]
        rrecord_list = [['aa', 'bb']]
        cand_set = {(0, 0)}
        actual_topk_heap = db._topk_sim_join(lrecord_list, rrecord_list, cand_set, 100)
        expected_topk_heap = []
        self.assertEqual(actual_topk_heap, expected_topk_heap)

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
        expected_columns = ['_id', 'similarity', 'ltable_ID', 'rtable_book_id',
                            'ltable_title', 'ltable_desc', 'ltable_year',
                            'ltable_lang', 'ltable_author', 'ltable_publisher',
                            'rtable_book_title', 'rtable_description',
                            'rtable_pub_year', 'rtable_language',
                            'rtable_author', 'rtable_publisher']
        self.assertEqual(list(ret_dataframe.columns), expected_columns)
        ret_record = list(ret_dataframe.ix[0])
        expected_record = [0, 0.33333333333333331, 2, 'B002',
                           'Thinking in Java',
                           'learn how to program in Java', 2000, 'ENG',
                           'Johnnie Doe', pd.np.nan, 'Thinking in C',
                           'learn programming in C++', '1990', pd.np.nan,
                           'Jane Doe', 'BCD publisher']
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