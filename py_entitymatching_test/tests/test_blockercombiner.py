# coding=utf-8
import os
from nose.tools import *
import unittest
import pandas as pd
import six

from py_entitymatching.utils.generic_helper import get_install_path
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.io.parsers import read_csv_metadata, to_csv_metadata
from py_entitymatching.blockercombiner.blockercombiner import combine_blocker_outputs_via_union

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
bc_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets',
                                'blockercombiner'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
path_c1 = os.sep.join([bc_datasets_path, 'C1.csv'])
path_c2 = os.sep.join([bc_datasets_path, 'C2.csv'])
path_c3 = os.sep.join([bc_datasets_path, 'C3.csv'])

class BlockerCombinerTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_blocker_combiner_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(path_c1, ltable=A, rtable=B)
        C2 = read_csv_metadata(path_c2, ltable=A, rtable=B)
        C3 = read_csv_metadata(path_c3, ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C2, C3])
        C_exp = read_csv_metadata(path_c, ltable=A, rtable=B)
        # try:
        #     C_exp.sort_values(['ltable_ID', 'rtable_ID'], inplace=True)
        # except AttributeError:
        #     C_exp.sort(['ltable_ID', 'rtable_ID'], inplace=True)
        # to_csv_metadata(C_exp, path_c)


        C_exp.reset_index(inplace=True, drop=True)
        C_exp['_id'] = six.moves.range(0, len(C_exp))
        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)

    def test_blocker_combiner_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C1_ex_1.csv']), ltable=A, rtable=B)
        C2 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C2_ex_1.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C2])

        C_exp = read_csv_metadata(os.sep.join([bc_datasets_path, 'C_ex_1.csv']), ltable=A, rtable=B)
        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)


    def test_blocker_combiner_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C1_ex_1.csv']), ltable=A, rtable=B)
        C2 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C3_ex_2.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C2])

        C_exp = read_csv_metadata(os.sep.join([bc_datasets_path, 'C_ex_2.csv']), ltable=A, rtable=B)

        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)


    def test_blocker_combiner_valid_4(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C1_ex_1.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C1])
        # try:
        #     C1.sort_values(['ltable_ID', 'rtable_ID'], inplace=True)
        # except AttributeError:
        #     C1.sort(['ltable_ID', 'rtable_ID'], inplace=True)
        # to_csv_metadata(C1, os.sep.join([bc_datasets_path, 'C1_ex_1.csv']))

        C1.reset_index(inplace=True, drop=True)
        C1['_id'] = six.moves.range(0, len(C1))

        if os.name != 'nt':
            self.assertEqual(C.equals(C1), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C1)
        self.assertEqual(p1, p2)

    def test_blocker_combiner_valid_5(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C3_ex_2.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C1])
        self.assertEqual(len(C), 0)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C1)
        self.assertEqual(p1, p2)

    def test_blocker_combiner_valid_6(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_1.csv']), ltable=A, rtable=B)
        C2 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_2.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C2], 'l_', 'r_')

        C_exp = read_csv_metadata(os.sep.join([bc_datasets_path, 'C_ex_4.csv']), ltable=A, rtable=B)
        # try:
        #     C_exp.sort_values(['l_ID', 'r_ID'], inplace=True)
        # except AttributeError:
        #     C_exp.sort(['l_ID', 'r_ID'], inplace=True)
        C_exp.reset_index(inplace=True, drop=True)
        C_exp['_id'] = six.moves.range(0, len(C_exp))

        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)

    def test_blocker_combiner_valid_7(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_1.csv']), ltable=A, rtable=B)
        C1.rename(columns={'r_address':'address'}, inplace=True)
        C2 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_2.csv']), ltable=A, rtable=B)
        C = combine_blocker_outputs_via_union([C1, C2], 'l_', 'r_')
        C_exp = read_csv_metadata(os.sep.join([bc_datasets_path, 'C_ex_4.csv']), ltable=A, rtable=B)
        # C_exp.sort_values(['l_ID', 'r_ID'], inplace=True)
        # C_exp.reset_index(inplace=True, drop=True)
        # C_exp['_id'] = six.moves.range(0, len(C_exp))
        C_exp.drop('r_address', axis=1, inplace=True)
        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)


    def test_blocker_combiner_valid_8(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_1.csv']), ltable=A, rtable=B)
        C1.rename(columns={'l_ID':'ltable_ID'}, inplace=True)
        C1.rename(columns={'r_ID':'rtable_ID'}, inplace=True)
        cm.set_fk_ltable(C1, 'ltable_ID')
        cm.set_fk_rtable(C1, 'rtable_ID')
        C2 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C4_ex_2.csv']), ltable=A, rtable=B)
        C2.rename(columns={'l_ID':'ltable_ID'}, inplace=True)
        C2.rename(columns={'r_ID':'rtable_ID'}, inplace=True)
        cm.set_fk_ltable(C2, 'ltable_ID')
        cm.set_fk_rtable(C2, 'rtable_ID')

        C = combine_blocker_outputs_via_union([C1, C2], 'l_', 'r_')
        C_exp = read_csv_metadata(os.sep.join([bc_datasets_path, 'C_ex_4.csv']), ltable=A, rtable=B)
        C_exp.rename(columns={'l_ID':'ltable_ID'}, inplace=True)
        C_exp.rename(columns={'r_ID':'rtable_ID'}, inplace=True)
        cm.set_fk_ltable(C_exp, 'ltable_ID')
        cm.set_fk_rtable(C_exp, 'rtable_ID')

        # C_exp.sort_values(['l_ID', 'r_ID'], inplace=True)
        # C_exp.reset_index(inplace=True, drop=True)
        # C_exp['_id'] = six.moves.range(0, len(C_exp))
        # C_exp.drop('r_address', axis=1, inplace=True)
        if os.name != 'nt':
            self.assertEqual(C.equals(C_exp), True)
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(C_exp)
        self.assertEqual(p1, p2)


    @raises(AssertionError)
    def test_blocker_combiner_invalid_df(self):
        combine_blocker_outputs_via_union([10, 10])


    @raises(AssertionError)
    def test_blocker_combiner_invalid_lprefix(self):
        combine_blocker_outputs_via_union([pd.DataFrame()], None, 'rtable_')

    @raises(AssertionError)
    def test_blocker_combiner_invalid_rprefix(self):
        combine_blocker_outputs_via_union([pd.DataFrame()], 'ltable_', None)
