# coding=utf-8
from __future__ import unicode_literals

import os
import unittest
import pandas as pd
from nose.tools import *

from py_entitymatching.io.pickles import load_object, load_table, save_object, save_table
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.blocker.rule_based_blocker import RuleBasedBlocker
from py_entitymatching.feature.autofeaturegen import get_features_for_blocking
from py_entitymatching.utils.generic_helper import get_install_path, del_files_in_dir, creat_dir_ifnot_exists
import py_entitymatching.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
sndbx_path = os.sep.join([os.sep.join([get_install_path(), 'tests',
                                       'test_datasets']), 'sandbox'])


class SaveObjectTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_invalid_path_1(self):
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        save_object(p, 10)

    @raises(AssertionError)
    def test_invalid_path_2(self):
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        save_object(p, None)


    def test_valid_object_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)

        A1 = load_object(p)
        self.assertEqual(A.equals(A1), True)

    def test_valid_object_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_blocking(A, B, validate_inferred_attr_types=False)
        rb = RuleBasedBlocker()
        rb.add_rule('zipcode_zipcode_exm(ltuple, rtuple) != 1', feature_table)
        C = rb.block_tables(A, B, show_progress=False)
        self.assertEqual(len(C), 15)
        p = os.sep.join([sndbx_path, 'C.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(rb, p)

        rb1 = load_object(p)
        C1 = rb1.block_tables(A, B, show_progress=False)
        self.assertEqual(C.equals(C1), True)

    def test_valid_object_overwrite(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)
        save_object(A, p)

        A1 = load_object(p)
        self.assertEqual(A.equals(A1), True)

    @raises(AssertionError)
    def test_invalid_path_cannotwrite(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'temp', 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)

class LoadObjectTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_invalid_path_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)

        A1 = load_object(10)
        # self.assertEqual(A.equals(A1), True)

    @raises(AssertionError)
    def test_invalid_path_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)
        p1 = os.sep.join([sndbx_path, 'temp', 'A_saved.pkl'])
        A1 = load_object(p1)

    def test_valid_object_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A_saved.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(A, p)

        A1 = load_object(p)
        self.assertEqual(A.equals(A1), True)

    def test_valid_object_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        feature_table = get_features_for_blocking(A, B, validate_inferred_attr_types=False)
        rb = RuleBasedBlocker()
        rb.add_rule('zipcode_zipcode_exm(ltuple, rtuple) != 1', feature_table)
        C = rb.block_tables(A, B, show_progress=False)
        self.assertEqual(len(C), 15)
        p = os.sep.join([sndbx_path, 'C.pkl'])
        creat_dir_ifnot_exists(sndbx_path)
        save_object(rb, p)

        rb1 = load_object(p)
        C1 = rb1.block_tables(A, B, show_progress=False)
        self.assertEqual(C.equals(C1), True)

class SaveTableTestCases(unittest.TestCase):
    def test_valid_path_table_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p)

        A1 = load_table(p)
        self.assertEqual(A.equals(A), True)
        self.assertEqual(cm.get_key(A), cm.get_key(A1))

    def test_valid_path_table_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        p = os.sep.join([sndbx_path, 'C.pkl'])
        save_table(C, p)

        C1 = load_table(p)
        self.assertEqual(C.equals(C1), True)
        self.assertEqual(cm.get_key(C), cm.get_key(C1))
        # self.assertEqual(cm.get_ltable(C).equals(cm.get_ltable(C1)), True)
        # self.assertEqual(cm.get_rtable(C).equals(cm.get_rtable(C1)), True)
        self.assertEqual(cm.get_fk_ltable(C), cm.get_fk_ltable(C1))
        self.assertEqual(cm.get_fk_rtable(C), cm.get_fk_rtable(C1))

    def test_valid_path_table_overwrite(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p)
        save_table(A, p)

        A1 = load_table(p)
        self.assertEqual(A.equals(A), True)
        self.assertEqual(cm.get_key(A), cm.get_key(A1))


    @raises(AssertionError)
    def test_invalid_table_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        # A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(10, p)

    @raises(AssertionError)
    def test_invalid_table_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        # A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(None, p)


    @raises(AssertionError)
    def test_invalid_path_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        # p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, 10)

    @raises(AssertionError)
    def test_invalid_path_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        # p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, None)

    @raises(AssertionError)
    def test_invalid_path_3(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'temp', 'A.pkl'])
        save_table(A, p)

    @raises(AssertionError)
    def test_invalid_metadataextn(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p, metadata_ext=10)

    @raises(AssertionError)
    def test_invalid_path_table(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        save_table(None, None)


    def test_valid_path_table_3(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p, metadata_ext='pkll')

        A1 = load_table(p, metadata_ext='pkll')
        self.assertEqual(A.equals(A), True)
        self.assertEqual(cm.get_key(A), cm.get_key(A1))

class LoadTableTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_invalid_metadataextn(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p)
        A1 = load_table(p, metadata_ext=10)

    @raises(AssertionError)
    def test_invalid_path(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p)
        A1 = load_table(None)

    def test_valid_path_table_3(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p, metadata_ext='pkll')

        A1 = load_table(p, metadata_ext='pklll')
        self.assertEqual(A.equals(A), True)

    def test_valid_path_table_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        p = os.sep.join([sndbx_path, 'A.pkl'])
        save_table(A, p)

        A1 = load_table(p)
        self.assertEqual(A.equals(A), True)
        self.assertEqual(cm.get_key(A), cm.get_key(A1))

    def test_valid_path_table_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        p = os.sep.join([sndbx_path, 'C.pkl'])
        save_table(C, p)

        C1 = load_table(p)
        self.assertEqual(C.equals(C1), True)
        self.assertEqual(cm.get_key(C), cm.get_key(C1))
        # self.assertEqual(cm.get_ltable(C).equals(cm.get_ltable(C1)), True)
        # self.assertEqual(cm.get_rtable(C).equals(cm.get_rtable(C1)), True)
        self.assertEqual(cm.get_fk_ltable(C), cm.get_fk_ltable(C1))
        self.assertEqual(cm.get_fk_rtable(C), cm.get_fk_rtable(C1))
