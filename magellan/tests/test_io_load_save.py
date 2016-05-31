# coding=utf-8
from __future__ import unicode_literals

import os
import unittest
import pandas as pd
from nose.tools import *

from magellan.io.pickles import load_object, load_table, save_object, save_table
from magellan.io.parsers import read_csv_metadata
from magellan.blocker.rule_based_blocker import RuleBasedBlocker
from magellan.feature.autofeaturegen import get_features_for_blocking
from magellan.utils.generic_helper import get_install_path, del_files_in_dir, creat_dir_ifnot_exists
import magellan.core.catalog_manager as cm

io_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'io'])
path_a = os.sep.join([io_datasets_path, 'A.csv'])
path_b = os.sep.join([io_datasets_path, 'B.csv'])
path_c = os.sep.join([io_datasets_path, 'C.csv'])
sndbx_path = os.sep.join([os.sep.join([get_install_path(), 'datasets', 'test_datasets']), 'sandbox'])


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
        feature_table = get_features_for_blocking(A, B)
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

