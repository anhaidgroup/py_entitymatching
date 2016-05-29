# coding=utf-8
from __future__ import unicode_literals

import os
import unittest
import pandas as pd
from nose.tools import *

from magellan.io.parsers import read_csv_metadata, to_csv_metadata
from magellan.utils.generic_helper import get_install_path
import magellan.core.catalog_manager as cm

io_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'io'])
path_a = os.sep.join([io_datasets_path, 'A.csv'])
path_b = os.sep.join([io_datasets_path, 'B.csv'])
path_c = os.sep.join([io_datasets_path, 'C.csv'])

class ReadCSVMetadataTestCases(unittest.TestCase):
    def test_valid_path_wi_valid_metadata(self):
        cm.del_catalog()
        A = read_csv_metadata(path_a)
        pd_A = pd.read_csv(path_a)
        self.assertEqual(A.equals(pd_A), True)
        self.assertEqual(cm.get_key(A), 'ID')

    def test_valid_path_candset_wi_valid_metadata(self):
        cm.del_catalog()
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID') # not initializing with ID will raise key_error
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        pd_C = pd.read_csv(path_c)
        self.assertEqual(C.equals(pd_C), True)
        self.assertEqual(len(cm.get_all_properties(C).keys()), 5)
        self.assertEqual(cm.get_key(C), '_id')
        self.assertEqual(cm.get_fk_ltable(C), 'ltable_ID')
        self.assertEqual(cm.get_fk_rtable(C), 'rtable_ID')


    @raises(AssertionError)
    def test_invalid_str_path(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'xyz.csv'])
        A = read_csv_metadata(p)

    @raises(AssertionError)
    def test_invalid_nonstr_path(self):
        cm.del_catalog()
        A = read_csv_metadata(10)

    @raises(AssertionError)
    def test_valid_path_wi_invalidmetadata_wrongformat(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_md_wrongformat.csv'])
        IM = read_csv_metadata(p, key='ID')


    def test_valid_path_wo_metadata(self):
        cm.del_catalog()
        B = read_csv_metadata(path_b)
        pd_B = pd.read_csv(path_b)
        self.assertEqual(B.equals(pd_B), True)
        self.assertEqual(cm.is_dfinfo_present(B), True)

    def test_valid_path_wi_metadata_unknownprop(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'InvalidMetadata1.csv'])
        IM = read_csv_metadata(p)
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.get_property(IM, 'key1'), 'ID')

    @raises(KeyError)
    def test_valid_path_wi_invalidmetadata_wrongkey(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'InvalidMetadata2.csv'])
        IM = read_csv_metadata(p)

    def test_valid_path_wi_invalidmetadata_key_mvals(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_mvals.csv'])
        IM = read_csv_metadata(p, key='ID')
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.has_property(IM, 'key'), False)

    def test_valid_path_wi_invalidmetadata_key_dups(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_dupid.csv'])
        IM = read_csv_metadata(p, key='ID')
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.has_property(IM, 'key'), False)

    def test_valid_path_wi_invalidmetadata_replace_key(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_key_zipcode.csv'])
        IM = read_csv_metadata(p, key='ID')
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.has_property(IM, 'key'), True)


class WriteCSVMetadataTestCases(unittest.TestCase):
    pass






















# import export test cases

# import
# # read csv with valid path
# # read csv with invalid path
# # # read csv with string but invalid path
# # # read csv with number

# # read csv with metadata file
# # # valid contents in metadata file
# # # invalid contents in metadata file

# # read csv with metadata information given in the command
# # # valid metadata given
# # # # single table
# # # # cand. set
# # # invalid metadata given
# # # # partial metadata
# # # # invalid metadata name
# # # # invalid column name (not in the dataframe)
# # # # invalid metadata: key
# # # # invalid ltable fk ltable
# # # # invalid rtable fk rtable
# # # parameters in command should take precendence
# # # # metadata has key, give a different key in parameter
# # # # metadata has fk_ltable, give a different fk_ltable in parameter
# # # # metadata has fk_rtable, give a different fk_rtable in parameter
# # # # metadata has ltable, give a different ltable in parameter
# # # # metadata has rtable, give a different rtable in parameter




# export
# invalid input:
# # type: df, path

