import os
from nose.tools import *
import unittest
import pandas as pd

from magellan.utils.generic_helper import get_install_path
import magellan.core.catalog_manager as cm
from magellan.io.parsers import read_csv_metadata

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
core_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'core'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([core_datasets_path, 'C.csv'])

class CatalogManagerTestCases(unittest.TestCase):
    def setUp(self):
        cm.del_catalog()

    def tearDown(self):
        cm.del_catalog()

    def test_get_property_valid_df_name_1(self):
        # cm.del_catalog()
        df = read_csv_metadata(path_a)
        self.assertEqual(cm.get_property(df, 'key'), 'ID')
        # cm.del_catalog()

    def test_get_property_valid_df_name_2(self):
        # cm.del_catalog()
        self.assertEqual(cm.get_catalog_len(), 0)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        self.assertEqual(cm.get_property(C, 'key'), '_id')
        self.assertEqual(cm.get_property(C, 'fk_ltable'), 'ltable_ID')
        self.assertEqual(cm.get_property(C, 'fk_rtable'), 'rtable_ID')
        self.assertEqual(cm.get_property(C, 'ltable').equals(A), True)
        self.assertEqual(cm.get_property(C, 'rtable').equals(B), True)
        # cm.del_catalog()

    @raises(AssertionError)
    def test_get_property_invalid_df_1(self):
        cm.get_property(10, 'key')

    @raises(AssertionError)
    def test_get_property_invalid_path_1(self):
        # cm.del_catalog()
        A = read_csv_metadata(path_a)
        cm.get_property(A, None)
        # cm.del_catalog()

    @raises(KeyError)
    def test_get_property_df_notin_catalog(self):
        # cm.del_catalog()
        A = pd.read_csv(path_a)
        cm.get_property(A, 'key')
        # cm.del_catalog()

    def test_set_property_valid_df_name_value(self):
        # cm.del_catalog()
        df = pd.read_csv(path_a)
        cm.set_property(df, 'key', 'ID')
        self.assertEqual(cm.get_property(df, 'key'), 'ID')
        # cm.del_catalog()

    @raises(AssertionError)
    def test_set_property_invalid_df(self):
        # cm.del_catalog()
        cm.set_property(None, 'key', 'ID')
        # cm.del_catalog()

    @raises(AssertionError)
    def test_set_property_valid_df_invalid_prop(self):
        # cm.del_catalog()
        A = pd.read_csv(path_a)
        cm.set_property(A, None, 'ID')
        # cm.del_catalog()


    def test_init_properties_valid(self):
        # cm.del_catalog()
        A = pd.read_csv(path_a)
        cm.init_properties(A)
        self.assertEqual(cm.is_dfinfo_present(A), True)
        # cm.del_catalog()

    @raises(AssertionError)
    def test_init_properties_invalid_df(self):
        cm.init_properties(None)


    def test_get_all_properties_valid_1(self):
        # cm.del_catalog()
        A = read_csv_metadata(path_a)
        m = cm.get_all_properties(A)
        self.assertEqual(len(m), 1)
        self.assertEqual(m['key'], 'ID')
        # cm.del_catalog()


    def test_get_all_properties_valid_2(self):
        # cm.del_catalog()
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        m = cm.get_all_properties(C)
        self.assertEqual(len(m), 5)
        self.assertEqual(m['key'], '_id')
        self.assertEqual(m['fk_ltable'], 'ltable_ID')
        self.assertEqual(m['fk_rtable'], 'rtable_ID')
        self.assertEqual(m['ltable'].equals(A), True)
        self.assertEqual(m['rtable'].equals(B), True)
        # cm.del_catalog()


    @raises(AssertionError)
    def test_get_all_properties_invalid_df_1(self):
        # cm.del_catalog()
        C = cm.get_all_properties(None)

    @raises(KeyError)
    def test_get_all_properties_invalid_df_2(self):
        # cm.del_catalog()
        A = pd.read_csv(path_a)
        C = cm.get_all_properties(A)


    def test_del_property_valid_df_name(self):
        A = read_csv_metadata(path_a)
        cm.del_property(A, 'key')
        self.assertEqual(len(cm.get_all_properties(A)), 0)

    @raises(AssertionError)
    def test_del_property_invalid_df(self):
        cm.del_property(None, 'key')

    @raises(AssertionError)
    def test_del_property_invalid_property(self):
        A = read_csv_metadata(path_a)
        cm.del_property(A, None)

    @raises(KeyError)
    def test_del_property_df_notin_catalog(self):
        A = pd.read_csv(path_a)
        cm.del_property(A, 'key')

    @raises(KeyError)
    def test_del_property_prop_notin_catalog(self):
        A = read_csv_metadata(path_a)
        cm.del_property(A, 'key1')



    def test_del_all_properties_valid_1(self):
        A = read_csv_metadata(path_a)
        cm.del_all_properties(A)
        self.assertEqual(cm.is_dfinfo_present(A), False)


    def test_del_all_properties_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        cm.del_all_properties(C)
        self.assertEqual(cm.is_dfinfo_present(C), False)

    @raises(AssertionError)
    def test_del_all_properties_invalid_df(self):
        cm.del_all_properties(None)


    @raises(KeyError)
    def test_del_all_properties_df_notin_catalog(self):
        A = pd.read_csv(path_a)
        cm.del_all_properties(A)


    def test_get_catalog_valid(self):
        A = read_csv_metadata(path_a)
        cg = cm.get_catalog()
        self.assertEqual(len(cg), 1)

    def test_del_catalog_valid(self):
        A = read_csv_metadata(path_a)
        cm.del_catalog()
        cg = cm.get_catalog()
        self.assertEqual(len(cg), 0)

    def test_is_catalog_empty(self):
        A = read_csv_metadata(path_a)
        cm.del_catalog()
        self.assertEqual(cm.is_catalog_empty(), True)

    def test_is_dfinfo_present_valid_1(self):
        A = read_csv_metadata(path_a)
        status = cm.is_dfinfo_present(A)
        self.assertEqual(status, True)

    def test_is_dfinfo_present_valid_2(self):
        A = pd.read_csv(path_a)
        status = cm.is_dfinfo_present(A)
        self.assertEqual(status, False)

    @raises(AssertionError)
    def test_is_dfinfo_present_invalid(self):
        cm.is_dfinfo_present(None)

    def test_is_property_present_for_df_valid_1(self):
        A = read_csv_metadata(path_a)
        status = cm.is_property_present_for_df(A, 'key')
        self.assertEqual(status, True)

    def test_is_property_present_for_df_valid_2(self):
        A = read_csv_metadata(path_a)
        status = cm.is_property_present_for_df(A, 'key1')
        self.assertEqual(status, False)


# import os
# from nose.tools import *
#
# import magellan as mg
# p = mg.get_install_path()
# path_for_A = os.sep.join([p, 'datasets', 'test_datasets', 'A.csv'])
# path_for_A_dup = os.sep.join([p, 'datasets', 'test_datasets', 'A_dupid.csv'])
# path_for_A_mvals = os.sep.join([p, 'datasets', 'test_datasets', 'table_A_mvals.csv'])
#
#
# def test_set_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
#
# def test_get_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# @raises(AttributeError)
# def test_get_property_invalid_no_df():
#     mg.get_property(None, 'key')
#
#
# def test_reset_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID1')
#     assert_equal(mg.get_property(df, 'key'), 'ID1')
#
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# def test_set_key_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(mg.get_key(df), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# def test_set_key_invalid_dup():
#     df = mg.read_csv_metadata(path_for_A_dup)
#     status = mg.set_key(df, 'ID')
#     assert_equal(status, False)
#
# def test_set_key_invalid_mv():
#     df = mg.read_csv_metadata(path_for_A_dup)
#     status = mg.set_key(df, 'ID')
#     assert_equal(status, False)
#
# def test_get_all_properties_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(len(mg.get_all_properties(df)), 1)
#     mg.del_catalog()
#
# @raises(KeyError)
# def test_get_all_properties_invalid():
#     df = mg.read_csv_metadata(path_for_A)
#     assert_equal(len(mg.get_all_properties(df)), 1)
#
#
# def test_get_catalog_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(len(mg.get_all_properties(df)), 1)
#     c = mg.get_catalog()
#     assert_equal(len(c), 1)
#     mg.del_catalog()
#
# def test_del_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(mg.get_key(df), 'ID')
#     mg.del_property(df, 'key')
#     assert_equal(mg.is_property_present_for_df(df, 'key'), False)
#     mg.del_catalog()
#
#
# def test_del_property_invalid():
#     pass
#
#
# def test_del_all_properties_valid():
#     pass
#
#
# def test_del_all_properties_invalid():
#     pass
#
#
# def test_del_catalog_valid():
#     pass
