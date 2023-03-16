import os
# from nose.tools import *
import unittest
import pandas as pd
import numpy as np
from .utils import raises

from py_entitymatching.utils.generic_helper import get_install_path
import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.io.parsers import read_csv_metadata

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
catalog_datasets_path = os.sep.join([get_install_path(), 'tests',
                                     'test_datasets', 'catalog'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

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

    @raises(AssertionError)
    def test_is_property_present_for_df_invalid_df(self):
        cm.is_property_present_for_df(None, 'key')

    @raises(KeyError)
    def test_is_property_present_for_df_notin_catalog(self):
        A = pd.read_csv(path_a)
        cm.is_property_present_for_df(A, 'key')

    def test_catalog_len(self):
        A = read_csv_metadata(path_a)
        self.assertEqual(cm.get_catalog_len(), 1)

    def test_set_properties_valid_1(self):
        A = read_csv_metadata(path_a)
        p = cm.get_all_properties(A)
        B = pd.read_csv(path_b)
        cm.init_properties(B)
        cm.set_properties(B,p)
        self.assertEqual(cm.get_all_properties(B)==p, True)

    def test_set_properties_valid_2(self):
        A = read_csv_metadata(path_a)
        p = cm.get_all_properties(A)
        B = pd.read_csv(path_b)
        cm.set_properties(B,p)
        self.assertEqual(cm.get_all_properties(B)==p, True)

    @raises(AssertionError)
    def test_set_properties_invalid_df_1(self):
        cm.set_properties(None, {})

    @raises(AssertionError)
    def test_set_properties_invalid_dict_1(self):
        A = read_csv_metadata(path_a)
        cm.set_properties(A, None)


    def test_set_properties_df_notin_catalog_replace_false(self):
        A = read_csv_metadata(path_a)
        cm.set_properties(A, {}, replace=False)
        self.assertEqual(cm.get_key(A), 'ID')

    # def test_has_property_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     self.assertEqual(cm.has_property(A, 'key'), True)
    #
    # def test_has_property_valid_2(self):
    #     A = read_csv_metadata(path_a)
    #     self.assertEqual(cm.has_property(A, 'key1'), False)
    #
    # @raises(AssertionError)
    # def test_has_property_invalid_df(self):
    #     cm.has_property(None, 'key')
    #
    # @raises(AssertionError)
    # def test_has_property_invalid_prop_name(self):
    #     A = read_csv_metadata(path_a)
    #     cm.has_property(A, None)
    #
    # @raises(KeyError)
    # def test_has_property_df_notin_catalog(self):
    #     A = pd.read_csv(path_a)
    #     cm.has_property(A, 'key')

    def test_copy_properties_valid_1(self):
        A = read_csv_metadata(path_a)
        A1 = pd.read_csv(path_a)
        cm.copy_properties(A, A1)
        self.assertEqual(cm.is_dfinfo_present(A1), True)
        p = cm.get_all_properties(A)
        p1 = cm.get_all_properties(A1)
        self.assertEqual(p, p1)
        self.assertEqual(cm.get_key(A1), cm.get_key(A))

    def test_copy_properties_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)

        C1 = pd.read_csv(path_c)
        cm.copy_properties(C, C1)
        self.assertEqual(cm.is_dfinfo_present(C1), True)
        p = cm.get_all_properties(C1)
        p1 = cm.get_all_properties(C1)
        self.assertEqual(p, p1)
        self.assertEqual(cm.get_key(C1), cm.get_key(C))
        self.assertEqual(cm.get_ltable(C1).equals(A), True)
        self.assertEqual(cm.get_rtable(C1).equals(B), True)
        self.assertEqual(cm.get_fk_ltable(C1), cm.get_fk_ltable(C))
        self.assertEqual(cm.get_fk_rtable(C1), cm.get_fk_rtable(C))

    @raises(AssertionError)
    def test_copy_properties_invalid_tar_df(self):
        A = read_csv_metadata(path_a)
        cm.copy_properties(A, None)

    @raises(AssertionError)
    def test_copy_properties_invalid_src_df(self):
        A = read_csv_metadata(path_a)
        cm.copy_properties(None, A)

    def test_copy_properties_update_false_1(self):
        A = read_csv_metadata(path_a)
        A1 = read_csv_metadata(path_a)
        status=cm.copy_properties(A, A1, replace=False)
        self.assertEqual(status, False)

    def test_copy_properties_update_false_2(self):
        A = read_csv_metadata(path_a)
        A1 = pd.read_csv(path_a)
        cm.copy_properties(A, A1, replace=False)
        p = cm.get_all_properties(A)
        p1 = cm.get_all_properties(A1)
        self.assertEqual(p, p1)
        self.assertEqual(cm.get_key(A1), cm.get_key(A))

    @raises(KeyError)
    def test_copy_properties_src_df_notin_catalog(self):
        A = pd.read_csv(path_a)
        A1 = pd.read_csv(path_a)
        cm.copy_properties(A, A1)

    def test_get_key_valid(self):
        A = pd.read_csv(path_a)
        cm.set_key(A, 'ID')
        self.assertEqual(cm.get_key(A), 'ID')

    @raises(AssertionError)
    def test_get_key_invalid_df(self):
        cm.get_key(None)

    @raises(KeyError)
    def test_get_key_df_notin_catalog(self):
        A = pd.read_csv(path_a)
        cm.get_key(A)


    def test_set_key_valid(self):
        A = pd.read_csv(path_a)
        cm.set_key(A, 'ID')
        self.assertEqual(cm.get_key(A), 'ID')

    @raises(AssertionError)
    def test_set_key_invalid_df(self):
        cm.set_key(None, 'ID')

    @raises(KeyError)
    def test_set_key_notin_df(self):
        A = pd.read_csv(path_a)
        cm.set_key(A, 'ID1')

    def test_set_key_with_dupids(self):
        p = os.sep.join([catalog_datasets_path, 'A_dupid.csv'])
        A = pd.read_csv(p)
        status = cm.set_key(A, 'ID')
        self.assertEqual(status, False)

    def test_set_key_with_mvals(self):
        p = os.sep.join([catalog_datasets_path, 'A_mvals.csv'])
        A = pd.read_csv(p)
        status = cm.set_key(A, 'ID')
        self.assertEqual(status, False)

    def test_get_fk_ltable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        self.assertEqual(cm.get_fk_ltable(C), cm.get_property(C, 'fk_ltable'))
        self.assertEqual(cm.get_fk_ltable(C), 'ltable_ID')

    @raises(AssertionError)
    def test_get_fk_ltable_invalid_df(self):
        cm.get_fk_ltable(None)

    def test_get_fk_rtable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        self.assertEqual(cm.get_fk_rtable(C), cm.get_property(C, 'fk_rtable'))
        self.assertEqual(cm.get_fk_rtable(C), 'rtable_ID')

    @raises(AssertionError)
    def test_get_fk_rtable_invalid_df(self):
        cm.get_fk_rtable(None)


    def test_set_fk_ltable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = pd.read_csv(path_c)
        cm.set_fk_ltable(C, 'ltable_ID')
        self.assertEqual(cm.get_fk_ltable(C), 'ltable_ID')


    @raises(AssertionError)
    def test_set_fk_ltable_invalid_df(self):
        cm.set_fk_ltable(None, 'ltable_ID')

    @raises(KeyError)
    def test_set_fk_ltable_invalid_col(self):
        C = pd.read_csv(path_c)
        cm.set_fk_ltable(C, 'ltable_ID1')

    def test_set_fk_rtable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = pd.read_csv(path_c)
        cm.set_fk_rtable(C, 'rtable_ID')
        self.assertEqual(cm.get_fk_rtable(C), 'rtable_ID')


    @raises(AssertionError)
    def test_set_fk_rtable_invalid_df(self):
        cm.set_fk_rtable(None, 'rtable_ID')

    @raises(KeyError)
    def test_set_fk_rtable_invalid_col(self):
        C = pd.read_csv(path_c)
        cm.set_fk_rtable(C, 'rtable_ID1')

    def test_validate_and_set_fk_ltable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b)
        C = pd.read_csv(path_c)
        cm.validate_and_set_fk_ltable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(cm.get_fk_ltable(C), 'ltable_ID')

    def test_validate_and_set_fk_ltable_err_case_1(self):
        C = pd.read_csv(path_c)
        p = os.sep.join([catalog_datasets_path, 'A_dupid.csv'])
        A = pd.read_csv(p)
        status = cm.validate_and_set_fk_ltable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, False)
        self.assertEqual(cm.is_dfinfo_present(C), False)

    def test_validate_and_set_fk_ltable_err_case_2(self):
        C = pd.read_csv(path_c)
        p = os.sep.join([catalog_datasets_path, 'A_inv_fk.csv'])
        A = pd.read_csv(p)
        status = cm.validate_and_set_fk_ltable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, False)
        self.assertEqual(cm.is_dfinfo_present(C), False)


    def test_validate_and_set_fk_rtable_valid(self):
        A = read_csv_metadata(path_a)
        C = pd.read_csv(path_c)
        cm.validate_and_set_fk_rtable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(cm.get_fk_rtable(C), 'ltable_ID')

    def test_validate_and_set_fk_rtable_err_case_1(self):
        C = pd.read_csv(path_c)
        p = os.sep.join([catalog_datasets_path, 'A_dupid.csv'])
        A = pd.read_csv(p)
        status = cm.validate_and_set_fk_rtable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, False)
        self.assertEqual(cm.is_dfinfo_present(C), False)

    def test_validate_and_set_fk_rtable_err_case_2(self):
        C = pd.read_csv(path_c)
        p = os.sep.join([catalog_datasets_path, 'A_inv_fk.csv'])
        A = pd.read_csv(p)
        status = cm.validate_and_set_fk_rtable(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, False)
        self.assertEqual(cm.is_dfinfo_present(C), False)


    # def test_get_reqd_metadata_from_catalog_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_reqd_metadata_from_catalog(A, 'key')
    #     self.assertEqual(d['key'], cm.get_key(A))
    #
    # def test_get_reqd_metadata_from_catalog_valid_2(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_reqd_metadata_from_catalog(A, ['key'])
    #     self.assertEqual(d['key'], cm.get_key(A))
    #
    # def test_get_reqd_metadata_from_catalog_valid_3(self):
    #     A = read_csv_metadata(path_a)
    #     B = read_csv_metadata(path_b, key='ID')
    #     C = read_csv_metadata(path_c, ltable=A, rtable=B)
    #     d = cm.get_reqd_metadata_from_catalog(C, ['key', 'fk_ltable', 'fk_rtable', 'ltable', 'rtable'])
    #     self.assertEqual(d['key'], cm.get_key(C))
    #     self.assertEqual(d['fk_ltable'], cm.get_fk_ltable(C))
    #     self.assertEqual(d['fk_rtable'], cm.get_fk_rtable(C))
    #     self.assertEqual(cm.get_ltable(C).equals(A), True)
    #     self.assertEqual(cm.get_rtable(C).equals(B), True)
    #
    # @raises(AssertionError)
    # def test_get_reqd_metadata_from_catalog_err_1(self):
    #     cm.get_reqd_metadata_from_catalog(None, ['key'])
    #
    # @raises(AssertionError)
    # def test_get_reqd_metadata_from_catalog_err_2(self):
    #     A = read_csv_metadata(path_a)
    #     B = read_csv_metadata(path_b, key='ID')
    #     C = read_csv_metadata(path_c, ltable=A, rtable=B)
    #     d = cm.get_reqd_metadata_from_catalog(C, ['key', 'fk_ltable1', 'fk_rtable', 'ltable', 'rtable'])
    #
    #
    # def test_update_reqd_metadata_with_kwargs_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     metadata = {}
    #     cm._update_reqd_metadata_with_kwargs(metadata, d, ['key'])
    #     self.assertEqual(metadata['key'], d['key'])
    #
    # def test_update_reqd_metadata_with_kwargs_valid_2(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     metadata = {}
    #     cm._update_reqd_metadata_with_kwargs(metadata, d, 'key')
    #     self.assertEqual(metadata['key'], d['key'])
    #
    # @raises(AssertionError)
    # def test_update_reqf_metadata_with_kwargs_invalid_dict_1(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     cm._update_reqd_metadata_with_kwargs(None, d, 'key')
    #
    # @raises(AssertionError)
    # def test_update_reqf_metadata_with_kwargs_invalid_dict_2(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     cm._update_reqd_metadata_with_kwargs(d, None, 'key')
    #
    # @raises(AssertionError)
    # def test_update_reqd_metadata_with_kwargs_invalid_elts(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     metadata = {}
    #     cm._update_reqd_metadata_with_kwargs(metadata, d, ['key1'])


    # def test_get_diff_with_reqd_metadata_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     d1 = cm._get_diff_with_required_metadata(d, 'key1')
    #     self.assertEqual(len(d1), 1)
    #
    # def test_get_diff_with_reqd_metadata_valid_2(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     d1 = cm._get_diff_with_required_metadata(d, ['key1'])
    #     self.assertEqual(len(d1), 1)
    #
    # @raises(AssertionError)
    # def test_get_diff_with_reqd_metadata_invalid_dict(self):
    #     d1 = cm._get_diff_with_required_metadata(None, ['key1'])

    # def test_is_all_reqd_metadata_present_valid_1(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     self.assertEqual(cm.is_all_reqd_metadata_present(d, 'key'),True)
    #
    # def test_is_all_reqd_metadata_present_valid_2(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     self.assertEqual(cm.is_all_reqd_metadata_present(d, ['key']),True)
    #
    # def test_is_all_reqd_metadata_present_valid_3(self):
    #     A = read_csv_metadata(path_a)
    #     d = cm.get_all_properties(A)
    #     self.assertEqual(cm.is_all_reqd_metadata_present(d, ['key1']), False)
    #
    # @raises(AssertionError)
    # def test_is_all_reqd_metadata_present_invalid_dict(self):
    #     cm.is_all_reqd_metadata_present(None, 'key')


    def test_show_properties_for_df_valid_1(self):
        A = read_csv_metadata(path_a)
        cm.show_properties(A)


    def test_show_properties_for_df_valid_2(self):
        A = pd.read_csv(path_a)
        cm.show_properties(A)

    def test_show_properties_for_df_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        cm.show_properties(C)


    def test_show_properties_for_objid_valid_1(self):
        A = read_csv_metadata(path_a)
        cm.show_properties_for_id(id(A))

    @raises(KeyError)
    def test_show_properties_for_objid_err_1(self):
        A = pd.read_csv(path_a)
        cm.show_properties_for_id(id(A))

    def test_show_properties_for_objid_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        cm.show_properties_for_id(id(C))


    def test_validate_metadata_for_table_valid_1(self):
        A = pd.read_csv(path_a)
        status = cm._validate_metadata_for_table(A, 'ID', 'table', None, False)
        self.assertEqual(status, True)

    def test_validate_metadata_for_table_valid_2(self):
        import logging
        logger = logging.getLogger(__name__)
        A = pd.read_csv(path_a)
        status = cm._validate_metadata_for_table(A, 'ID', 'table', logger, True)
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_validate_metadata_for_table_invalid_df(self):
        status = cm._validate_metadata_for_table(None, 'ID', 'table', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_table_key_notin_catalog(self):
        A = pd.read_csv(path_a)
        status = cm._validate_metadata_for_table(A, 'ID1', 'table', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_table_key_notstring(self):
        A = pd.read_csv(path_a)
        status = cm._validate_metadata_for_table(A, None, 'table', None, False)

    @raises(AssertionError)
    def test_validate_metadata_for_table_key_notstring(self):
        A = pd.read_csv(path_a)
        status = cm._validate_metadata_for_table(A, 'zipcode', 'table', None, False)


    def test_validate_metadata_for_candset_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtable_ID', A, B, 'ID', 'ID', None, False)
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_validate_metadata_for_candset_invalid_df(self):
        status = cm._validate_metadata_for_candset(None, '_id', 'ltable_ID', 'rtable_ID', None, None,
                                                  'ID', 'ID', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_candset_id_notin(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        status = cm._validate_metadata_for_candset(C, 'id', 'ltable_ID', 'rtable_ID', A, B, 'ID', 'ID', None, False)


    @raises(KeyError)
    def test_validate_metadata_for_candset_fk_ltable_notin(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltableID', 'rtable_ID', A, B, 'ID', 'ID', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_candset_fk_rtable_notin(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtableID', A, B, 'ID', 'ID', None, False)

    @raises(AssertionError)
    def test_validate_metadata_for_candset_invlaid_ltable(self):
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtable_ID', None, B, 'ID', 'ID', None, False)

    @raises(AssertionError)
    def test_validate_metadata_for_candset_invlaid_rtable(self):
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtable_ID', B, None, 'ID', 'ID', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_candset_lkey_notin_ltable(self):
        A = pd.read_csv(path_a)
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtable_ID', A, B, 'ID1', 'ID', None, False)

    @raises(KeyError)
    def test_validate_metadata_for_candset_rkey_notin_rtable(self):
        A = pd.read_csv(path_a)
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = cm._validate_metadata_for_candset(C, '_id', 'ltable_ID', 'rtable_ID', A, B, 'ID', 'ID1', None, False)


    def test_get_keys_for_ltable_rtable_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_key, r_key = cm.get_keys_for_ltable_rtable(A, B, None, False)
        self.assertEqual(l_key, 'ID')
        self.assertEqual(r_key, 'ID')

    @raises(AssertionError)
    def test_get_keys_for_ltable_rtable_invalid_ltable(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_key, r_key = cm.get_keys_for_ltable_rtable(None, B, None, False)

    @raises(AssertionError)
    def test_get_keys_for_ltable_rtable_invalid_rtable(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        l_key, r_key = cm.get_keys_for_ltable_rtable(A, None, None, False)


    def test_get_metadata_for_candset_valid(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(C, None, False)
        self.assertEqual(key, '_id')
        self.assertEqual(fk_ltable, 'ltable_ID')
        self.assertEqual(fk_rtable, 'rtable_ID')
        self.assertEqual(l_key, 'ID')
        self.assertEqual(r_key, 'ID')
        self.assertEqual(ltable.equals(A), True)
        self.assertEqual(rtable.equals(B), True)

    @raises(AssertionError)
    def test_get_metadata_for_candset_invalid_df(self):
        cm.get_metadata_for_candset(None, None, False)

    #--- catalog ---
    def test_catalog_singleton_isinstance(self):
        from py_entitymatching.catalog.catalog import Singleton
        x = Singleton(object)
        x.__instancecheck__(object)

    @raises(TypeError)
    def test_catalog_singleton_call(self):
        from py_entitymatching.catalog.catalog import Singleton
        x = Singleton(object)
        x.__call__()


    # -- catalog helper --
    def test_check_attrs_present_valid_1(self):
        A = pd.read_csv(path_a)
        status = ch.check_attrs_present(A, 'ID')
        self.assertEqual(status, True)

    def test_check_attrs_present_valid_2(self):
        A = pd.read_csv(path_a)
        status = ch.check_attrs_present(A, ['ID'])
        self.assertEqual(status, True)

    def test_check_attrs_present_valid_3(self):
        A = pd.read_csv(path_a)
        status = ch.check_attrs_present(A, ['_ID'])
        self.assertEqual(status, False)

    @raises(AssertionError)
    def test_check_attrs_present_invalid_df(self):
        ch.check_attrs_present(None, 'ID')

    def test_check_attrs_invalid_None(self):
        A = pd.read_csv(path_a)
        status = ch.check_attrs_present(A, None)
        self.assertEqual(status, False)


    @raises(AssertionError)
    def test_are_all_attrs_present_invalid_df(self):
        ch.are_all_attrs_in_df(None, 'id')

    def test_are_all_attrs_present_invalid_None(self):
        A = pd.read_csv(path_a)
        status = ch.are_all_attrs_in_df(A, None)
        self.assertEqual(status, False)

    def test_is_attr_unique_valid_1(self):
        A = pd.read_csv(path_a)
        status = ch.is_attr_unique(A, 'ID')
        self.assertEqual(status, True)

    def test_is_attr_unique_valid_2(self):
        A = pd.read_csv(path_a)
        status = ch.is_attr_unique(A, 'zipcode')
        self.assertEqual(status, False)

    @raises(AssertionError)
    def test_is_attr_unique_invalid_df(self):
        ch.is_attr_unique(None, 'zipcode')

    @raises(AssertionError)
    def test_is_attr_unique_invalid_attr(self):
        A = pd.read_csv(path_a)
        ch.is_attr_unique(A, None)

    def test_does_contain_missing_values_valid_1(self):
        A = pd.read_csv(path_a)
        status = ch.does_contain_missing_vals(A, 'ID')
        self.assertEqual(status, False)

    def test_does_contain_missing_values_valid_2(self):
        p = os.sep.join([catalog_datasets_path, 'A_mvals.csv'])
        A = pd.read_csv(p)
        status = ch.does_contain_missing_vals(A, 'ID')
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_does_contain_missing_values_invalid_df(self):
        ch.does_contain_missing_vals(None, 'zipcode')

    @raises(AssertionError)
    def test_does_invalid_attr(self):
        A = pd.read_csv(path_a)
        ch.does_contain_missing_vals(A, None)


    def test_is_key_attribute_valid_1(self):
        A = pd.read_csv(path_a)
        status = ch.is_key_attribute(A, 'ID', True)
        self.assertEqual(status, True)

    def test_is_key_attribute_valid_2(self):
        A = pd.read_csv(path_a)
        status = ch.is_key_attribute(A, 'zipcode', True)
        self.assertEqual(status, False)

    def test_is_key_attribute_valid_3(self):
        p = os.sep.join([catalog_datasets_path, 'A_mvals.csv'])
        A = pd.read_csv(p)
        status = ch.is_key_attribute(A, 'ID', True)
        self.assertEqual(status, False)

    def test_is_key_attribute_valid_4(self):
        A = pd.DataFrame(columns=['id', 'name'])
        status = ch.is_key_attribute(A, 'id')
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_is_key_attribute_invalid_df(self):
        ch.is_key_attribute(None, 'id')

    @raises(AssertionError)
    def test_is_key_attribute_invalid_attr(self):
        A = pd.read_csv(path_a)
        ch.is_key_attribute(A, None)


    def test_check_fk_constraint_valid_1(self):
        A = pd.read_csv(path_a)
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = ch.check_fk_constraint(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, True)
        status = ch.check_fk_constraint(C, 'rtable_ID', B, 'ID')
        self.assertEqual(status, True)

    @raises(AssertionError)
    def test_check_fk_constraint_invalid_foreign_df(self):
        ch.check_fk_constraint(None, 'rtable_ID', pd.DataFrame(), 'ID')

    @raises(AssertionError)
    def test_check_fk_constraint_invalid_base_df(self):
        ch.check_fk_constraint(pd.DataFrame(), 'rtable_ID', None, 'ID')

    @raises(AssertionError)
    def test_check_fk_constraint_invalid_base_attr(self):
        ch.check_fk_constraint(pd.DataFrame(), 'rtable_ID', pd.DataFrame(), None)

    @raises(AssertionError)
    def test_check_fk_constraint_invalid_foreign_attr(self):
        ch.check_fk_constraint(pd.DataFrame(), None, pd.DataFrame(), 'ID')

    def test_check_fk_constraint_invalid_attr_notin(self):
        A = pd.read_csv(path_a)
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        status = ch.check_fk_constraint(C, 'ltable_ID', A, 'ID1')
        self.assertEqual(status, False)

    def test_check_fk_constraint_invalid_attr_mval(self):
        A = pd.read_csv(path_a)
        B = pd.read_csv(path_b)
        C = pd.read_csv(path_c)
        C.loc[0, 'ltable_ID'] = np.NaN
        status = ch.check_fk_constraint(C, 'ltable_ID', A, 'ID')
        self.assertEqual(status, False)


    def test_does_contain_rows_valid_1(self):
        A = pd.read_csv(path_a)
        status = ch.does_contain_rows(A)
        self.assertEqual(status, True)

    def test_does_contain_rows_valid_2(self):
        A = pd.DataFrame()
        status = ch.does_contain_rows(A)
        self.assertEqual(status, False)

    @raises(AssertionError)
    def test_does_contain_rows_invalid(self):
        ch.does_contain_rows(None)


    def test_get_name_for_key_valid_1(self):
        A = pd.read_csv(path_a)
        ch.add_key_column(A, '_id')
        s = ch.get_name_for_key(A)
        self.assertEqual(s, '_id0')

    def test_get_name_for_key_valid_2(self):
        A = pd.read_csv(path_a)
        s = ch.get_name_for_key(A, 'ID')
        self.assertEqual(s, 'ID0')

    @raises(AssertionError)
    def test_add_key_column_invalid_df(self):
        ch.add_key_column(None, 'id')

    @raises(AssertionError)
    def test_add_key_column_invalid_attr(self):
        ch.add_key_column(pd.DataFrame(), None)
