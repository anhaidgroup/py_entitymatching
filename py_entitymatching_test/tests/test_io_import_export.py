# coding=utf-8
from __future__ import unicode_literals

import os
import unittest
import pandas as pd
from nose.tools import raises

from py_entitymatching.io.parsers import read_csv_metadata, to_csv_metadata, _get_metadata_from_file
from py_entitymatching.utils.generic_helper import get_install_path, del_files_in_dir, creat_dir_ifnot_exists
import py_entitymatching.catalog.catalog_manager as cm
datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
io_datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets',
                                'io'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
sndbx_path = os.sep.join([os.sep.join([get_install_path(), 'tests',
                                       'test_datasets']), 'sandbox'])

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
        self.assertEqual(cm.is_property_present_for_df(IM, 'key'), False)

    def test_valid_path_wi_invalidmetadata_key_dups(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_dupid.csv'])
        IM = read_csv_metadata(p, key='ID')
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.is_property_present_for_df(IM, 'key'), False)

    def test_valid_path_wi_invalidmetadata_replace_key(self):
        cm.del_catalog()
        p = os.sep.join([io_datasets_path, 'A_key_zipcode.csv'])
        IM = read_csv_metadata(p, key='ID')
        self.assertEqual(cm.is_dfinfo_present(IM), True)
        self.assertEqual(cm.is_property_present_for_df(IM, 'key'), True)

    def test_valid_path_candset_with_diff_metadataextn_1(self):
        cm.del_catalog()
        path_a = os.sep.join([io_datasets_path, 'A.csv'])
        A = read_csv_metadata(path_a, metadata_extn='mdx')
        pd_A = pd.read_csv(path_a)
        self.assertEqual(A.equals(pd_A), True)
        self.assertEqual(cm.get_key(A), 'ID')

    def test_valid_path_candset_with_diff_metadataextn_2(self):
        cm.del_catalog()
        path_a = os.sep.join([io_datasets_path, 'A.csv'])
        A = read_csv_metadata(path_a, metadata_extn='.mdx')
        pd_A = pd.read_csv(path_a)
        self.assertEqual(A.equals(pd_A), True)
        self.assertEqual(cm.get_key(A), 'ID')

    @raises(KeyError)
    def test_validpath_metadata_set_to_none_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a, key=None)
        self.assertEqual(cm.is_dfinfo_present(A), True)
        cm.get_key(A)

        # self.assertEqual(cm.get_key(A1), cm.get_key(A), 'The keys in the catalog are not same')

    @raises(AssertionError)
    def test_valid_path_df_metadata_set_to_none_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        path_c = os.sep.join([io_datasets_path, 'C_partialmeta.csv'])

        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable=None)


    def test_valid_path_df_metadata_split_betn_file_kw(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        path_c = os.sep.join([io_datasets_path, 'C_partialmeta.csv'])
        C = read_csv_metadata(path_c, ltable=A, rtable=B, fk_ltable='ltable_ID')


    @raises(AssertionError)
    def test_valid_path_df_metadata_invalid_ltable(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        # path_c = os.sep.join([io_datasets_path, 'C_partialmeta.csv'])

        C = read_csv_metadata(path_c, ltable="temp", rtable=B)

        # p = os.sep.join([sndbx_path, 'C_saved.csv'])
        # creat_dir_ifnot_exists(sndbx_path)
        # to_csv_metadata(C, p)
        #
        # C1 = read_csv_metadata(p, ltable=10, rtable=B)

        # self.assertEqual(cm.get_all_properties(C1), cm.get_all_properties(C), 'The properties in the '
        #                                                                           'catalog are not same')




    @raises(AssertionError)
    def test_valid_path_df_metadata_invalid_rtable(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        # path_c = os.sep.join([io_datasets_path, 'C_partialmeta.csv'])

        C = read_csv_metadata(path_c, rtable="temp", ltable=A)

    def test_valid_path_type_is_not_string(self):
        cm.del_catalog()
        with self.assertRaises(AssertionError) as ctx:
            read_csv_metadata(1001)

        actual = str(ctx.exception)
        expected = 'Input file path: 1001 \nis not of type string'
        self.assertEqual(actual, expected)



class ToCSVMetadataTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_invalid_df_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        p = os.sep.join([sndbx_path, 'A_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(10, p)

    @raises(AssertionError)
    def test_invalid_df_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        p = os.sep.join([sndbx_path, 'A_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(None, p)


    @raises(AssertionError)
    def test_invalid_path_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        to_csv_metadata(A, 10)

    @raises(AssertionError)
    def test_invalid_path_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        to_csv_metadata(A, None)

    @raises(AssertionError)
    def test_invalid_path_df(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(None, None)

    def test_valid_path_df_chk_metadatafile_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)

        p = os.sep.join([sndbx_path, 'A_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(A, p)

        p_meta_1=os.sep.join([sndbx_path, 'A_saved.metadata'])
        m1 = _get_metadata_from_file(p_meta_1)

        p_meta_2=os.sep.join([io_datasets_path, 'expected_A.metadata'])
        m2 = _get_metadata_from_file(p_meta_2)

        self.assertEqual(m1, m2, 'The metadata information is not same.')


    def test_valid_path_df_chk_metadatafile_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)

        p = os.sep.join([sndbx_path, 'C_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(C, p)

        p_meta_1=os.sep.join([sndbx_path, 'C_saved.metadata'])
        m1 = _get_metadata_from_file(p_meta_1)

        p_meta_2=os.sep.join([io_datasets_path, 'expected_C.metadata'])
        m2 = _get_metadata_from_file(p_meta_2)

        self.assertEqual(m1, m2, 'The metadata information is not same.')

    def test_valid_path_df_chk_metadatafile_3(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)

        p = os.sep.join([sndbx_path, 'A_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(A, p, metadata_extn='mdx')

        p_meta_1=os.sep.join([sndbx_path, 'A_saved.mdx'])
        m1 = _get_metadata_from_file(p_meta_1)

        p_meta_2=os.sep.join([io_datasets_path, 'expected_A.metadata'])
        m2 = _get_metadata_from_file(p_meta_2)

        self.assertEqual(m1, m2, 'The metadata information is not same.')


    def test_valid_path_df_chk_metadatafile_4(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)

        p = os.sep.join([sndbx_path, 'C_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(C, p, metadata_extn='.mdx')

        p_meta_1=os.sep.join([sndbx_path, 'C_saved.mdx'])
        m1 = _get_metadata_from_file(p_meta_1)

        p_meta_2=os.sep.join([io_datasets_path, 'expected_C.metadata'])
        m2 = _get_metadata_from_file(p_meta_2)

        self.assertEqual(m1, m2, 'The metadata information is not same.')


    def test_valid_path_df_chk_catalog_1(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)

        p = os.sep.join([sndbx_path, 'A_saved.csv'])

        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(A, p)

        A1 = read_csv_metadata(p)

        self.assertEqual(cm.get_key(A1), cm.get_key(A), 'The keys in the catalog are not same')

    def test_valid_path_df_chk_catalog_2(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')

        C = read_csv_metadata(path_c, ltable=A, rtable=B)

        p = os.sep.join([sndbx_path, 'C_saved.csv'])
        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(C, p)

        C1 = read_csv_metadata(p, ltable=A, rtable=B)

        self.assertEqual(cm.get_all_properties(C1), cm.get_all_properties(C), 'The properties in the '
                                                                                  'catalog are not same')

    def test_valid_path_df_overwrite(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)

        p = os.sep.join([sndbx_path, 'A_saved.csv'])

        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(A, p)
        to_csv_metadata(A, p)

        A1 = read_csv_metadata(p)

        self.assertEqual(cm.get_key(A1), cm.get_key(A), 'The keys in the catalog are not same')


    @raises(AssertionError)
    def test_invalid_path_cannotwrite(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)

        p = os.sep.join([sndbx_path, 'temp', 'A_saved.csv'])

        creat_dir_ifnot_exists(sndbx_path)
        to_csv_metadata(A, p)

    def test_valid_path_type_is_string(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)
        A = read_csv_metadata(path_a)
        with self.assertRaises(AssertionError) as ctx:
            to_csv_metadata(A, 1001)

        actual = str(ctx.exception)
        expected = 'Input file path: 1001 \nis not of type string'
        self.assertEqual(actual, expected)

    def test_invalid_data_frame_type(self):
        cm.del_catalog()
        del_files_in_dir(sndbx_path)

        p = os.sep.join([sndbx_path, 'temp', 'A_saved.csv'])
        with self.assertRaises(AssertionError) as ctx:
            to_csv_metadata(1001, p)

        actual = str(ctx.exception)
        expected = 'Input object: 1001 \nis not of type pandas dataframe'
        self.assertEqual(actual, expected)















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

