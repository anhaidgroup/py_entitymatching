import os
from nose.tools import *
import unittest
import pandas as pd

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata

from py_entitymatching.feature.extractfeatures import extract_feature_vecs
from py_entitymatching.feature.autofeaturegen import get_features_for_matching
import py_entitymatching.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class ExtractFeaturesTestCases(unittest.TestCase):
    def test_extract_feature_vecs_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'], feature_table=feature_table,
                                 attrs_after='label')
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1], 'label')
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_2(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'], feature_table=feature_table)
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', False)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_with_default_value_for_n_jobs(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'], feature_table=feature_table, n_jobs=1)
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', False)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_with_parralel_job_count_more_than_one(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'], feature_table=feature_table, n_jobs=2)
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', False)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_with_parralel_job_count_less_than_zero(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'], feature_table=feature_table, n_jobs=-1)
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', False)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_3(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name'],
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after='label')
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_4(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name', 'ltable_ID', 'rtable_ID'],
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after='label')
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_5(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name', 'ltable_ID', 'rtable_ID', '_id'],
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after='label')
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_6(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before=['ltable_name', 'rtable_name', 'ltable_ID', 'rtable_ID', '_id'],
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after=['label', '_id'])
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[4], 'rtable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_7(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before='ltable_name',
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after=['label', '_id'])
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    def test_extract_feature_vecs_valid_8(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C,
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after=['label', '_id'])
        self.assertEqual(isinstance(F, pd.DataFrame), True)
        self.assertEqual(F.columns[0], '_id')
        self.assertEqual(F.columns[1], cm.get_fk_ltable(C))
        self.assertEqual(F.columns[2], cm.get_fk_rtable(C))
        # self.assertEqual(F.columns[3], 'ltable_name')
        self.assertEqual(F.columns[len(F.columns) - 1] == 'label', True)
        self.assertEqual(cm.get_all_properties(C) == cm.get_all_properties(F), True)

    @raises(AssertionError)
    def test_extract_feature_vecs_invalid_df(self):
        F = extract_feature_vecs(None, attrs_before='ltable_name',
                                 feature_table=pd.DataFrame(),
                                 attrs_after=['label', '_id'])

    @raises(AssertionError)
    def test_extract_feature_vecs_invalid_attrs_before(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before='ltable_name1',
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after=['label', '_id'])

    @raises(AssertionError)
    def test_extract_feature_vecs_invalid_attrs_after(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before='ltable_name',
                                 feature_table=pd.DataFrame(columns=feature_table.columns),
                                 attrs_after=['label1', '_id'])

    @raises(AssertionError)
    def test_extract_feature_vecs_invalid_feature_table(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        col_pos = len(C.columns)
        C.insert(col_pos, 'label', [0] * len(C))
        feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        F = extract_feature_vecs(C, attrs_before='ltable_name',
                                 feature_table=None,
                                 attrs_after=['label', '_id'])
