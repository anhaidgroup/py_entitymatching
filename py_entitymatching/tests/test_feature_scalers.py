import os
from nose.tools import *
import unittest

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.feature.extractfeatures import extract_feature_vecs
from py_entitymatching.feature.autofeaturegen import get_features_for_matching
from py_entitymatching.feature.scalers import scale_vectors, scale_features

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
A = read_csv_metadata(path_a)
B = read_csv_metadata(path_b, key='ID')
C = read_csv_metadata(path_c, ltable=A, rtable=B)
feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
F = extract_feature_vecs(C,
                         attrs_before=['_id', 'ltable_ID', 'rtable_ID'],
                         feature_table=feature_table)

class ScalersTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_scale_features_invalid_data_type(self):
        scale_features(F.values)

    @raises(AssertionError)
    def test_scale_features_invalid_data_entry(self):
        scale_features(F)

    @raises(AssertionError)
    def test_scale_features_invalid_method(self):
        scale_features(F,
                       exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                       scaling_method='Weird_Scaler')

    @raises(AssertionError)
    def test_scale_features_invalid_method(self):
        _, scaler = scale_features(F,
                                   exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                                   scaling_method='Weird_Scaler')
        scale_features(F,
                       exclude_attrs=['ltable_ID', 'rtable_ID'],
                       scaler=scaler)

    def test_scale_features_valid_method(self):
        x, scaler = scale_features(F,
                                   exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                                   scaling_method='MinMax')
        y, _ = scale_features(F,
                              exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
                              scaler=scaler)
        self.assertEqual(x.iat[5, 5], y.iat[5, 5])
