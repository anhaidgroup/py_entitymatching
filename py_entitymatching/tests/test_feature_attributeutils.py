import os
from nose.tools import *
import unittest
import pandas as pd
import six

from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.feature.simfunctions import get_sim_funs_for_matching
from py_entitymatching.feature.tokenizers import get_tokenizers_for_matching
from py_entitymatching.feature.autofeaturegen import get_features_for_matching
from py_entitymatching.feature.attributeutils import get_attrs_to_project, \
    get_attr_corres, get_attr_types, _get_type, _len_handle_nan

import py_entitymatching.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])


class AttributeUtilsTestCases(unittest.TestCase):
    def test_get_attrs_to_project_types_valid(self):
        A = read_csv_metadata(path_a)
        attrs = get_attrs_to_project(table=A)

    @raises(AssertionError)
    def test_get_attrs_to_project_types_invalid(self):
        A = read_csv_metadata(path_a)
        attrs = get_attrs_to_project(table=A.values)

    def test_get_attrs_to_project_exclude_None(self):
        A = read_csv_metadata(path_a)
        attrs = get_attrs_to_project(table=A, exclude_attrs=None)
        self.assertEqual(attrs, list(A.columns))

    @raises(AssertionError)
    def test_get_attrs_to_project_invalid_exclude_attrs(self):
        A = read_csv_metadata(path_a)
        exclude_attrs = ['Weird_ID']
        attrs = get_attrs_to_project(table=A, exclude_attrs=exclude_attrs)

    def test_get_attrs_to_project_exclude_attrs(self):
        A = read_csv_metadata(path_a)
        exclude_attrs = ['name']
        attrs = get_attrs_to_project(table=A, exclude_attrs=exclude_attrs)
        self.assertEqual(set(attrs), set(A.columns).difference(set(exclude_attrs)))

    @raises(AssertionError)
    def test_get_attrs_to_project_invalid_target_attr(self):
        A = read_csv_metadata(path_a)
        target = 'Weird_target'
        attrs = get_attrs_to_project(table=A, target_attr=target)

    def test_get_attrs_to_project_target_attr(self):
        A = read_csv_metadata(path_a)
        target = 'address'
        attrs = get_attrs_to_project(table=A, target_attr=target)
        self.assertEqual(set(attrs), set(A.columns).difference(set([target])))

    def test_get_attr_types_valid(self):
        A = read_csv_metadata(path_a)
        x = get_attr_types(A)

    @raises(AssertionError)
    def test_get_attr_types_invalid_df(self):
        x = get_attr_types(None)

    def test_get_attr_corres_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        ac = get_attr_corres(A, B)
        for c in ac['corres']:
            self.assertEqual(c[0], c[1])

        self.assertEqual(all(ac['ltable'] == A), True)
        self.assertEqual(all(ac['rtable'] == B), True)

    def test_get_attr_corres_valid_2(self):
        A = read_csv_metadata(path_a)
        A['label'] = 0
        B = read_csv_metadata(path_b, key='ID')
        ac = get_attr_corres(A, B)
        for c in ac['corres']:
            self.assertEqual(c[0], c[1])

        self.assertEqual(all(ac['ltable'] == A), True)
        self.assertEqual(all(ac['rtable'] == B), True)


    @raises(AssertionError)
    def test_get_attr_corres_invalid_df1(self):
        ac = get_attr_corres(None, pd.DataFrame())

    @raises(AssertionError)
    def test_get_attr_corres_invalid_df2(self):
        ac = get_attr_corres(pd.DataFrame(), None)

    def test_get_type_valid(self):
        A = read_csv_metadata(path_a)
        t = _get_type(A['ID'])
        self.assertEqual(t, 'str_eq_1w')

    @raises(AssertionError)
    def test_get_type_invalid_series(self):
        _get_type(None)


    def test_get_type_empty_series(self):
        t = _get_type(pd.Series())
        self.assertEqual(t, 'un_determined')

    @raises(AssertionError)
    def test_get_type_multiple_types(self):
        A = read_csv_metadata(path_a)
        A.ix[0, 'ID'] = 1000
        t = _get_type(A['ID'])

    def test_get_type_valid_2(self):
        A = read_csv_metadata(path_a)
        A['temp'] = True
        t = _get_type(A['temp'])
        self.assertEqual(t, 'boolean')

    def test_get_type_valid_3(self):
        A = read_csv_metadata(path_a)
        A['temp'] = "This is a very very very very very very very very very very very very very long string"
        t = _get_type(A['temp'])
        self.assertEqual(t, "str_gt_10w")

    def test_len_handle_nan_invalid(self):
        result = _len_handle_nan(None)
        self.assertEqual(pd.isnull(result), True)