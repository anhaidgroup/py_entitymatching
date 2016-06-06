import os
from nose.tools import *
import unittest
import pandas as pd
import six

from magellan.utils.generic_helper import get_install_path
from magellan.io.parsers import read_csv_metadata
from magellan.feature.simfunctions import get_sim_funs_for_matching
from magellan.feature.tokenizers import get_tokenizers_for_matching
from magellan.feature.autofeaturegen import get_features_for_matching
from magellan.feature.attributeutils import get_attr_corres, get_attr_types, get_type, len_handle_nan

import magellan.catalog.catalog_manager as cm

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])


class AttributeUtilsTestCases(unittest.TestCase):
    def test_get_attr_types_valid(self):
        A = read_csv_metadata(path_a)
        x = get_attr_types(A)

    @raises(AssertionError)
    def test_get_attr_types_invalid_df(self):
        x = get_attr_types(None)

    def test_get_attr_corres_valid(self):
        A = read_csv_metadata(path_a)
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
        t = get_type(A['ID'])
        self.assertEqual(t, 'str_eq_1w')

    @raises(AssertionError)
    def test_get_type_invalid_series(self):
        get_type(None)


    def test_get_type_empty_series(self):
        t = get_type(pd.Series())
        self.assertEqual(t, 'numeric')

    @raises(AssertionError)
    def test_get_type_multiple_types(self):
        A = read_csv_metadata(path_a)
        A.ix[0, 'ID'] = 1000
        t = get_type(A['ID'])