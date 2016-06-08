# coding=utf-8
import os
import unittest
from nose.tools import *

import six

from magellan.matcher.dtmatcher import DTMatcher
from magellan.matcher.linregmatcher import LinRegMatcher
from magellan.matcher.logregmatcher import LogRegMatcher
from magellan.matcher.nbmatcher import NBMatcher
from magellan.matcher.rfmatcher import RFMatcher
from magellan.matcher.svmmatcher import SVMMatcher

from magellan.io.parsers import read_csv_metadata
import magellan.matcher.matcherutils as mu
import magellan.catalog.catalog_manager as cm
from magellan.utils.generic_helper import get_install_path

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])


class MLMatcherTestCases(unittest.TestCase):
    def test_valid_names_for_matchers(self):
        matchers1 = {"DT": DTMatcher(), "LinReg": LinRegMatcher(), "LogReg": LogRegMatcher(),
                    "NB": NBMatcher(), "RF": RFMatcher(), "SVM": SVMMatcher()}

        matchers2 = {"DT": DTMatcher(name='temp'), "LinReg": LinRegMatcher(name='temp'),
                     "LogReg": LogRegMatcher(name='temp'),
                    "NB": NBMatcher(name='temp'), "RF": RFMatcher(name='temp'), "SVM": SVMMatcher(name='temp')}

        for m_name, matcher in six.iteritems(matchers1):
                self.assertEqual(isinstance(matcher.name, six.string_types), True)

        for m_name, matcher in six.iteritems(matchers2):
                self.assertEqual(matcher.name, 'temp')


    def test_train_test_split_valid_1(self):
        A = read_csv_metadata(path_a)
        B = read_csv_metadata(path_b, key='ID')
        C = read_csv_metadata(path_c, ltable=A, rtable=B)
        result = mu.train_test_split(C)
        train = result['train']
        test = result['test']
        self.assertEqual(len(train)+len(test), len(C))
        p1 = cm.get_all_properties(C)
        p2 = cm.get_all_properties(train)
        p3 = cm.get_all_properties(test)
        # d = {}
        # d['ltable'] = A
        # d['rtable'] = A
        # d['key'] = '_id'
        # d['fk_ltable'] = 'ltable_ID'
        # d['fk_rtable'] = 'rtable_ID'
        self.assertEqual(p1 == p2, True)
        self.assertEqual(p1 == p3, True)
        # self.assertEqual(p1 == d, True)

    @raises(AssertionError)
    def test_train_test_split_invalid_df(self):
        mu.train_test_split(None)





