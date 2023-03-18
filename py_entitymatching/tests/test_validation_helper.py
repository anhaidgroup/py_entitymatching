# coding=utf-8

import unittest

# from nose.tools import *
import pandas as pd
from .utils import raises

from py_entitymatching.utils import validation_helper as vh


class ValidationHelperTestCases(unittest.TestCase):
    def test_validate_object_type_with_valid_type(self):
        vh.validate_object_type('ABC', str)
        vh.validate_object_type(pd.DataFrame(), pd.DataFrame)
        vh.validate_object_type(list(), list),
        vh.validate_object_type(True, bool),
        vh.validate_object_type(123, int),
        vh.validate_object_type(dict(), dict)
        
        # Currently, can validate unexpected types
        class A(object): pass
        a = A()
        vh.validate_object_type(a, A)

    def test_validate_object_type_with_invalid_type(self):
        self.assertRaises(AssertionError, lambda: vh.validate_object_type('ABC', int))
        self.assertRaises(AssertionError, lambda: vh.validate_object_type(123, str))
        self.assertRaises(AssertionError, lambda: vh.validate_object_type(list(), dict))
        self.assertRaises(AssertionError, lambda: vh.validate_object_type(dict(), list))

    def test_validate_object_type_with_unexpected_type(self):
        class B(object): pass
        self.assertRaises(KeyError, lambda: vh.validate_object_type(123, B))

    def test_validate_subclass_with_valid_class(self):
        class C(object): pass
        class D(C): pass
        class E(D): pass
        vh.validate_subclass(E, E)
        vh.validate_subclass(E, D)
        vh.validate_subclass(E, C)

    def test_validate_subclass_with_invalid_class(self):
        class F(object): pass
        class G(object): pass
        class H(G): pass
        self.assertRaises(AssertionError, lambda: vh.validate_subclass(G, F))
        self.assertRaises(AssertionError, lambda: vh.validate_subclass(H, F))
