from functools import partial
from nose.tools import *
import unittest
import pandas as pd
import numpy as np
import six

import py_entitymatching.feature.tokenizers as tok

class TokenizerTestCases(unittest.TestCase):
    def test_get_global_tokenizers(self):
        x = tok._global_tokenizers

    def test_get_tokenizers_for_blocking(self):
        x = tok.get_tokenizers_for_blocking()
        self.assertEqual(isinstance(x, dict), True)
        input = 'data science'
        for name, value in six.iteritems(x):
            self.assertEqual(isinstance(value(input), list), True)

    @raises(AssertionError)
    def test_get_tokenizers_for_blocking_invalid(self):
        tok.get_tokenizers_for_blocking(None, None)

    def test_get_tokenizers_for_matching(self):
        x = tok.get_tokenizers_for_matching()
        self.assertEqual(isinstance(x, dict), True)
        input = 'data science'
        for name, value in six.iteritems(x):
            self.assertEqual(isinstance(value(input), list), True)

    @raises(AssertionError)
    def test_get_tokenizers_for_matching_invalid(self):
        x = tok.get_tokenizers_for_matching(None, None)


    @raises(AssertionError)
    def test_get_single_arg_tokenizers_invalid_1(self):
        tok._get_single_arg_tokenizers(None, None)


    def test_get_single_arg_tokenizers_valid_2(self):
        tok._get_single_arg_tokenizers(q=3, dlm_char=' ')

    def test_get_single_arg_tokenizers_valid_3(self):
        tok._get_single_arg_tokenizers(q=[], dlm_char=[])

    def test_get_single_arg_tokenizers_valid_4(self):
        tok._get_single_arg_tokenizers(q=None, dlm_char=[' '])

    def test_get_single_arg_tokenizers_valid_5(self):
        tok._get_single_arg_tokenizers(q=3, dlm_char=None)

    def test_qgram_invalid(self):
        x = tok._make_tok_qgram(3)
        self.assertEqual(pd.isnull(x(np.NaN)), True)

    def test_qgram_delim(self):
        x = tok._make_tok_delim(' ')
        self.assertEqual(pd.isnull(x(np.NaN)), True)

    def test_tokqgram_valid(self):
        x = tok.tok_qgram('data science', 3)
        self.assertEqual(isinstance(x, list), True)

    def test_tokdelim_valid(self):
        x = tok.tok_delim('data science', ' ')
        self.assertEqual(isinstance(x, list), True)
        self.assertEqual(len(x), 2)

    def test_tokqgram_invalid(self):
        x = tok.tok_qgram(np.NaN, 3)
        self.assertEqual(pd.isnull(x), True)

    def test_tokdelim_invalid(self):
        x = tok.tok_delim(np.NaN, ' ')
        self.assertEqual(pd.isnull(x), True)
