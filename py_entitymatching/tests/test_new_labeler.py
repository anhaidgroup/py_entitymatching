from nose.tools import *
import unittest
from py_entitymatching.labeler.new_labeler.new_labeler import new_label_table


class NewLabelerTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_label_table_invalid_df(self):
        col_name = 'label'
        new_label_table(None, col_name)
