from nose.tools import *
import unittest
import os
from py_entitymatching.labeler.new_labeler.new_labeler import new_label_table
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
from py_entitymatching.labeler.new_labeler.controller.FilterController import FilterController
from py_entitymatching.labeler.new_labeler.controller.StatsController import StatsController
from py_entitymatching.labeler.new_labeler.controller.TuplePairDisplayController import TuplePairDisplayController
from py_entitymatching.labeler.new_labeler.view import Renderer

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
path_d = os.sep.join([datasets_path, 'D.csv'])


# dummy page === to MainPage in new_labeler.py
class DummyPage:
    def setHtml(self, arg):
        pass


class NewLabelerTestCases(unittest.TestCase):
    @raises(AssertionError)
    def test_label_table_invalid_df(self):
        col_name = 'label'
        new_label_table(None, col_name)

    @raises(AssertionError)
    def test_label_table_invalid_colname(self):
        A = read_csv_metadata(path_a)
        new_label_table(A, None)

    @raises(AssertionError)
    def test_label_invalid_column(self):
        C = read_csv_metadata(path_a)
        col_name = "zipcode"
        new_label_table(C, col_name)


# Test Controlers

class FilterControllerTestCases(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        A = read_csv_metadata(path_a)
        ApplicationContext.LABEL_COLUMN = "label"
        A[ApplicationContext.LABEL_COLUMN] = "Not-Labeled"
        ApplicationContext.COMPLETE_DATA_FRAME = A
        ApplicationContext.FILTER_CONTROLLER = FilterController(None)
        ApplicationContext.STATS_CONTROLLER = StatsController(None)

        # FilterController.ApplicationContext.COMPLETE_DATA_FRAME = A

    def tearDown(self):
        return None

    @istest
    def test_get_matching_tuple_pairs(self):
        rows = ApplicationContext.FILTER_CONTROLLER.get_matching_tuple_pairs()
        assert rows.empty

    @istest
    def test_get_non_matching_tuple_pairs(self):
        rows = ApplicationContext.FILTER_CONTROLLER.get_non_matched_tuple_pairs()
        assert rows.empty

    @istest
    def test_get_non_sure_tuple_pairs(self):
        rows = ApplicationContext.FILTER_CONTROLLER.get_non_sure_tuple_pairs()
        assert rows.empty

    @istest
    def test_get_not_labeled_tuple_pairs(self):
        rows = ApplicationContext.FILTER_CONTROLLER.get_non_sure_tuple_pairs()
        assert rows.shape[1] is 7


class StatsControllerTestCases(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        ApplicationContext.LABEL_COLUMN = "label"
        ApplicationContext.COMPLETE_DATA_FRAME = read_csv_metadata(path_d)
        # ApplicationContext.FILTER_CONTROLLER = FilterController(None)
        ApplicationContext.STATS_CONTROLLER = StatsController(None)

    def tearDown(self):
        return None

    @istest
    def test_count_matched_tuple_pairs(self):
        assert ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                             ApplicationContext.LABEL_COLUMN) is 8

    @istest
    def test_count_non_matched_tuple_pairs(self):
        assert ApplicationContext.STATS_CONTROLLER.count_non_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                 ApplicationContext.LABEL_COLUMN) is 3

    @istest
    def test_count_not_labeled_tuple_pairs(self):
        assert ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                 ApplicationContext.LABEL_COLUMN) is 1

    @istest
    def test_count_not_sure_tuple_pairs(self):
        assert ApplicationContext.STATS_CONTROLLER.count_not_sure_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                              ApplicationContext.LABEL_COLUMN) is 3


class TuplePairDisplayControllerTestCases(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        ApplicationContext.LABEL_COLUMN = "label"
        ApplicationContext.COMPLETE_DATA_FRAME = read_csv_metadata(path_d)
        ApplicationContext.current_data_frame = ApplicationContext.COMPLETE_DATA_FRAME
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER = TuplePairDisplayController(DummyPage())
        ApplicationContext.current_page_number = 1

    def tearDown(self):
        pass

    @istest
    def test_get_per_page_count(self):
        # def get_per_page_count(self):
        # Check for default value of per page count
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_per_page_count(), 5)

    @istest
    def test_set_per_page_count(self):
        # check for default first
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count(12)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_per_page_count(), 12)
        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count, 0)
        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count, -1)

    @istest
    def test_get_current_page(self):
        #                 def get_current_page(self):
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_current_page(), 1)

    @istest
    def test_set_current_page(self):
        # def set_current_page(self, current_page):
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_current_page(), 1)
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_page(5)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_current_page(), 5)

    @istest
    def test_get_number_of_pages(self):
        # def get_number_of_pages(self, data_frame=ApplicationContext.current_data_frame):
        # default per page count
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count(5)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_per_page_count(), 5)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(ApplicationContext.current_data_frame), 3)

        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count(10)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(ApplicationContext.current_data_frame), 2)

        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count(30)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(ApplicationContext.current_data_frame), 1)

        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_per_page_count(5)
        self.assertEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(ApplicationContext.current_data_frame), 3)

    @istest
    def test_set_current_layout(self):
        # def set_current_layout(self, layout):
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_layout(ApplicationContext.VALID_LAYOUTS[0])
        self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[0])

        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_layout(ApplicationContext.VALID_LAYOUTS[1])
        self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[1])

        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_layout(ApplicationContext.VALID_LAYOUTS[2])
        self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[2])

        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_layout, "new_layout")

    @istest
    def test_change_page(self):
        # def change_page(self, page_number):
        return None

    @istest
    def test_change_layout(self):
        # def change_layout(self, layout):
        # check default
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page

            self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[0])
            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_page(3)
            page_number_before_change = ApplicationContext.current_page_number

            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_layout("single")
            self.assertEqual(ApplicationContext.current_layout, "single")
            self.assertEqual(ApplicationContext.tuple_pair_count_per_page, 1)
            self.assertEqual(ApplicationContext.current_page_number, page_number_before_change)

            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_layout(ApplicationContext.VALID_LAYOUTS[0])
            self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[0])
            self.assertEqual(ApplicationContext.tuple_pair_count_per_page, ApplicationContext.DEFAULT_TUPLE_PAIR_COUNT_PER_PAGE)
            self.assertEqual(ApplicationContext.current_page_number, page_number_before_change)

            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_layout(ApplicationContext.VALID_LAYOUTS[1])
            self.assertEqual(ApplicationContext.current_layout, ApplicationContext.VALID_LAYOUTS[1])
            self.assertEqual(ApplicationContext.tuple_pair_count_per_page, ApplicationContext.DEFAULT_TUPLE_PAIR_COUNT_PER_PAGE)
            self.assertEqual(ApplicationContext.current_page_number, page_number_before_change)

            self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_layout, "random_layout")

        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_change_token_count(self):
        # def change_token_count(self, token_count):
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page

            self.assertEqual(ApplicationContext.alphabets_per_attribute_display, 5)
            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_token_count(10)
            self.assertEqual(ApplicationContext.alphabets_per_attribute_display, 10)

            self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_token_count, -1)
        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_get_tuples_for_page(self):
        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_tuples_for_page, -1)
        self.assertLessEqual(ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_tuples_for_page(0).shape[1],
                             ApplicationContext.COMPLETE_DATA_FRAME.shape[1])

        # def get_tuples_for_page(self, page_number):
