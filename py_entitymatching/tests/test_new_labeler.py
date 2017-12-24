from nose.tools import *
import unittest
import os
import sys
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
if sys.version_info >= (3, 5):
    from py_entitymatching.labeler.new_labeler.new_labeler import new_label_table
    from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
    from py_entitymatching.labeler.new_labeler.controller.FilterController import FilterController
    from py_entitymatching.labeler.new_labeler.controller.StatsController import StatsController
    from py_entitymatching.labeler.new_labeler.controller.LabelUpdateController import LabelUpdateController
    from py_entitymatching.labeler.new_labeler.controller.TuplePairDisplayController import TuplePairDisplayController
    from py_entitymatching.labeler.new_labeler.view import Renderer
else:
    print('Skipping new_labeler imports for {0}'.format(sys.version_info))

datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])
path_d = os.sep.join([datasets_path, 'D.csv'])


# dummy page === to MainPage in new_labeler.py
class DummyPage:
    def setHtml(self, arg):
        pass


@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping tests")
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
@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping tests for FilterController")
class FilterControllerTestCases(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        A = read_csv_metadata(path_a)
        ApplicationContext.LABEL_COLUMN = "label"
        A[ApplicationContext.LABEL_COLUMN] = "Not-Labeled"
        ApplicationContext.COMPLETE_DATA_FRAME = A
        ApplicationContext.FILTER_CONTROLLER = FilterController(None)

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
        self.assertEqual(rows.shape[1], 7)


@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping test for StatsController")
class StatsControllerTestCases(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        ApplicationContext.LABEL_COLUMN = "label"
        ApplicationContext.COMPLETE_DATA_FRAME = read_csv_metadata(path_d)
        ApplicationContext.STATS_CONTROLLER = StatsController(None)

    def tearDown(self):
        return None

    @istest
    def test_count_matched_tuple_pairs(self):
        self.assertRaises(KeyError, ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs, ApplicationContext.COMPLETE_DATA_FRAME,
                          "non-existent-column-name")
        self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                       ApplicationContext.LABEL_COLUMN), 7)

    @istest
    def test_count_non_matched_tuple_pairs(self):
        self.assertRaises(KeyError, ApplicationContext.STATS_CONTROLLER.count_non_matched_tuple_pairs, ApplicationContext.COMPLETE_DATA_FRAME,
                          "non-existent-column-name")
        self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_non_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 3)

    @istest
    def test_count_not_labeled_tuple_pairs(self):
        self.assertRaises(KeyError, ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs, ApplicationContext.COMPLETE_DATA_FRAME,
                          "non-existent-column-name")
        self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

    @istest
    def test_count_not_sure_tuple_pairs(self):
        self.assertRaises(KeyError, ApplicationContext.STATS_CONTROLLER.count_not_sure_tuple_pairs, ApplicationContext.COMPLETE_DATA_FRAME,
                          "non-existent-column-name")
        self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_sure_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                        ApplicationContext.LABEL_COLUMN), 3)


@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping test for TupleDisplayController")
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

        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_page, -1)

    @istest
    def test_get_number_of_pages(self):
        # def get_number_of_pages(self, data_frame=ApplicationContext.current_data_frame):
        self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages, None)

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
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page
            self.assertRaises(ValueError, ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_page, -1)
            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_page(2)
            self.assertEqual(ApplicationContext.current_page_number, 2)
        finally:
            Renderer.render_main_page = orig_render_main_page

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


@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping test for LabelUpdateController")
class LabelUpdateControllerTestCases(unittest.TestCase):
    def setUp(self):
        ApplicationContext.LABEL_COLUMN = "label"
        ApplicationContext.COMPLETE_DATA_FRAME = read_csv_metadata(path_d)
        ApplicationContext.COMPLETE_DATA_FRAME.set_index("_id", inplace=True, drop=False)
        ApplicationContext.current_data_frame = ApplicationContext.COMPLETE_DATA_FRAME
        # ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN] = "Not-Labeled"
        ApplicationContext.LABEL_CONTROLLER = LabelUpdateController(DummyPage())
        ApplicationContext.TAGS_COLUMN = "tags"
        ApplicationContext.COMMENTS_COLUMN = "comments"
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER = TuplePairDisplayController(DummyPage())

    def tearDown(self):
        pass

    @istest
    def test_change_label(self):
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page

            self.assertRaises(KeyError, ApplicationContext.LABEL_CONTROLLER.change_label, 99, ApplicationContext.MATCH)
            self.assertRaises(ValueError, ApplicationContext.LABEL_CONTROLLER.change_label, 1, "non-existent-column")

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[4][ApplicationContext.LABEL_COLUMN], ApplicationContext.NON_MATCH)
            ApplicationContext.LABEL_CONTROLLER.change_label("4", ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[4][ApplicationContext.LABEL_COLUMN], ApplicationContext.MATCH)
        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_edit_tags(self):
        # ApplicationContext.LABEL_CONTROLLER.edit_tags()
        self.assertRaises(ValueError, ApplicationContext.LABEL_CONTROLLER.edit_tags, "1", 1234)
        self.assertRaises(KeyError, ApplicationContext.LABEL_CONTROLLER.edit_tags, -3, "value")

        self.assertNotIn(ApplicationContext.TAGS_COLUMN, ApplicationContext.COMPLETE_DATA_FRAME.columns)
        ApplicationContext.LABEL_CONTROLLER.edit_tags("3", "new_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.TAGS_COLUMN], "new_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.TAGS_COLUMN], 1)

        ApplicationContext.LABEL_CONTROLLER.edit_tags("3", "changed_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.TAGS_COLUMN], "changed_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.TAGS_COLUMN], 1)

        ApplicationContext.LABEL_CONTROLLER.edit_tags("4", "added_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[4][ApplicationContext.TAGS_COLUMN], "added_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.TAGS_COLUMN], "changed_tag")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.TAGS_COLUMN], 2)

    @istest
    def test_edit_comments(self):
        # ApplicationContext.LABEL_CONTROLLER.edit_tags()
        self.assertRaises(ValueError, ApplicationContext.LABEL_CONTROLLER.edit_comments, "1", 1234)
        self.assertRaises(KeyError, ApplicationContext.LABEL_CONTROLLER.edit_comments, -3, "value")

        self.assertNotIn(ApplicationContext.TAGS_COLUMN, ApplicationContext.COMPLETE_DATA_FRAME.columns)
        ApplicationContext.LABEL_CONTROLLER.edit_comments("3", "3_new_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.COMMENTS_COLUMN], "3_new_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.COMMENTS_COLUMN], 1)

        ApplicationContext.LABEL_CONTROLLER.edit_comments("3", "3_changed_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.COMMENTS_COLUMN], "3_changed_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.COMMENTS_COLUMN], 1)

        ApplicationContext.LABEL_CONTROLLER.edit_comments("4", "4_new_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[4][ApplicationContext.COMMENTS_COLUMN], "4_new_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.COMMENTS_COLUMN], "3_changed_comment")
        self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.count()[ApplicationContext.COMMENTS_COLUMN], 2)
