from nose.tools import *
import unittest
import os
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.io.parsers import read_csv_metadata
import sys

if sys.version_info >= (3, 5):
    from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
    from py_entitymatching.labeler.new_labeler.view import Renderer
    from py_entitymatching.labeler.new_labeler.controller.FilterController import FilterController
    from py_entitymatching.labeler.new_labeler.controller.StatsController import StatsController
    from py_entitymatching.labeler.new_labeler.controller.LabelUpdateController import LabelUpdateController
    from py_entitymatching.labeler.new_labeler.controller.TuplePairDisplayController import TuplePairDisplayController
else:
    print('Skipping new_labeler imports for {0}'.format(sys.version_info))
datasets_path = os.sep.join([get_install_path(), 'tests', 'test_datasets'])
path_c = os.sep.join([datasets_path, 'C1.csv'])


# dummy page === to MainPage in new_labeler.py
class DummyPage:
    def setHtml(self, arg):
        pass


@unittest.skipIf(sys.version_info < (3, 5), "New labeler not supported in this version. Skipping tests")
class WorkflowTest(unittest.TestCase):
    def setUp(self):
        # setup Application Context
        C = read_csv_metadata(path_c)
        ApplicationContext.LABEL_COLUMN = "label"
        C[ApplicationContext.LABEL_COLUMN] = "Not-Labeled"
        ApplicationContext.TAGS_COLUMN = "tags"
        ApplicationContext.COMMENTS_COLUMN = "comments"
        ApplicationContext.COMPLETE_DATA_FRAME = C
        ApplicationContext.current_data_frame = C
        ApplicationContext.ALL_ATTRIBUTES = ApplicationContext.COMPLETE_DATA_FRAME.columns
        ApplicationContext.FILTER_CONTROLLER = FilterController(DummyPage())
        ApplicationContext.STATS_CONTROLLER = StatsController(DummyPage())
        ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER = TuplePairDisplayController(DummyPage())
        ApplicationContext.LABEL_CONTROLLER = LabelUpdateController(DummyPage())

    def tearDown(self):
        return None

    @istest
    def test_label_with_page_layout(self):
        # Changing layout does not affect number of label
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page
            ApplicationContext.LABEL_CONTROLLER.edit_tags(3, "new_tag")
            ApplicationContext.LABEL_CONTROLLER.edit_comments(3, "3_new_comment")

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.LABEL_COLUMN], ApplicationContext.NOT_LABELED)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 0)

            ApplicationContext.LABEL_CONTROLLER.change_label(4, ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == int(4), ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.MATCH)

            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.set_current_layout(ApplicationContext.VALID_LAYOUTS[2])

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[1][ApplicationContext.LABEL_COLUMN], ApplicationContext.NOT_LABELED)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_label_with_page_number(self):
        # Changing page number does not affect number of label
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page
            ApplicationContext.LABEL_CONTROLLER.edit_tags(3, "new_tag")
            ApplicationContext.LABEL_CONTROLLER.edit_comments(3, "3_new_comment")

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[3][ApplicationContext.LABEL_COLUMN], ApplicationContext.NOT_LABELED)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 0)

            ApplicationContext.LABEL_CONTROLLER.change_label(4, ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 4, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.MATCH)

            ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.change_page(2)

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 3, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.NOT_LABELED)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_label_with_filter_attributes(self):
        # Changing layout does not affect number of label
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page
            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 2, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.NOT_LABELED)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                               ApplicationContext.LABEL_COLUMN), 14)

            ApplicationContext.LABEL_CONTROLLER.change_label(2, ApplicationContext.MATCH)
            ApplicationContext.FILTER_CONTROLLER.filter_attribute(','.join([ApplicationContext.COMPLETE_DATA_FRAME.columns[3]]))
            self.assertEqual(len(ApplicationContext.current_attributes), 1)
            self.assertEqual(ApplicationContext.current_attributes[0], ApplicationContext.COMPLETE_DATA_FRAME.columns[3])

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 2, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                               ApplicationContext.LABEL_COLUMN), 13)

        finally:
            Renderer.render_main_page = orig_render_main_page

    @istest
    def test_label_with_filter_label(self):
        # Changing layout does not affect number of label
        orig_render_main_page = Renderer.render_main_page

        # mock renderer
        def mock_render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
            return None

        try:
            Renderer.render_main_page = mock_render_main_page
            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 2, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.NOT_LABELED)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                               ApplicationContext.LABEL_COLUMN), 14)

            ApplicationContext.LABEL_CONTROLLER.change_label(2, ApplicationContext.MATCH)
            ApplicationContext.FILTER_CONTROLLER.get_filtered_tuple_pairs(ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.current_data_frame.shape[0], 1)

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 2, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                               ApplicationContext.LABEL_COLUMN), 13)

            ApplicationContext.FILTER_CONTROLLER.get_filtered_tuple_pairs(ApplicationContext.ALL)

            self.assertEqual(ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                                        == 2, ApplicationContext.LABEL_COLUMN].values[0],
                             ApplicationContext.MATCH)
            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                           ApplicationContext.LABEL_COLUMN), 1)

            self.assertEqual(ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(ApplicationContext.COMPLETE_DATA_FRAME,
                                                                                               ApplicationContext.LABEL_COLUMN), 13)
            self.assertEqual(ApplicationContext.current_data_frame.shape[0], 14)

        finally:
            Renderer.render_main_page = orig_render_main_page
