try:
    from PyQt5.QtCore import QFile
    from PyQt5.QtCore import pyqtSlot
    from PyQt5.QtCore import QObject
except ImportError:
    raise ImportError("PyQt5 is not installed. Please install PyQt5 to use "
                      "GUI related functions in py_entitymatching.")

from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
from py_entitymatching.labeler.new_labeler.view import Renderer


class FilterController(QObject):
    """
    Responds to requests to apply filters on tuple pairs
    """

    def __init__(self, main_page):
        super(FilterController, self).__init__(None)
        self.main_page = main_page

    def get_matching_tuple_pairs(self):
        """Gets tuple pairs whose label value is currently "MATCH" from complete data frame.

        Args:
            None
            
        Returns:
            Data frame with tuple pairs whose label value is currently NON-MATCH.
            
        Raises:
                
        """
        return ApplicationContext.COMPLETE_DATA_FRAME[
            ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN] == ApplicationContext.MATCH]

    def get_non_matched_tuple_pairs(self):
        """Gets tuple pairs whose label value is currently NON-MATCH from complete data frame.

        Args:
            None
        
        Returns:
            Data frame with tuple pairs whose label value is currently NON-MATCH.
            
        Raises:
        """
        return ApplicationContext.COMPLETE_DATA_FRAME[
            ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN] == ApplicationContext.NON_MATCH]

    def get_non_sure_tuple_pairs(self):
        """Gets tuple pairs whose label value is currently NON-MATCH from complete data frame.

        Args:
            None.
            
        Returns:
            Data frame with tuple pairs whose label value is currently NON-MATCH.
        
        Raises:    
            
        """
        return ApplicationContext.COMPLETE_DATA_FRAME[
            ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN] == ApplicationContext.NOT_SURE]

    def get_not_labeled_tuple_pairs(self):
        """Gets tuple pairs whose label value is currently NON-MATCH from complete data frame.

        Args:
            None.
            
        Returns:
            Data frame with tuple pairs whose label value is currently NON-MATCH.
            
        Raises:
        """
        return ApplicationContext.COMPLETE_DATA_FRAME[
            ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN] == ApplicationContext.NOT_LABELED]

    @pyqtSlot(str)
    def get_filtered_tuple_pairs(self, label):
        """ Filters tuple pairs by label value and renders them.
        
        Args: 
            label (str): label value to filter by. Has to be one of the valid constant
            label values from ApplicationContext.
            
        Returns:
            None
            
        Raises:
            Value error if label is not a valid value for label column
        """
        data = None
        if label not in ApplicationContext.VALID_LABELS and label != ApplicationContext.ALL:
            raise ValueError("{label_value} is not a valid value for label column {label_column}".format(label_value=label,
                                                                                                         label_column=ApplicationContext.LABEL_COLUMN))
        if label == ApplicationContext.MATCH:
            data = self.get_matching_tuple_pairs()
        elif label == ApplicationContext.NON_MATCH:
            data = self.get_non_matched_tuple_pairs()
        elif label == ApplicationContext.NOT_SURE:
            data = self.get_non_sure_tuple_pairs()
        elif label == ApplicationContext.NOT_LABELED:
            data = self.get_not_labeled_tuple_pairs()
        elif label == ApplicationContext.ALL:
            data = ApplicationContext.COMPLETE_DATA_FRAME

        ApplicationContext.current_data_frame = data
        data = data.iloc[
               0 * ApplicationContext.tuple_pair_count_per_page: 0 * ApplicationContext.tuple_pair_count_per_page +
                                                                 ApplicationContext.tuple_pair_count_per_page]
        self.main_page.setHtml(
            Renderer.render_main_page(current_page_tuple_pairs=data,
                                      match_count=ApplicationContext.COMPLETE_DATA_FRAME[
                                          ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                          == ApplicationContext.MATCH].shape[0],
                                      not_match_count=ApplicationContext.COMPLETE_DATA_FRAME[
                                          ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                          == ApplicationContext.NON_MATCH].shape[0],
                                      not_sure_count=ApplicationContext.COMPLETE_DATA_FRAME[
                                          ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                          == ApplicationContext.NOT_SURE].shape[0],
                                      unlabeled_count=ApplicationContext.COMPLETE_DATA_FRAME[
                                          ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                          == ApplicationContext.NOT_LABELED].shape[0]
                                      )
        )

    @pyqtSlot(str)
    def filter_attribute(self, attributes):
        """ Filters the attributes shown for every tuple pair and renders them.
        
        Args:
            attributes (list[str]): List of attributes to show for each tuple pair
            
        Returns:
            None
            
        Raises:
        """

        attributes = attributes.split(",")
        if "_show_all" in attributes:
            ApplicationContext.current_attributes = ApplicationContext.ALL_ATTRIBUTES
        else:
            # todo 5/7/17 check if attributes are valid
            if "" in attributes:
                attributes.remove("")
            ApplicationContext.current_attributes = attributes
        html = Renderer.render_main_page(
            current_page_tuple_pairs=ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_tuples_for_page(ApplicationContext.current_page_number),
            match_count=ApplicationContext.STATS_CONTROLLER.count_matched_tuple_pairs(
                ApplicationContext.current_data_frame, label_column_name=ApplicationContext.LABEL_COLUMN),
            not_match_count=ApplicationContext.STATS_CONTROLLER.count_non_matched_tuple_pairs(
                ApplicationContext.current_data_frame, label_column_name=ApplicationContext.LABEL_COLUMN),
            not_sure_count=ApplicationContext.STATS_CONTROLLER.count_not_sure_tuple_pairs(
                ApplicationContext.current_data_frame, label_column_name=ApplicationContext.LABEL_COLUMN),
            unlabeled_count=ApplicationContext.STATS_CONTROLLER.count_not_labeled_tuple_pairs(
                ApplicationContext.current_data_frame, label_column_name=ApplicationContext.LABEL_COLUMN)
        )
        self.main_page.setHtml(html)
