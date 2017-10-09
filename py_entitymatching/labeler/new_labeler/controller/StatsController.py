try:
    from PyQt5.QtCore import QObject
except ImportError:
    raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

from py_entitymatching.labeler.new_labeler.utils import ApplicationContext


class StatsController(QObject):
    """
    Computes statistics to be displayed
    """

    def __init__(self, main_page):
        super(StatsController, self).__init__(None)
        self.main_page = main_page

    def count_matched_tuple_pairs(self, data_frame, label_column_name):
        """ Returns a count of tuple pairs whose label value is MATCH.

        Args: 
            data_frame (DataFrame): Pandas data frame with label column.
        
        Returns:
            Count of tuple pairs with label == MATCH (int).

        Raises:
            KeyError if label_column_name is not in data frame
        """
        # todo check if assertion is correct thing to do
        if label_column_name not in data_frame.columns:
            raise KeyError("label column {label_column} not in dataframe".format(label_column=label_column_name))

        return data_frame[data_frame[label_column_name] == ApplicationContext.MATCH].shape[0]

    def count_non_matched_tuple_pairs(self, data_frame, label_column_name):
        """Returns a count of tuple pairs whose label value is NON-MATCH

        Args: 
            data_frame (DataFrame): Pandas data frame with label column.
        
        Returns:
            Count of tuple pairs with label == NON MATCH (int).

        Raises:
            KeyError if label_column_name is not in data frame
        """
        # todo check if assertion is correct thing to do
        if label_column_name not in data_frame.columns:
            raise KeyError("label column {label_column} not in dataframe".format(label_column=label_column_name))
        return data_frame[data_frame[label_column_name] == ApplicationContext.NON_MATCH].shape[0]

    def count_not_labeled_tuple_pairs(self, data_frame, label_column_name):
        """Returns a count of tuple pairs whose label value is NOT_LABELED

        Args: 
            data_frame (DataFrame): Pandas data frame with label column.
        
        Returns:
            Count of tuple pairs with label == NOT LABELED (int).

        Raises:
            KeyError if label_column_name is not in data frame
        """
        # todo check if assertion is correct thing to do
        if label_column_name not in data_frame.columns:
            raise KeyError("label column {label_column} not in dataframe".format(label_column=label_column_name))
        return data_frame[data_frame[label_column_name] == ApplicationContext.NOT_LABELED].shape[0]

    def count_not_sure_tuple_pairs(self, data_frame, label_column_name):
        """Returns a count of tuple pairs whose label value is NOT_SURE

        Args: 
            data_frame (DataFrame): Pandas data frame with label column.
        
        Returns:
            Count of tuple pairs with label == NOT SURE (int).

        Raises:
            KeyError if label_column_name is not in data frame
        """
        # todo check if assertion is correct thing to do
        if label_column_name not in data_frame.columns:
            raise KeyError("label column {label_column} not in dataframe".format(label_column=label_column_name))
        return data_frame[data_frame[label_column_name] == ApplicationContext.NOT_SURE].shape[0]
