try:
    from PyQt5.QtCore import QObject
    from PyQt5.QtCore import pyqtSlot
except ImportError:
    raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

from math import ceil

from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
from py_entitymatching.labeler.new_labeler.view import Renderer
import os


class TuplePairDisplayController(QObject):
    """
    Controls the display of tuple pairs i.e. the main area of the
    labeler.
    Listens to events that can change the appearance of main area.
    """

    def __init__(self, main_page):
        super(TuplePairDisplayController, self).__init__(None)
        self.main_page = main_page

    @pyqtSlot()
    def set_per_page_count(self, count_per_page):
        """ Sets the number of tuple pairs to be shown per page.

        Args: 
            count_per_page (int): per page tuple pair count value.
            
        Returns:
            None.
            
        Raises:
            ValueError if count_per_page is negative
        """
        if count_per_page <= 0:
            raise ValueError("count of tuple pairs per page can not be negative")

        ApplicationContext.tuple_pair_count_per_page = count_per_page

    def set_current_page(self, current_page):
        """ Sets the current page of tuple pairs being displayed.
        
        Args:
            current_page (int): Current page number.
            
        Returns:
            None.
            
        Raises:
            ValueError if current page is negative
        """
        if current_page < 0:
            raise ValueError
        ApplicationContext.current_page_number = current_page

    def set_current_layout(self, layout):
        """ Sets the current tuple pair display layout.
        
        Args:
            layout (str): Layout value to change to.
             
        Returns:
            None.
            
        Raises:
            ValueError if parameter is not in list of valid layouts
        """
        # todo 5/7/17 better way to see allowed layouts
        if layout not in ApplicationContext.VALID_LAYOUTS:
            raise ValueError("not a valid layout")
        else:
            ApplicationContext.current_layout = layout

    def get_tuples_for_page(self, page_number):
        """ Gets tuple pairs for a given page number.
        
        Args:
            page_number (int): page number of tuple pairs to get.
            
        Returns:
            Tuple Pairs (DataFrame).
            
        Raises:
            ValueError if page number is not positive
        """
        if page_number < 0:
            raise ValueError("page number parameter must be non-negative")

        return ApplicationContext.current_data_frame.iloc[
               page_number * ApplicationContext.tuple_pair_count_per_page: page_number * ApplicationContext.tuple_pair_count_per_page + ApplicationContext.tuple_pair_count_per_page]

    def get_current_page(self):
        """ Gets current page number being displayed.
        
        Args:
            None.
            
        Returns:
            Page number (int).
            
        Raises:         
        """
        return ApplicationContext.current_page_number

    def get_per_page_count(self):
        """ Gets number of tuple pairs being displayed
        
        Args:
            None.
            
        Returns:
            Tuple pair count per page (int).
            
        Raises:         
        """
        return ApplicationContext.tuple_pair_count_per_page

    def get_number_of_pages(self, data_frame=ApplicationContext.current_data_frame):
        """ Gets number of pages of tuple pairs
        
        Args:
            data_frame (DataFrame): DataFrame whose pages are counted. 
            Default Value is the application's current data frame with filters applied. 
            
        Returns:
            Number of pages (int): Number of pages in DataFrame.
            
        Raises:         
            ValueError if data_frame is None
        """
        if data_frame is None:
            raise ValueError("None passed as data frame")

        return ceil(data_frame.shape[0] / ApplicationContext.tuple_pair_count_per_page)

    @pyqtSlot(int)
    def change_page(self, page_number):
        """ Changes the page being displayed and renders it.
        
        Args:
            page_number (int): Page number to change the display to.
            
        Returns:
            None.
            
        Raises:
            ValueError if current page is negative
        """
        self.set_current_page(page_number)
        self.main_page.setHtml(
            Renderer.render_main_page(
                current_page_tuple_pairs=
                self.get_tuples_for_page(page_number),
                match_count=
                ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                                       == ApplicationContext.MATCH].shape[0],
                not_match_count=
                ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                                       == ApplicationContext.NON_MATCH].shape[0],
                not_sure_count=
                ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                                       == ApplicationContext.NOT_SURE].shape[0],
                unlabeled_count=
                ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.COMPLETE_DATA_FRAME[ApplicationContext.LABEL_COLUMN]
                                                       == ApplicationContext.NOT_LABELED].shape[0],
            )
        )

    @pyqtSlot(str)
    def change_layout(self, layout):
        """ Changes the layout of the tuple pair display and renders it.

        Args:
            page_number (int): Page number to change the display to.

        Returns:
            None.

        Raises:
            ValueError if parameter is not in list of valid layouts
        """
        self.set_current_layout(layout)
        if layout == 'single':
            ApplicationContext.tuple_pair_count_per_page = 1
        else:
            ApplicationContext.tuple_pair_count_per_page = ApplicationContext.DEFAULT_TUPLE_PAIR_COUNT_PER_PAGE
        self.change_page(ApplicationContext.current_page_number)

    @pyqtSlot(str)
    def save_data(self, save_file_name):
        """ Save tuple pairs with their current labels, comments and tags to a CSV file.

        Args:
            save_file_name (str): Name of CSV file to save tuple pairs in.
            
        Returns:
            None
            
        Raises:

        """
        # todo WINDOWS
        path = save_file_name.split("/")
        # path.remove("/")
        # path.remove("")

        if os.path.isdir(ApplicationContext.SAVEPATH + "/".join(path[:len(path) - 1])):
            ApplicationContext.save_file_name = save_file_name
            ApplicationContext.COMPLETE_DATA_FRAME.to_csv(ApplicationContext.SAVEPATH + save_file_name)
        else:
            return

    @pyqtSlot(int)
    def change_token_count(self, token_count):
        """ Changes the number of letters displayed per attribute of a tuple pair.
        
        Args:
            token_count (int): Number
        
        Returns:
            None.
        
        Raises:
            ValueError if new token_count is not positive
        """
        if token_count <= 0:
            raise ValueError
        ApplicationContext.alphabets_per_attribute_display = token_count
        self.change_page(ApplicationContext.current_page_number)
