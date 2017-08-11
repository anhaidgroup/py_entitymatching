try:
    from PyQt5.QtCore import QFile
    from PyQt5.QtCore import pyqtSlot
    from PyQt5.QtCore import QObject
except ImportError:
    raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                          'GUI related functions in py_entitymatching.')

from py_entitymatching.labeler.new_labeler.utils import ApplicationContext
from py_entitymatching.labeler.new_labeler.view import Renderer


class LabelUpdateController(QObject):
    """
    Responds to events changing tuple pair labels.
    """

    def __init__(self, main_page):
        super(LabelUpdateController, self).__init__(None)
        self.main_page = main_page

    @pyqtSlot(str, str)
    def change_label(self, tuple_pair_id, new_label):
        """ Changes the label of a tuple pair and renders the main window with changed stats values.
        
        Args:
            tuple_pair_id (int): ID of the tuple pair whose label has been changed.
            new_label (string): New label to apply to the tuple pair with the given ID.
            
        Returns:
            None.
            
        Raises:
        """
        ApplicationContext.COMPLETE_DATA_FRAME.loc[ApplicationContext.COMPLETE_DATA_FRAME['_id']
                                                   == int(tuple_pair_id), ApplicationContext.LABEL_COLUMN] = new_label
        self.main_page.setHtml(
            Renderer.render_main_page(
                current_page_tuple_pairs=
                ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_tuples_for_page(ApplicationContext.current_page_number),
                match_count=
                ApplicationContext.current_data_frame[ApplicationContext.current_data_frame[ApplicationContext.LABEL_COLUMN]
                                                      == ApplicationContext.MATCH].shape[0],
                not_match_count=
                ApplicationContext.current_data_frame[ApplicationContext.current_data_frame[ApplicationContext.LABEL_COLUMN]
                                                      == ApplicationContext.NON_MATCH].shape[0],
                not_sure_count=
                ApplicationContext.current_data_frame[ApplicationContext.current_data_frame[ApplicationContext.LABEL_COLUMN]
                                                      == ApplicationContext.NOT_SURE].shape[0],
                unlabeled_count=
                ApplicationContext.current_data_frame[ApplicationContext.current_data_frame[ApplicationContext.LABEL_COLUMN]
                                                      == ApplicationContext.NOT_LABELED].shape[0],
            )
        )

    @pyqtSlot(str, str)
    def edit_tags(self, tuple_pair_id, tags):
        """ Replaces the current value of tags for a tuple pair with a new value.
        
        Args:
            tuple_pair_id (int): ID of the tuple pair whose tags value is to be changed.
            tags (str): New value of tags for the tuple pair with given ID.
            
        Returns:
            None.
            
        Raises:
        """
        ApplicationContext.COMPLETE_DATA_FRAME.loc[
            ApplicationContext.COMPLETE_DATA_FRAME['_id'] == int(tuple_pair_id), ApplicationContext.TAGS_COLUMN] = tags

    @pyqtSlot(str, str)
    def edit_comments(self, tuple_pair_id, comments):
        """ Replaces the current value of comments for a tuple pair with a new value.
        
        Args:
            tuple_pair_id (int): ID of the tuple pair whose comments value is to be changed.
            comments (str): New value of comments for the tuple pair with given ID.
            
        Returns:
            None.
            
        Raises:
        """
        ApplicationContext.COMPLETE_DATA_FRAME.loc[
            ApplicationContext.COMPLETE_DATA_FRAME['_id'] == int(tuple_pair_id), ApplicationContext.COMMENTS_COLUMN] = comments
