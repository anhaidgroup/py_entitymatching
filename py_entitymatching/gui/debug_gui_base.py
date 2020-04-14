from collections import OrderedDict


try:
    from PyQt5 import QtCore, QtWidgets
except ImportError:
    raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

import py_entitymatching as em
import py_entitymatching.catalog.catalog_manager as cm
from  py_entitymatching.gui.gui_utils import DictTableViewWithLabel, \
    DataFrameTableViewWithLabel, TreeViewWithLabel

class MainWindowManager(QtWidgets.QWidget):
    """
    This class defines the main window manager
    """
    def __init__(self, matcher, matcher_type, exclude_attrs_or_feature_table,
                 dictionary, table,
                 fp_dataframe, fn_dataframe):
        super(MainWindowManager, self).__init__()
        # Set the parameters as attributes to the class.
        self.matcher = matcher
        self.matcher_type = matcher_type
        self.exclude_attrs_or_feature_table = exclude_attrs_or_feature_table
        self.dictionary = dictionary
        self.table = table
        self.fp_dataframe = fp_dataframe
        self.fn_dataframe = fn_dataframe
        # Get the instance for QtWidgets
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        app = em._viewapp

        ltable = cm.get_ltable(self.table)
        rtable = cm.get_rtable(self.table)

        # Get the copy of ltable and rtable
        l_df = ltable.copy()
        r_df = rtable.copy()

        # Set it as dataframes in the class
        self.l_df = l_df.set_index(cm.get_key(ltable), drop=False)
        self.r_df = r_df.set_index(cm.get_key(rtable), drop=False)

        # Set the metric widget, dataframe widget , combo box and the
        # dataframe correctly.
        self.metric_widget = None
        self.dataframe_widget = None
        self.combo_box = None
        self.current_combo_text = 'False Positives'
        self.current_dataframe = self.fp_dataframe
        self.setup_gui()

        width = min((40 + 1) * 105, app.desktop().screenGeometry().width() - 50)
        height = min((50 + 1) * 41, app.desktop().screenGeometry().width() -
                     100)
        # set the size of height and width corrrectly.
        self.resize(width, height)

    def setup_gui(self):
        """
        This function sets up the GUI
        """
        # Set the combo box with the values false postives and negatives
        self.combo_box = QtWidgets.QComboBox()
        self.combo_box.addItems(['False Positives', 'False Negatives'])
        self.combo_box.activated[str].connect(self.combobox_onactivated)

        # Set the metric widget
        self.metric_widget = DictTableViewWithLabel(self,
                                                    self.dictionary, 'Metrics',
                                                    self.combo_box)
        # Set the dataframe widget
        self.dataframe_widget = DataFrameTableViewWithLabel(self,
                                                        self.current_dataframe,
                                                        self.current_combo_text)
        # Set the window title
        self.setWindowTitle('Debugger')
        # We want to split the main window vertically into two halves and on
        # the left have metric, and the combo box for false positives and
        # negatives
        layout = QtWidgets.QVBoxLayout(self)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        splitter.addWidget(self.metric_widget)
        splitter.addWidget(self.dataframe_widget)
        layout.addWidget(splitter)
        self.setLayout(layout)

    def handle_debug_button(self, index):
        """
        Function to handle debug button
        """
        r = self.current_dataframe.iloc[[index]]
        # Handle debug, show the tree to the user
        l_fkey = cm.get_fk_ltable(self.table)
        r_fkey = cm.get_fk_rtable(self.table)
        l_val = r.loc[r.index.values[0], l_fkey]
        r_val = r.loc[r.index.values[0], r_fkey]
        d1 = OrderedDict(self.l_df.loc[l_val])
        d2 = OrderedDict(self.r_df.loc[r_val])
        if self.matcher_type == 'dt':
            ret_val, node_list = em.vis_tuple_debug_dt_matcher(
                self.matcher, r,
                self.exclude_attrs_or_feature_table)
        elif self.matcher_type == 'rf':
            ret_val, node_list = em.vis_tuple_debug_rf_matcher(self.matcher, r,
                                                               self.exclude_attrs_or_feature_table)
        else:
            raise TypeError('Unknown matcher type ')
        debug_result = [ret_val]
        debug_result.append(node_list)
        # Create the debug window manager
        debug_obj = DebugWindowManager(d1, d2, self.matcher_type, debug_result)
        # Show the debug window to the user
        debug_obj.show()

    def handle_show_button(self, index):
        """
        The function to handle the show butting
        """
        r = self.current_dataframe.iloc[[index]]
        l_fkey = cm.get_fk_ltable(self.table)
        r_fkey = cm.get_fk_rtable(self.table)

        l_val = r.loc[r.index.values[0], l_fkey]
        r_val = r.loc[r.index.values[0], r_fkey]
        d1 = OrderedDict(self.l_df.loc[l_val])
        d2 = OrderedDict(self.r_df.loc[r_val])
        # Create a window manager
        show_obj = ShowWindowManager(d1, d2)
        # Show the window to the user
        show_obj.show()

    def combobox_onactivated(self, text):
        """
        Function to handle combobox activation
        """
        if text != self.current_combo_text:
            # Find that is selected. Based on what is selected show the
            # appropriate DataFrame
            if text == 'False Negatives':
                self.current_combo_text = text
                self.current_dataframe = self.fn_dataframe
            else:
                self.current_combo_text = text
                self.current_dataframe = self.fp_dataframe

            # Set up gui to show the Dataframe
            self.dataframe_widget.tbl_obj.dataframe = self.current_dataframe
            self.dataframe_widget.tbl_obj.setup_gui()
            self.dataframe_widget.label_obj.setText(text)


class ShowWindowManager(QtWidgets.QWidget):
    """
    Class to handle the window manager
    """
    def __init__(self, left_tuple_dict, right_tuple_dict):
        super(ShowWindowManager, self).__init__()
        # set the left tuple and right tuple
        self.left_tuple_dict = left_tuple_dict
        self.right_tuple_dict = right_tuple_dict
        self.left_tuple_widget = None
        self.right_tuple_widget = None

        self.setup_gui()

    def setup_gui(self):
        """
        Set up gui for the window manager

        """
        # Set the left tuple and right  tuple
        self.left_tuple_widget = DictTableViewWithLabel(self, self.left_tuple_dict, 'Left Tuple')
        self.right_tuple_widget = DictTableViewWithLabel(self, self.right_tuple_dict, 'Right Tuple')
        self.setWindowTitle('Show Tuples')

        layout = QtWidgets.QVBoxLayout()
        # Set the widgets at appropriate position
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        splitter.addWidget(self.left_tuple_widget)
        splitter.addWidget(self.right_tuple_widget)
        layout.addWidget(splitter)
        # Set the layout correctly
        self.setLayout(layout)


class DebugWindowManager(QtWidgets.QWidget):
    """
    Class defining the over all debug window manager
    """
    def __init__(self, left_tuple_dict, right_tuple_dict, matcher_type,
                 debug_result):
        super(DebugWindowManager, self).__init__()
        # Set the tuples and widgets as attributes to the class
        self.left_tuple_dict = left_tuple_dict
        self.right_tuple_dict = right_tuple_dict
        self.matcher_type = matcher_type
        self.debug_result = debug_result
        self.left_tuple_widget = None
        self.right_tuple_widget = None
        self.debug_widget = None
        # Set up the gui
        self.setup_gui()

    def setup_gui(self):
        """
        Set up the gui for debugger
        """
        # Set the parameters and set the window title
        self.left_tuple_widget = DictTableViewWithLabel(self,
                                                        self.left_tuple_dict,
                                                        'Left Tuple')
        self.right_tuple_widget = DictTableViewWithLabel(self,
                                                         self.right_tuple_dict,
                                                         'Right Tuple')
        self.setWindowTitle('Debug Tuples')

        # Create a Tree view object to be embedded in the window
        self.debug_widget = TreeViewWithLabel(self, "Tree details",
                                              type=self.matcher_type,
                                              debug_result=self.debug_result
                                              )
        # Show the tree and the have a layout to show the tuples
        layout = QtWidgets.QHBoxLayout()
        splitter1 = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter1.addWidget(self.left_tuple_widget)
        splitter1.addWidget(self.right_tuple_widget)

        splitter2 = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        splitter2.addWidget(splitter1)
        splitter2.addWidget(self.debug_widget)
        layout.addWidget(splitter2)
        # Set the layout correctly.
        self.setLayout(layout)

