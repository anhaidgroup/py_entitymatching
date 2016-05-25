from PyQt4 import QtGui, QtCore
from collections import OrderedDict
import pandas as pd

import magellan as mg
import magellan.core.catalog_manager as cm
from  magellan.gui.gui_utils import DictTableViewWithLabel, DataFrameTableViewWithLabel, TreeViewWithLabel


class MainWindowManager(QtGui.QWidget):

    def __init__(self, matcher, matcher_type, exclude_attrs_or_feature_table, dictionary, table, fp_dataframe, fn_dataframe):
        super(MainWindowManager, self).__init__()
        self.matcher = matcher
        self.matcher_type = matcher_type
        self.exclude_attrs_or_feature_table = exclude_attrs_or_feature_table
        self.dictionary = dictionary
        self.table = table
        self.fp_dataframe = fp_dataframe
        self.fn_dataframe = fn_dataframe

        # ltable = self.table.get_property('ltable')
        # rtable = self.table.get_property('rtable')
        ltable = cm.get_property(self.table, 'ltable')
        rtable = cm.get_property(self.table, 'rtable')

        l_df = ltable.to_dataframe()
        r_df = rtable.to_dataframe()

        self.l_df = l_df.set_index(cm.get_key(ltable), drop=False)
        self.r_df = r_df.set_index(cm.get_key(rtable), drop=False)
        self.metric_widget = None
        self.dataframe_widget = None
        self.combo_box = None
        self.current_combo_text = 'False Positives'
        self.current_dataframe = self.fp_dataframe
        self.setup_gui()
        width = min((40 + 1)*105, mg._viewapp.desktop().screenGeometry().width() - 50)
        height = min((50 + 1)*41, mg._viewapp.desktop().screenGeometry().width() - 100)
        self.resize(width, height)


    def setup_gui(self):
        self.combo_box = QtGui.QComboBox()
        self.combo_box.addItems(['False Positives', 'False Negatives'])
        self.combo_box.activated[str].connect(self.combobox_onactivated)

        self.metric_widget = DictTableViewWithLabel(self, self.dictionary, 'Metrics',
                                                    self.combo_box)
        self.dataframe_widget = DataFrameTableViewWithLabel(self,
                                                            self.current_dataframe, self.current_combo_text)
        self.setWindowTitle('Debugger')
        layout = QtGui.QVBoxLayout(self)
        splitter = QtGui.QSplitter(QtCore.Qt.Horizontal)
        splitter.addWidget(self.metric_widget)
        splitter.addWidget(self.dataframe_widget)
        layout.addWidget(splitter)
        self.setLayout(layout)


    # def handle_debug_button(self, index):
    #     # print 'Debug button clicked : ' + str(index)
    #     r = self.current_dataframe.iloc[[index]]
    #     l_fkey = self.table.get_property('foreign_key_ltable')
    #     r_fkey = self.table.get_property('foreign_key_rtable')
    #     l_val = r.ix[r.index.values[0], l_fkey]
    #     r_val = r.ix[r.index.values[0], r_fkey]
    #     d1 = OrderedDict(self.l_df.ix[l_val])
    #     d2 = OrderedDict(self.r_df.ix[r_val])
    #
    #
    #     # if self.matcher_type == 'dt':
    #     #     ret_val, node_list = mg.vis_tuple_debug_dt_matcher(self.matcher, r, self.exclude_attrs_or_feature_table)
    #     # elif self.matcher_type == 'rf':
    #     #     ret_val, node_list = mg.vis_tuple_debug_rf_matcher(self.matcher, r, self.exclude_attrs_or_feature_table)
    #     # elif self.matcher_type == 'rm':
    #     #     ret_val, node_list = mg.vis_tuple_debug_rm_matcher(self.matcher, self.l_df.ix[l_val], self.r_df.ix[r_val],
    #     #                                                        self.exclude_attrs_or_feature_table)
    #     # else:
    #     #     raise TypeError('Unknown matcher type ')
    #     debug_result = [ret_val]
    #     debug_result.append(node_list)
    #     debug_obj = DebugWindowManager(d1, d2, self.matcher_type, debug_result)
    #     debug_obj.show()

    def handle_show_button(self, index):
        # print 'Show button clicked : ' + str(index)
        r = self.current_dataframe.iloc[[index]]
        l_fkey = cm.get_property(self.table, 'fk_ltable')
        r_fkey = cm.get_property(self.table, 'fk_rtable')
        l_val = r.ix[r.index.values[0], l_fkey]
        r_val = r.ix[r.index.values[0], r_fkey]
        d1 = OrderedDict(self.l_df.ix[l_val])
        d2 = OrderedDict(self.r_df.ix[r_val])
        show_obj = ShowWindowManager(d1, d2)
        show_obj.show()

    def combobox_onactivated(self, text):
        if text != self.current_combo_text:
            # print text
            if text == 'False Negatives':
                self.current_combo_text = text
                self.current_dataframe = self.fn_dataframe
            else:
                self.current_combo_text = text
                self.current_dataframe = self.fp_dataframe
            self.dataframe_widget.tbl_obj.dataframe = self.current_dataframe
            self.dataframe_widget.tbl_obj.setup_gui()
            self.dataframe_widget.label_obj.setText(text)
            # self.dataframe_widget.setup_gui()


class ShowWindowManager(QtGui.QWidget):
    def __init__(self, left_tuple_dict, right_tuple_dict):
        super(ShowWindowManager, self).__init__()
        self.left_tuple_dict = left_tuple_dict
        self.right_tuple_dict = right_tuple_dict
        self.left_tuple_widget = None
        self.right_tuple_widget = None

        self.setup_gui()

    def setup_gui(self):
        self.left_tuple_widget = DictTableViewWithLabel(self, self.left_tuple_dict, 'Left Tuple')
        self.right_tuple_widget = DictTableViewWithLabel(self, self.right_tuple_dict, 'Right Tuple')
        self.setWindowTitle('Show Tuples')
        layout = QtGui.QVBoxLayout()
        splitter = QtGui.QSplitter(QtCore.Qt.Horizontal)
        splitter.addWidget(self.left_tuple_widget)
        splitter.addWidget(self.right_tuple_widget)
        layout.addWidget(splitter)
        self.setLayout(layout)



class DebugWindowManager(QtGui.QWidget):
    def __init__(self, left_tuple_dict, right_tuple_dict, matcher_type, debug_result):
        super(DebugWindowManager, self).__init__()
        self.left_tuple_dict = left_tuple_dict
        self.right_tuple_dict = right_tuple_dict
        self.matcher_type = matcher_type
        self.debug_result = debug_result
        self.left_tuple_widget = None
        self.right_tuple_widget = None
        self.debug_widget = None
        self.setup_gui()

    def setup_gui(self):
        self.left_tuple_widget = DictTableViewWithLabel(self, self.left_tuple_dict, 'Left Tuple')
        self.right_tuple_widget = DictTableViewWithLabel(self, self.right_tuple_dict, 'Right Tuple')
        self.setWindowTitle('Debug Tuples')
        # self.debug_widget = DictTableViewWithLabel(self, self.debug_result, 'Debug Result')
        self.debug_widget = TreeViewWithLabel(self, "Tree details", type=self.matcher_type,
                                              debug_result = self.debug_result
                                              )

        layout = QtGui.QHBoxLayout()
        splitter1 = QtGui.QSplitter(QtCore.Qt.Vertical)
        splitter1.addWidget(self.left_tuple_widget)
        splitter1.addWidget(self.right_tuple_widget)

        splitter2 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        splitter2.addWidget(splitter1)
        splitter2.addWidget(self.debug_widget)
        layout.addWidget(splitter2)
        self.setLayout(layout)



