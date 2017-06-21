from functools import partial

import pandas as pd
import six
try:
    from PyQt5 import QtCore, QtWidgets
except ImportError:
    raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

import py_entitymatching as em


class DataFrameTableView(QtWidgets.QTableWidget):
    """
    Class implementing DataFrame table view
    """

    def __init__(self, controller, dataframe):
        super(DataFrameTableView, self).__init__()
        # Set the parameters and set up the GUI
        self.controller = controller
        self.dataframe = dataframe
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        self.setup_gui()

    def set_dataframe(self, dataframe):
        # Set the DataFrame
        self.dataframe = dataframe

    def setup_gui(self):
        # Set up the GUI for DataFrame table
        # Set rowcount
        nrows = len(self.dataframe)
        self.setRowCount(nrows)
        # Set col count
        ncols = len(self.dataframe.columns)
        self.setColumnCount(ncols + 2)  # + 2 because of show and debug icons

        # Set headers
        # hHriz. header
        headers = ['Show', 'Debug']
        headers.extend(list(self.dataframe.columns))
        self.setHorizontalHeaderLabels(headers)
        self.horizontalHeader().setStretchLastSection(True)
        # vertic. header
        self.verticalHeader().setVisible(True)

        # populate data
        # print self.dataframe
        if nrows > 0:
            for i in range(nrows):
                for j in range(ncols + 2):
                    if j == 0:
                        button = QtWidgets.QPushButton('Show', self)
                        self.setCellWidget(i, j, button)
                        button.clicked.connect(
                            partial(self.controller.handle_show_button, i))
                    elif j == 1:
                        button = QtWidgets.QPushButton('Debug', self)
                        self.setCellWidget(i, j, button)
                        button.clicked.connect(
                            partial(self.controller.handle_debug_button, i))
                    else:
                        if pd.isnull(self.dataframe.iloc[i, j - 2]):
                            self.setItem(i, j, QtWidgets.QTableWidgetItem(""))
                        else:
                            self.setItem(i, j, QtWidgets.QTableWidgetItem(
                                str(self.dataframe.iloc[i, j - 2])
                            ))
                        self.item(i, j).setFlags(
                            QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)


class DataFrameTableViewWithLabel(QtWidgets.QWidget):
    """
    Class implementing DataFrame table view with the label
    """

    def __init__(self, controller, dataframe, label):
        super(DataFrameTableViewWithLabel, self).__init__()
        # Set the parameters
        self.dataframe = dataframe
        self.label = label
        self.controller = controller
        self.tbl_obj = None
        self.label_obj = None
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        self.setup_gui()

    def set_dataframe(self, data_frame):
        # Set the DataFrame
        self.dataframe = data_frame

    def set_label(self, label):
        # Set the label
        self.label = label

    def setup_gui(self):
        # Setup the GUI
        label = QtWidgets.QLabel(self.label)
        tbl_view = DataFrameTableView(self.controller, self.dataframe)
        self.label_obj = label
        self.tbl_obj = tbl_view
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(tbl_view)
        # Set the layout
        self.setLayout(layout)


class DictTableViewWithLabel(QtWidgets.QWidget):
    """
    Class implementing Dictionary table view with label
    """

    def __init__(self, controller, dictionary, label, combo_box=None):
        super(DictTableViewWithLabel, self).__init__()
        # Set the parameters
        self.dictionary = dictionary
        self.label = label
        self.controller = controller
        self.combo_box = combo_box

        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        app = em._viewapp
        # Set up the GUI
        self.setup_gui()

    def setup_gui(self):
        # Set up the GUI
        # Set up the label
        label = QtWidgets.QLabel(self.label)
        # Create a dict table view
        dict_view = DictTableView(self.controller, self.dictionary,
                                  self.combo_box)
        layout = QtWidgets.QVBoxLayout()
        # Set the label
        layout.addWidget(label)
        layout.addWidget(dict_view)
        # Set the layout
        self.setLayout(layout)


class DictTableView(QtWidgets.QTableWidget):
    """
    Class implementing the Dictionary table view
    """

    def __init__(self, controller, dictionary, combo_box=None):
        super(DictTableView, self).__init__()
        # Set the parameters
        self.controller = controller
        self.dictionary = dictionary
        self.combo_box = combo_box
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        self.setup_gui()

    def set_dictionary(self, dictionary):
        # Set the dictionary
        self.dictionary = dictionary

    def setup_gui(self):
        # Set up the GUI
        # Set the sorting enabled
        self.setSortingEnabled(False)
        # Set the column count to be 1
        self.setColumnCount(1)

        # Get the number of rows
        nrows = len(list(self.dictionary.keys()))
        if self.combo_box is not None:
            nrows += 1
        self.setRowCount(nrows)

        # Horizontal headers
        self.setHorizontalHeaderLabels(['Value'])
        self.horizontalHeader().setStretchLastSection(True)

        # Vertical headers
        h = list(self.dictionary.keys())
        h.append('Show')
        self.setVerticalHeaderLabels(h)

        idx = 0

        for k, v in six.iteritems(self.dictionary):
            self.setItem(idx, 0, QtWidgets.QTableWidgetItem(str(v)))
            idx += 1
        if self.combo_box is not None:
            self.setCellWidget(idx, 0, self.combo_box)


class TreeView(QtWidgets.QTreeWidget):
    """
    Class implementing the Tree view
    """

    def __init__(self, controller, type, debug_result):
        super(TreeView, self).__init__()
        # Initialize the parameters
        self.controller = controller
        self.debug_result = debug_result
        self.type = type
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        app = em._viewapp
        self.setup_gui()

    def setup_gui(self):
        # Set up the GUI, set the header appropriately
        if self.type == 'dt':
            header = QtWidgets.QTreeWidgetItem(["Debug-Tree", "Status", "Predicate",
                                            "Feature value"])
            self.setHeaderItem(header)
            root = self.get_treewidget_items_for_dt()
        elif self.type == 'rf':
            header = QtWidgets.QTreeWidgetItem(["Debug-Tree", "Status", "Predicate",
                                            "Feature value"])
            self.setHeaderItem(header)
            root = self.get_treewidget_items_for_rf()
        elif self.type == 'rm':
            header = QtWidgets.QTreeWidgetItem(["Debug-Rules", "Status",
                                            "Conjunct", "Feature value"])
            self.setHeaderItem(header)
            root = self.get_treewidget_items_for_rm()
        else:
            raise TypeError('Unknown matcher type ')

    def get_treewidget_items_for_dt(self):
        """
        Get treewidget iterms for decision tree
        """
        # This is going to create a tree widget for debugging dt
        # matcher.
        overall_status = self.debug_result[0]
        node_list = self.debug_result[1]
        root = QtWidgets.QTreeWidgetItem(self, ['Nodes', str(overall_status), '',
                                            ''])
        idx = 0
        for ls in node_list:
            temp = QtWidgets.QTreeWidgetItem(root, ['', '', '', ''])
            temp = QtWidgets.QTreeWidgetItem(root, ['Node '
                                                + str(idx + 1), str(ls[0]),
                                                str(ls[1]), str(ls[2])])

            idx += 1
        return root

    def get_treewidget_items_for_rf(self):
        """
        Get treewidget iterms for random forest
        """
        # This is going to create a tree widget for debugging rf
        # matcher.

        overall_status = self.debug_result[0]
        consol_node_list = self.debug_result[1]
        root = QtWidgets.QTreeWidgetItem(self,
                                     ['Trees(' + str(len(consol_node_list))
                                      + ')', str(overall_status), '', ''])
        tree_idx = 1
        for node_list in consol_node_list:
            sub_root = QtWidgets.QTreeWidgetItem(root, ['', '', '', ''])
            sub_root = QtWidgets.QTreeWidgetItem(root, ['Tree ' + str(tree_idx),
                                                    str(node_list[0]), '', ''])

            node_idx = 1
            for ls in node_list[1]:
                temp = QtWidgets.QTreeWidgetItem(sub_root, ['', '', '', ''])
                temp = QtWidgets.QTreeWidgetItem(sub_root, ['Node ' + str(node_idx),
                                                        str(ls[0]), str(ls[1]),
                                                        str(ls[2])])
                node_idx += 1
            tree_idx += 1
        return root

    def get_treewidget_items_for_rm(self):
        """
        Get treewidget iterms for rule based matcher forest
        """
        # This is going to create a tree widget for debugging rule based matcher
        # matcher.

        overall_status = self.debug_result[0]
        consol_rule_list = self.debug_result[1]
        root = QtWidgets.QTreeWidgetItem(self, ['Rules(' + str(
            len(consol_rule_list)) + ')', str(overall_status),
                                            '', ''])
        rule_idx = 1
        for rule_list in consol_rule_list:
            sub_root = QtWidgets.QTreeWidgetItem(root, ['', '', '', ''])
            sub_root = QtWidgets.QTreeWidgetItem(root, ['Rule ' + str(rule_idx),
                                                    str(rule_list[0]), '', ''])

            node_idx = 1
            for ls in rule_list[1]:
                temp = QtWidgets.QTreeWidgetItem(sub_root, ['', '', '', ''])
                temp = QtWidgets.QTreeWidgetItem(sub_root, ['Conjunct ' +
                                                        str(node_idx),
                                                        str(ls[0]), str(ls[1]),
                                                        str(ls[2])])
                node_idx += 1
            rule_idx += 1
        return root


class TreeViewWithLabel(QtWidgets.QWidget):
    """
    Class implementing Tree view with label
    """

    def __init__(self, controller, label, type, debug_result):
        super(TreeViewWithLabel, self).__init__()
        # Initialize the parameters
        self.type = type
        self.debug_result = debug_result
        self.label = label
        self.controller = controller
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])

        # Set up the GUI
        self.setup_gui()

    def setup_gui(self):
        label = QtWidgets.QLabel(self.label)
        tree_view = TreeView(self.controller, self.type, self.debug_result)
        # Set up the GUI with tree and label
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(tree_view)
        # Set the layout
        self.setLayout(layout)
