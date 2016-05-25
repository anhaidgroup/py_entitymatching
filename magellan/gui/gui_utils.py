from functools import partial
from PyQt4 import QtGui, QtCore, Qt
import pandas as pd

class DataFrameTableView(QtGui.QTableWidget):
    def __init__(self, controller, dataframe):
        super(DataFrameTableView, self).__init__()
        self.controller = controller
        self.dataframe = dataframe
        self.setup_gui()

    def set_dataframe(self, dataframe):
        self.dataframe = dataframe

    def setup_gui(self):
        # set rowcount
        nrows = len(self.dataframe)
        self.setRowCount(nrows)
        # set col count
        ncols = len(self.dataframe.columns)
        self.setColumnCount(ncols + 2) # + 2 because of show and debug icons

        # set headers
        # horiz. header
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
                        button = QtGui.QPushButton('Show', self)
                        self.setCellWidget(i, j, button)
                        button.clicked.connect(partial(self.controller.handle_show_button, i))
                    elif j == 1:
                        button = QtGui.QPushButton('Debug', self)
                        self.setCellWidget(i, j, button)
                        button.clicked.connect(partial(self.controller.handle_debug_button, i))
                    else:
                        if pd.isnull(self.dataframe.iloc(i, j - 2)):
                            self.setItem(i, j, QtGui.QTableWidgetItem(""))
                        else:
                            self.setItem(i, j, QtGui.QTableWidgetItem(
                                str(self.dataframe.iloc[i, j - 2])
                            ))
                        self.item(i, j).setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)




class DataFrameTableViewWithLabel(QtGui.QWidget):
    def __init__(self, controller, dataframe, label):
        super(DataFrameTableViewWithLabel, self).__init__()
        self.dataframe = dataframe
        self.label = label
        self.controller = controller
        self.tbl_obj = None
        self.label_obj = None
        self.setup_gui()

    def set_dataframe(self, data_frame):
        self.dataframe = data_frame

    def set_label(self, label):
        self.label = label

    def setup_gui(self):
        label = QtGui.QLabel(self.label)
        tbl_view = DataFrameTableView(self.controller, self.dataframe)
        self.label_obj = label
        self.tbl_obj = tbl_view
        layout = QtGui.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(tbl_view)
        self.setLayout(layout)

class DictTableViewWithLabel(QtGui.QWidget):
    def __init__(self, controller, dictionary, label, combo_box=None):
        super(DictTableViewWithLabel, self).__init__()
        self.dictionary = dictionary
        self.label = label
        self.controller = controller
        self.combo_box = combo_box
        self.setup_gui()

    def setup_gui(self):
        label = QtGui.QLabel(self.label)
        dict_view = DictTableView(self.controller, self.dictionary,
                                  self.combo_box)
        layout = QtGui.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(dict_view)
        self.setLayout(layout)



class DictTableView(QtGui.QTableWidget):
    def __init__(self, controller, dictionary, combo_box=None):
        super(DictTableView, self).__init__()
        self.controller = controller
        self.dictionary = dictionary
        self.combo_box = combo_box
        self.setup_gui()


    def set_dictionary(self, dictionary):
        self.dictionary = dictionary

    def setup_gui(self):
        #sorting
        self.setSortingEnabled(False)
        self.setColumnCount(1)

        #nrows
        nrows = len(self.dictionary.keys())
        if self.combo_box is not None:
            nrows += 1
        self.setRowCount(nrows)

        # horizontal headers
        self.setHorizontalHeaderLabels(['Value'])
        self.horizontalHeader().setStretchLastSection(True)

        # vertical headers
        h = self.dictionary.keys()
        h.append('Show')
        self.setVerticalHeaderLabels(h)

        idx = 0

        for k, v in self.dictionary.iteritems():
            self.setItem(idx, 0, QtGui.QTableWidgetItem(str(v)))
            idx += 1
        if self.combo_box is not None:
            self.setCellWidget(idx, 0, self.combo_box)

class TreeView(QtGui.QTreeWidget):
    def __init__(self, controller, type, debug_result):
        super(TreeView, self).__init__()
        self.controller = controller
        self.debug_result = debug_result
        self.type = type
        self.setup_gui()

    def setup_gui(self):
        if self.type == 'dt':
            header=QtGui.QTreeWidgetItem(["Debug-Tree","Status","Predicate", "Feature value"])
            self.setHeaderItem(header)   #Another alternative is setHeaderLabels(["Tree","First",...])
            # root = QtGui.QTreeWidgetItem(self, ["Nodes", '', "", ""])
            # # A = QtGui.QTreeWidgetItem(root, ["Node 1"])
            # barA = QtGui.QTreeWidgetItem(root, ["Node 1", "Pass", "name_name_lev > 0.4", "0.7"])
            # barA = QtGui.QTreeWidgetItem(root, ["" "", "", ""])
            # barA = QtGui.QTreeWidgetItem(root, ["Node 2", "Pass", "name_name_mel > 0.6", "0.9"])
            # barA = QtGui.QTreeWidgetItem(root, ["" "", "", ""])
            # barA = QtGui.QTreeWidgetItem(root, ["Node 3", "Pass", "zipcode_zipcode_lev > 0.8", "0.94"])
            # barA = QtGui.QTreeWidgetItem(root, ["" "", "", ""])
            # barA = QtGui.QTreeWidgetItem(root, ["Node 4", "Fail", "address_address_lev > 0.6", "0.4"])
            root = self.get_treewidget_items_for_dt()
        elif self.type == 'rf':
            header=QtGui.QTreeWidgetItem(["Debug-Tree","Status","Predicate", "Feature value"])
            self.setHeaderItem(header)   #Another alternative is setHeaderLabels(["Tree","First",...])
            root = self.get_treewidget_items_for_rf()
        elif self.type == 'rm':
            header=QtGui.QTreeWidgetItem(["Debug-Rules","Status","Conjunct", "Feature value"])
            self.setHeaderItem(header)
            root = self.get_treewidget_items_for_rm()
        else:
            raise TypeError('Unknown matcher type ')



    def get_treewidget_items_for_dt(self):
        overall_status = self.debug_result[0]
        node_list = self.debug_result[1]
        root = QtGui.QTreeWidgetItem(self, ['Nodes', str(overall_status), '', ''])
        idx = 0
        for ls in node_list:
            temp = QtGui.QTreeWidgetItem(root, ['','','',''])
            temp = QtGui.QTreeWidgetItem(root, ['Node ' + str(idx+1), str(ls[0]), str(ls[1]), str(ls[2])])

            idx += 1
        return root

    def get_treewidget_items_for_rf(self):
        overall_status = self.debug_result[0]
        consol_node_list = self.debug_result[1]
        root = QtGui.QTreeWidgetItem(self, ['Trees('+str(len(consol_node_list))+')', str(overall_status),'', ''])
        tree_idx = 1
        for node_list in consol_node_list:
            sub_root = QtGui.QTreeWidgetItem(root, ['','','',''])
            sub_root = QtGui.QTreeWidgetItem(root, ['Tree ' + str(tree_idx), str(node_list[0]), '', ''])

            node_idx = 1
            for ls in node_list[1]:
                temp = QtGui.QTreeWidgetItem(sub_root, ['','','',''])
                temp = QtGui.QTreeWidgetItem(sub_root, ['Node ' + str(node_idx), str(ls[0]), str(ls[1]), str(ls[2])])
                node_idx += 1
            tree_idx += 1
        return root

    def get_treewidget_items_for_rm(self):
        overall_status = self.debug_result[0]
        consol_rule_list = self.debug_result[1]
        root = QtGui.QTreeWidgetItem(self, ['Rules('+str(len(consol_rule_list))+')',  str(overall_status),'', ''])
        rule_idx = 1
        for rule_list  in consol_rule_list:
            sub_root = QtGui.QTreeWidgetItem(root, ['','','',''])
            sub_root = QtGui.QTreeWidgetItem(root, ['Rule ' + str(rule_idx), str(rule_list[0]), '', ''])

            node_idx = 1
            for ls in rule_list[1]:
                temp = QtGui.QTreeWidgetItem(sub_root, ['','','',''])
                temp = QtGui.QTreeWidgetItem(sub_root, ['Conjunct ' + str(node_idx), str(ls[0]), str(ls[1]), str(ls[2])])
                node_idx += 1
            rule_idx += 1
        return root


class TreeViewWithLabel(QtGui.QWidget):
    def __init__(self, controller, label, type, debug_result):
        super(TreeViewWithLabel, self).__init__()
        self.type = type
        self.debug_result = debug_result
        self.label = label
        self.controller = controller
        self.setup_gui()

    def setup_gui(self):
        label = QtGui.QLabel(self.label)
        tree_view = TreeView(self.controller, self.type, self.debug_result)
        layout = QtGui.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(tree_view)
        self.setLayout(layout)