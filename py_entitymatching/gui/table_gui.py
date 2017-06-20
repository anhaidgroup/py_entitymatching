import logging
# from builtins import str

import six
import sys

import py_entitymatching as em
from py_entitymatching.utils.generic_helper import remove_non_ascii

logger = logging.getLogger(__name__)


def view_table(table, edit_flag=False, show_flag=True):
    """
    This function opens up the window to view the table
    Args:
        table (DataFrame): Input pandas DataFrame that should be displayed.
        edit_flag (boolean): Flag to indicate whether editing should be
            allowed.
        show_flag (boolean): Flag to indicate whether the window should be
        displayed

    """
    try:
        from PyQt5 import QtWidgets
    except ImportError:
        raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

    em._viewapp = QtWidgets.QApplication.instance()
    if em._viewapp is None:
        em._viewapp = QtWidgets.QApplication([])
    app = em._viewapp

    datatable = QtWidgets.QTableWidget()

    # disable edit
    if edit_flag == False:
        datatable.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

    datatable.setRowCount(len(table.index))
    datatable.setColumnCount(len(table.columns))

    # set data
    for i in range(len(table.index)):
        for j in range(len(table.columns)):
            datatable.setItem(i, j,
                              QtWidgets.QTableWidgetItem(str(table.iat[i, j])))

    list_col = list(table.columns.values)
    datatable.setHorizontalHeaderLabels(list_col)

    # set window size


    width = min((j + 1) * 105, app.desktop().screenGeometry().width() - 50)
    height = min((i + 1) * 41, app.desktop().screenGeometry().width() - 100)
    datatable.resize(width, height)

    # set window title
    datatable.setWindowTitle("Dataframe")

    if show_flag:
        # show window
        datatable.show()
        em._viewapp = QtWidgets.QApplication.instance()
        if em._viewapp is None:
            em._viewapp = QtWidgets.QApplication([])
        app = em._viewapp
        app.exec_()

    if edit_flag:
        return datatable


def edit_table(table, show_flag=True):
    """
    Edit table
    """
    try:
        from PyQt5 import QtGui
    except ImportError:
        raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                      'GUI related functions in py_entitymatching.')

    datatable = view_table(table, edit_flag=True, show_flag=show_flag)
    cols = list(table.columns)
    idxv = list(table.index)
    j = len(table.columns) - 1
    for i in range(len(table.index)):
            val = datatable.item(i, j).text()
            inp = table.iat[i, j]
            val = _cast_val(val, inp)
            table.set_value(idxv[i], cols[j], val)

            

def _cast_val(v, i):
    # need to cast string values from edit window
    if v == "None":
        return None
    elif isinstance(i, bool):
        return bool(v)
    elif isinstance(i, float):
        return float(v)
    elif isinstance(i, int):
        return int(v)
    elif isinstance(i, six.string_types):
        if sys.version_info[0] < 3:
            v = unicode(v.toUtf8(), encoding="UTF-8")
        return v
    elif isinstance(i, object):
        return v
    else:
        logger.warning('Input value did not match any of the known types')
        return v

