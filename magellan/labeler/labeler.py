import logging
import pandas as pd
import six
import magellan.catalog.catalog_manager as cm

logger = logging.getLogger(__name__)
def label_table(table, col_name, replace=True):

    if not isinstance(table, pd.DataFrame):
        logger.error('Input object is not of type data frame')
        raise AssertionError('Input object is not of type data frame')
    if not isinstance(col_name, six.string_types):
        logger.error('Input attr. is not of type string')
        raise AssertionError('Input attr. is not of type string')


    from magellan.gui.table_gui import edit_table
    lbl_table = table.copy()

    if col_name in lbl_table.columns:
        if replace == True:
            logger.warning('Input table already contains column %s. '
                                                '' %col_name)
            lbl_table[col_name] = 0
        else:
            logger.error('Input table already contains column %s. '
                                                '' %col_name)
            raise AssertionError('Input table already contains column %s. '
                                                '' %col_name)
    else:
        lbl_table[col_name] = 0
    edit_table(lbl_table)
    lbl_table[col_name] = lbl_table[col_name].astype(int)
    # check if the table contains only 0s and 1s
    c1 = lbl_table[col_name] == 1
    c2 = lbl_table[col_name] == 0
    c = sum(c1|c2)
    if not c == len(lbl_table):
        logger.error('The label column contains values other than 0 and 1')
        raise AssertionError('The label column contains values other than 0 and 1')

    cm.init_properties(lbl_table)
    cm.copy_properties(table, lbl_table)

    return lbl_table