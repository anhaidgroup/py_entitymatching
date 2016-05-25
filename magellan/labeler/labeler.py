import logging
import magellan.core.catalog_manager as cm
def label_table(table, col_name, replace=True):
    from magellan.gui.table_gui import edit_table
    lbl_table = table.copy()

    if col_name in lbl_table.columns:
        if replace == True:
            logging.getLogger(__name__).warning('Input table already contains column %s. '
                                                '' %col_name)
            lbl_table[col_name] = 0
    else:
        lbl_table[col_name] = 0
    edit_table(lbl_table)
    lbl_table[col_name] = lbl_table[col_name].astype(int)
    # check if the table contains only 0s and 1s
    c1 = lbl_table[col_name] == 1
    c2 = lbl_table[col_name] == 0
    c = sum(c1|c2)
    assert c == len(lbl_table), 'The label column contains values other than 0 and 1'

    cm.init_properties(lbl_table)

    cm.copy_properties(table, lbl_table)
    return lbl_table