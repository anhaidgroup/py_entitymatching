"""
This module contains utility functions for debugging matchers.
"""
import pandas as pd
from collections import OrderedDict
import numpy as np

import py_entitymatching.catalog.catalog_manager as cm

def _get_metric(summary_stats):
    """
    This function formats the evaluation summary statistics in a way that
    can be displayed in the debugger window.
    """
    # Initialize a dictionary to hold the summary statistics.
    d = OrderedDict()
    keys = summary_stats.keys()
    _ = [k for k in keys if k not in ['false_pos_ls', 'false_neg_ls']]

    # Get the precision from the evaluation summary  and format it for display.
    p = round(summary_stats['precision']*100, 2)
    pn = int(summary_stats['prec_numerator'])
    pd = int(summary_stats['prec_denominator'])
    d['Precision'] = str(p) +"% (" + str(pn) +"/" + str(pd) +")"

    # Get the recall from the evaluation summary  and format it for display.
    r = round(summary_stats['recall']*100, 2)
    rn = int(summary_stats['recall_numerator'])
    rd = int(summary_stats['recall_denominator'])
    d['Recall'] = str(r)+"% (" + str(rn) +"/" + str(rd) +")"

    # Get the F1 from the evaluation summary  and format it for display.
    f1 = round(summary_stats['f1']*100, 2)
    d['F1'] = str(f1) +"%"

    # Get the false positives from the evaluation summary  and format it for
    # display.
    ppos_num = int(summary_stats['pred_pos_num'])
    fpos_num = int(summary_stats['false_pos_num'])
    d['False positives']=str(fpos_num) + " (out of " + str(ppos_num) + \
                         " positive predictions)"

    # Get the false negatives from the evaluation summary  and format it for
    # display.
    pneg_num = int(summary_stats['pred_neg_num'])
    fneg_num = int(summary_stats['false_neg_num'])
    d['False negatives'] = str(fneg_num) + " (out of " + str(pneg_num) + \
                           " negative predictions)"

    # Finally return the updated dictionary.
    return d

def _get_dataframe(table, ls):
    """
    This function returns the selection of the table based on ls.
    Specifically, ls is expected to contain a list of fk_ltable and fk_rtables.
    """

    # table = table.copy()
    # Get the column values.
    ret = pd.DataFrame(columns=table.columns.values)
    # If the list has some values then proceed
    if len(ls) > 0:
        fk_ltable = cm.get_fk_ltable(table)
        fk_rtable = cm.get_fk_rtable(table)
        # Set the index on fk_ltable, fk_rtable
        table = table.set_index([fk_ltable, fk_rtable], drop=False)
        # Do the selection
        d = table.loc[ls]
        ret = d
        ret.reset_index(inplace=True, drop=True)
    # Finally return the selected values
    return ret


def _get_code_vis(tree, feature_names, target_names,
                  spacer_base="    "):

    """
    Produce psuedo-code for decision tree.
    Note: This is based on http://stackoverflow.com/a/30104792.

    """
    # Get the left, right trees and the threshold from the tree
    left = tree.tree_.children_left
    right = tree.tree_.children_right
    threshold = tree.tree_.threshold

    # Get the features from the tree
    features = [feature_names[i] for i in tree.tree_.feature]
    value = tree.tree_.value

    code_list = []

    # Now recursively build the tree by going through each node.
    def recurse(left, right, threshold, features, node, depth):
        """
        Recurse function to encode the debug logic at each node.
        """

        spacer = spacer_base * depth

        # For each of the threshold conditions, add appropriate code that
        # should be executed.
        if threshold[node] != -2:
            code_str = spacer + "if ( " + features[node] + " <= " + \
                       str(threshold[node]) + " ):"
            code_list.append(code_str)
            code_str = spacer + spacer_base + "node_list.append([True, \'" + str(features[node]) + " <= " + \
                       str(threshold[node]) + "\', " + str(features[node]) + "])"

            code_list.append(code_str)
            if left[node] != -1:
                recurse(left, right, threshold, features,
                        left[node], depth + 1)
            code_str = spacer + "else:"
            code_list.append(code_str)
            code_str = spacer + spacer_base + "node_list.append([False, \'" + str(features[node]) + " <= " + \
                       str(threshold[node]) + "\', " + str(features[node]) + "])"

            code_list.append(code_str)
            if right[node] != -1:
                recurse(left, right, threshold, features,
                        right[node], depth + 1)
        else:
            target = value[node]
            winning_target_name = None
            winning_target_count = None
            for i, v in zip(np.nonzero(target)[1],
                            target[np.nonzero(target)]):
                target_name = target_names[i]
                target_count = int(v)
                if winning_target_count == None:
                    winning_target_count = target_count
                    winning_target_name = target_name

                elif target_count > winning_target_count:
                    winning_target_count = target_count
                    winning_target_name = target_name
            code_str = spacer + "return " + str(winning_target_name) + ", node_list" + \
                           " #( " + str(winning_target_count) + " examples )"
            code_list.append(code_str)

    recurse(left, right, threshold, features, 0, 0)
    return code_list


def _get_dbg_fn_vis(code):
    """
    Create a wrapper for the python statements, that encodes the debugging
    logic for the matcher.
    """

    spacer_basic = '    '
    wrapped_code = "def debug_fn(): \n"
    wrapped_code += spacer_basic + "node_list = []\n"
    upd_code = [spacer_basic + e + "\n" for e in code]
    wrapped_code = wrapped_code + ''.join(upd_code)
    return wrapped_code


def get_name_for_predict_column(columns):
    """
    Get a unique name for predicted column.
    """
    # As of now, the predicted column will take the form
    # constant string + number

    # The constant string is fixed as '_predicted'
    k = '_predicted'
    i = 0
    # Try attribute name of the form "_id", "_id0", "_id1", ... and
    # return the first available name
    while True:
        if k not in columns:
            break
        else:
            k = '_predicted' + str(i)
        i += 1
    # Finally, return the column name
    return k
