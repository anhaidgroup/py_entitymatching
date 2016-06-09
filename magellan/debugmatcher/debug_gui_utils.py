import pandas as pd
from collections import OrderedDict
import numpy as np

import magellan.catalog.catalog_manager as cm

def get_metric(summary_stats):
    d = OrderedDict()
    keys = summary_stats.keys()
    mkeys = [k for k in keys if k not in ['false_pos_ls', 'false_neg_ls']]
    p = round(summary_stats['precision']*100, 2)
    pn = int(summary_stats['prec_numerator'])
    pd = int(summary_stats['prec_denominator'])
    d['Precision'] = str(p) +"% (" + str(pn) +"/" + str(pd) +")"
    r = round(summary_stats['recall']*100, 2)
    rn = int(summary_stats['recall_numerator'])
    rd = int(summary_stats['recall_denominator'])
    d['Recall'] = str(r)+"% (" + str(rn) +"/" + str(rd) +")"
    f1 = round(summary_stats['f1']*100, 2)
    d['F1'] = str(f1) +"%"
    ppos_num = int(summary_stats['pred_pos_num'])
    fpos_num = int(summary_stats['false_pos_num'])
    d['False positives']=str(fpos_num) + " (out of " + str(ppos_num) + " positive predictions)"
    pneg_num = int(summary_stats['pred_neg_num'])
    fneg_num = int(summary_stats['false_neg_num'])
    d['False negatives'] = str(fneg_num) + " (out of " + str(pneg_num) + " negative predictions)"

    return d

def get_dataframe(table, ls):
    # table = table.copy()
    ret = pd.DataFrame(columns=table.columns.values)
    if len(ls) > 0:
        fk_ltable = cm.get_fk_ltable(table)
        fk_rtable = cm.get_fk_rtable(table)
        table = table.set_index([fk_ltable, fk_rtable], drop=False)
        d = table.ix[ls]
        ret = d
        ret.reset_index(inplace=True, drop=True)
    return ret


def get_code_vis(tree, feature_names, target_names,
                 spacer_base="    "):
    """Produce psuedo-code for decision tree.

    Args
    ----
    tree -- scikit-leant DescisionTree.
    feature_names -- list of feature names.
    target_names -- list of target (class) names.
    spacer_base -- used for spacing code (default: "    ").

    Notes
    -----
    based on http://stackoverflow.com/a/30104792.
    """
    left = tree.tree_.children_left
    right = tree.tree_.children_right
    threshold = tree.tree_.threshold
    features = [feature_names[i] for i in tree.tree_.feature]
    value = tree.tree_.value

    code_list = []

    def recurse(left, right, threshold, features, node, depth):
        spacer = spacer_base * depth
        if (threshold[node] != -2):
            code_str = spacer + "if ( " + features[node] + " <= " + \
                       str(threshold[node]) + " ):"
            code_list.append(code_str)
            # print(spacer + "if ( " + features[node] + " <= " + \
            #       str(threshold[node]) + " ):")

            # This code makes sense for printing the predicate
            # code_str = spacer + spacer_base + "print \'" + spacer_base + "" + features[node] + " <= " + str(
            #     threshold[node]) + \
            #            " is True " + "(  value : \'  + str(" + str(features[node]) + ") + \')\'"
            code_str = spacer + spacer_base + "node_list.append([True, \'" + str(features[node]) + " <= " + \
                       str(threshold[node]) + "\', " + str(features[node]) + "])"

            code_list.append(code_str)



            # print(spacer + spacer_base + "print \'" + features[node] + " <= " + str(threshold[node]) +
            #       " PASSED " + "(  value : \'  + str(" +  str(features[node])  + ") + \')\'")
            if left[node] != -1:
                recurse(left, right, threshold, features,
                        left[node], depth + 1)
            # print(spacer + "}\n" + spacer +"else:")
            code_str = spacer + "else:"
            code_list.append(code_str)
            # print(spacer + "else:")

            # code_str = spacer + spacer_base + "print \'" + spacer_base + "" + features[node] + " <= " + str(
            #     threshold[node]) + \
            #            " is False " + "(  value : \'  + str(" + str(features[node]) + ") + \')\'"

            code_str = spacer + spacer_base + "node_list.append([False, \'" + str(features[node]) + " <= " + \
                       str(threshold[node]) + "\', " + str(features[node]) + "])"

            code_list.append(code_str)
            # print(spacer + spacer_base + "print \'" + features[node] + " <= " + str(threshold[node]) +
            #       " FAILED " + "(  value : \'  + str(" +  str(features[node])  + ") + \')\'")
            if right[node] != -1:
                recurse(left, right, threshold, features,
                        right[node], depth + 1)
                # print(spacer + "}")
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

                # print(spacer + "return " + str(target_name) + \
                #       " ( " + str(target_count) + " examples )")
            code_str = spacer + "return " + str(winning_target_name) + ", node_list" + \
                           " #( " + str(winning_target_count) + " examples )"
            code_list.append(code_str)
            # print(spacer + "return " + str(target_name) + \
            #       " #( " + str(target_count) + " examples )")

    recurse(left, right, threshold, features, 0, 0)
    return code_list


def get_dbg_fn_vis(code):
    spacer_basic = '    '
    c = "def debug_fn(): \n"
    c += spacer_basic + "node_list = []\n"
    upd_code = [spacer_basic + e + "\n" for e in code]
    c = c + ''.join(upd_code)
    return c


def get_name_for_predict_column(columns):
    k = '_predicted'
    i = 0
    # try attribute name of the form "_id", "_id0", "_id1", ... and
    # return the first available name
    while True:
        if k not in columns:
            break
        else:
            k = '_predicted' + str(i)
        i += 1
    return k
