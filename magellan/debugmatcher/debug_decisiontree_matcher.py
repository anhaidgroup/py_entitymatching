import subprocess
import logging

import numpy as np
import pandas as pd
import six
from sklearn.tree import export_graphviz

from magellan.feature.extractfeatures import apply_feat_fns
from magellan.matcher.dtmatcher import DTMatcher

logger = logging.getLogger(__name__)

def visualize_tree(dt, fv_columns, exclude_attrs, create_file=True):
    """Create tree png using graphviz.

    """
    if isinstance(dt, DTMatcher):
        tree = dt.clf
    else:
        tree = dt
    if exclude_attrs is None:
        feature_names = fv_columns
    else:
        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]

    with open("dt_.dot", 'w') as f:
        export_graphviz(tree, out_file=f,
                        feature_names=feature_names)

    command = ["dot", "-Tpng", "dt_.dot", "-o", "dt_.png"]
    try:
        subprocess.check_call(command)
    except:
        logger.error("Could not run dot, ie graphviz, to "
             "produce visualization")
        return
    print("Execute the following command in IPython command prompt:")
    print("")
    print("from IPython.display import Image")
    print("Image(filename='dt_.png') ")


def get_code(tree, feature_names, target_names,
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

            code_str = spacer + spacer_base + "print( \'" + spacer_base + "" + features[
                node] + " <= " + str(
                threshold[node]) + \
                       " is True " + "(  value : \'  + str(" + str(features[node]) + ") + \')\')"
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
            code_str = spacer + spacer_base + "print( \'" + spacer_base + "" + features[
                node] + " <= " + str(
                threshold[node]) + \
                       " is False " + "(  value : \'  + str(" + str(features[node]) + ") + \')\')"
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
            code_str = spacer + "return " + str(winning_target_name) + "" + \
                       " #( " + str(winning_target_count) + " examples )"
            code_list.append(code_str)
            # print(spacer + "return " + str(target_name) + \
            #       " #( " + str(target_count) + " examples )")

    recurse(left, right, threshold, features, 0, 0)
    return code_list


def debug_decisiontree_matcher(dt, t1, t2, feat_table, fv_columns, exclude_attrs):
    _debug_decisiontree_matcher(dt, t1, t2, feat_table, fv_columns, exclude_attrs,
                                ensemble_flag=False)

def _debug_decisiontree_matcher(dt, t1, t2, feat_table, fv_columns, exclude_attrs,
                                ensemble_flag=False):
    if isinstance(dt, DTMatcher):
        clf = dt.clf
    else:
        clf = dt

    if exclude_attrs is None:
        feature_names = fv_columns
    else:
        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]

    code = get_code(clf, feature_names, ['False', 'True'])
    feat_vals = apply_feat_fns(t1, t2, feat_table)
    code = get_dbg_fn(code)
    d = {}
    d.update(feat_vals)
    six.exec_(code, d)
    ret_val = d['debug_fn']()
    if ensemble_flag is True:
        spacer = "    "
    else:
        spacer = ""

    if ensemble_flag is True:
        p = get_prob(clf, t1, t2, feat_table, feature_names)
        print(spacer + "Prob. for non-match : " + str(p[0]))
        print(spacer + "Prob for match : " + str(p[1]))
        return p
    else:
        print(spacer + "Match status : " + str(ret_val))


def get_prob(clf, t1, t2, feat_table, feature_names):
    feat_values = apply_feat_fns(t1, t2, feat_table)
    feat_values = pd.Series(feat_values)
    feat_values = feat_values[feature_names]
    v = feat_values.values
    v = v.reshape(1, -1)
    p = clf.predict_proba(v)
    return p[0]


def get_dbg_fn(code):
    spacer_basic = '    '
    c = "def debug_fn(): \n"
    upd_code = [spacer_basic + e + "\n" for e in code]
    c = c + ''.join(upd_code)
    return c
