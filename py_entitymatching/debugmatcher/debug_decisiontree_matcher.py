# coding=utf-8
"""
This module contains functions for debugging decision tree matcher.
"""
import logging
import subprocess

import pandas as pd
import numpy as np
import six
from py_entitymatching.utils.validation_helper import validate_object_type
from sklearn.tree import export_graphviz

from py_entitymatching.feature.extractfeatures import apply_feat_fns
from py_entitymatching.matcher.dtmatcher import DTMatcher

logger = logging.getLogger(__name__)


def visualize_tree(decision_tree, table_columns, exclude_attrs=None):
    """
    This function is used to visualize the decision tree learned from the
    training data using the 'fit' method.

    Note that, this function does not pop up a visualization of a decision tree.
    It creates a png file in the local directory and the user has to
    explicitly open the file to view the tree. More over, this function uses
    'dot'  command and graphviz to create the
    visualization. It is assumed that the 'dot' command is present and
    graphviz is installed in the local machine, which this function is executed.

    Args:
        decision_tree (DTMatcher or DecisionTreeClassifier): The decision tree
            matcher for which the visualization needs to be generated.
        table_columns (list): Attributes that were
            from the input table that was used to train the decision tree.
        exclude_attrs (list): Attributes that should be removed from the
            table columns to get the actual feature vectors (defaults to None).

    """
    # Validate input parameters
    # # We expect the input decision tree to be of type DTMatcher. If so get
    # the classifier out of it.
    if isinstance(decision_tree, DTMatcher):
        tree = decision_tree.clf
    else:
        tree = decision_tree
    # If the exclude attribute is nothing, then all the given columns are
    # feature vectors.
    if exclude_attrs is None:
        feature_names = table_columns
    else:
        # Else pick out the feature vector columns based on the exclude
        # attributes.
        columns = [c not in exclude_attrs for c in table_columns]
        feature_names = table_columns[columns]

    # Create a file (as of now hardcoded) and write the tree into that file.
    with open("dt_.dot", 'w') as f:
        export_graphviz(tree, out_file=f,
                        feature_names=feature_names)

    # Create a png file from the dot file and store it in the same directory
    command = ["dot", "-Tpng", "dt_.dot", "-o", "dt_.png"]
    # noinspection PyBroadException
    try:
        subprocess.check_call(command)
    except:
        logger.error("Could not run dot, ie graphviz, to "
                     "produce visualization")
        return
    # Finally, print a help information on how to display the visualization
    # from the ipython console.
    print("Execute the following command in IPython command prompt:")
    print("")
    print("from IPython.display import Image")
    print("Image(filename='dt_.png') ")


def _get_code(tree, feature_names, target_names,
              spacer_base="    "):
    """
    Produce psuedo-code for decision tree.
    This is  based on http://stackoverflow.com/a/30104792.
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
            code_str = spacer + spacer_base + "print( \'" + spacer_base + "" + \
                       features[
                           node] + " <= " + str(
                threshold[node]) + \
                       " is True " + "(  value : \'  + str(" + str(
                features[node]) + ") + \')\')"
            code_list.append(code_str)

            if left[node] != -1:
                recurse(left, right, threshold, features,
                        left[node], depth + 1)

            code_str = spacer + "else:"
            code_list.append(code_str)

            code_str = spacer + spacer_base + "print( \'" + spacer_base + "" + \
                       features[
                           node] + " <= " + str(
                threshold[node]) + \
                       " is False " + "(  value : \'  + str(" + str(
                features[node]) + ") + \')\')"
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

            code_str = spacer + "return " + str(winning_target_name) + "" + \
                       " #( " + str(winning_target_count) + " examples )"
            code_list.append(code_str)

    recurse(left, right, threshold, features, 0, 0)
    return code_list


def debug_decisiontree_matcher(decision_tree, tuple_1, tuple_2, feature_table,
                               table_columns,
                               exclude_attrs=None):
    """
    This function is used to debug a decision tree matcher using two input
    tuples.

    Specifically, this function takes in two tuples, gets the feature vector
    using the feature table and finally passes it to the decision tree and
    displays the path that the feature vector takes in the decision tree.

    Args:
        decision_tree (DTMatcher): The input
            decision tree object that should be debugged.
        tuple_1,tuple_2 (Series): Input tuples that should be debugged.
        feature_table (DataFrame): Feature table containing the functions
            for the features.
        table_columns (list): List of all columns that will be outputted
            after generation of feature vectors.
        exclude_attrs (list): List of attributes that should be removed from
            the table columns.

    Raises:
        AssertionError: If the input feature table is not of type pandas
            DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # devel is the labeled data used for development purposes, match_f is the feature table
        >>> H = em.extract_feat_vecs(devel, feat_table=match_f, attrs_after='gold_labels')
        >>> dt = em.DTMatcher()
        >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> # F is the feature vector got from evaluation set of the labeled data.
        >>> out = dt.predict(table=F, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> # A and B are input tables
        >>> em.debug_decisiontree_matcher(dt, A.loc[1], B.loc[2], match_f, H.columns, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
    """

    validate_object_type(feature_table, pd.DataFrame, 'The input feature table')

    _debug_decisiontree_matcher(decision_tree, tuple_1, tuple_2, feature_table,
                                table_columns,
                                exclude_attrs,
                                ensemble_flag=False)


def _debug_decisiontree_matcher(decision_tree, tuple_1, tuple_2,
                                feature_table, table_columns,
                                exclude_attrs,
                                ensemble_flag=False):
    """
    This function is used to print the debug information for decision tree
    and random forest matcher.
    """
    # Get the classifier from the input object.
    if isinstance(decision_tree, DTMatcher):
        clf = decision_tree.clf
    else:
        clf = decision_tree

    # Based on the exclude attributes derive the feature names.
    if exclude_attrs is None:
        feature_names = table_columns
    else:
        cols = [c not in exclude_attrs for c in table_columns]
        feature_names = table_columns[cols]

    # Get the python code based on the classifier, feature names and the
    # boolean results.
    code = _get_code(clf, feature_names, ['False', 'True'])
    # Apply feature functions to get feature vectors.
    feature_vectors = apply_feat_fns(tuple_1, tuple_2, feature_table)

    # Wrap the code in a a function
    code = _get_dbg_fn(code)

    # Initialize a dictionary with the given feature vectors. This is
    # important because the code must be linked with the values in the
    # feature vectors.
    code_dict = {}
    code_dict.update(feature_vectors)
    six.exec_(code, code_dict)
    ret_val = code_dict['debug_fn']()
    # Based on the ensemble flag, indent the output (as in RF, we need to
    # indent it a bit further right).
    if ensemble_flag is True:
        spacer = "    "
    else:
        spacer = ""

    # Further, if the ensemble flag is True, then print the prob. for match
    # and non-matches.
    if ensemble_flag is True:
        p = _get_prob(clf, tuple_1, tuple_2, feature_table, feature_names)
        print(spacer + "Prob. for non-match : " + str(p[0]))
        print(spacer + "Prob for match : " + str(p[1]))
        return p
    else:
        # Else, just print the match status.
        print(spacer + "Match status : " + str(ret_val))


def _get_prob(clf, t1, t2, feature_table, feature_names):
    """
    Get the probability of the match status.
    """
    # Get the feature vectors from the feature table and the input tuples.
    feat_values = apply_feat_fns(t1, t2, feature_table)
    feat_values = pd.Series(feat_values)
    feat_values = feat_values[feature_names]
    v = feat_values.values
    v = v.reshape(1, -1)
    # Use the classifier to predict the probability.
    p = clf.predict_proba(v)
    return p[0]


def _get_dbg_fn(code):
    """
    Create a wrapper for the python statements, that encodes the debugging
    logic for the decision tree.
    """
    spacer_basic = '    '
    # Create a function.
    wrapper_code = "def debug_fn(): \n"
    # For each line, add a tab in front and a newline at the end.
    upd_code = [spacer_basic + e + "\n" for e in code]
    # Finally, join everything
    wrapper_code += ''.join(upd_code)

    # Finally return the wrapped up code.
    return wrapper_code
