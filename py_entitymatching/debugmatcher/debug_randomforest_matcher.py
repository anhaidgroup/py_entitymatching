"""
This module contains functions for debugging random fores matcher.
"""
import logging

import pandas as pd

from py_entitymatching.debugmatcher.debug_decisiontree_matcher import \
    _debug_decisiontree_matcher, _get_prob
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def debug_randomforest_matcher(random_forest, tuple_1, tuple_2,
                               feature_table, table_columns,
                               exclude_attrs=None):

    """
    This function is used to debug a random forest matcher using two input
    tuples.

    Specifically, this function takes in two tuples, gets the feature vector
    using the feature table and finally passes it to the random forest  and
    displays the path that the feature vector takes in each of the decision
    trees that make up the random forest matcher.

    Args:
        random_forest (RFMatcher): The input
            random forest object that should be debugged.
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
        >>> rf = em.RFMatcher()
        >>> rf.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> # F is the feature vector got from evaluation set of the labeled data.
        >>> out = rf.predict(table=F, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> # A and B are input tables
        >>> em.debug_randomforest_matcher(rf, A.loc[1], B.loc[2], match_f, H.columns, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')

    """
    # Validate input parameters.
    # # We expect the feature table to be of type pandas DataFrame
    validate_object_type(feature_table, pd.DataFrame, error_prefix='The input feature')

    # Get the classifier based on the input object.
    i = 1
    if isinstance(random_forest, RFMatcher):
        clf = random_forest.clf
    else:
        clf = random_forest

    # Get the feature names based on the table columns and the exclude
    # attributes given
    if exclude_attrs is None:
        feature_names = table_columns
    else:
        cols = [c not in exclude_attrs for c in table_columns]
        feature_names = table_columns[cols]

    # Get the probability
    prob = _get_prob(clf, tuple_1, tuple_2, feature_table, feature_names)

    # Decide prediction based on the probability (i.e num. of trees that said
    #  match over total number of trees).
    prediction = False

    if prob[1] > prob[0]:
        prediction = True
    # Print the result summary.
    print(
        "Summary: Num trees = {0}; Mean Prob. for non-match = {1}; "
        "Mean Prob for match = {2}; "
        "Match status =  {3}".format(
            str(len(clf.estimators_)), str(
                prob[0]), str(prob[1]), str(prediction)))

    print("")
    # Now, for each estimator (i.e the decision tree call the decision tree
    # matcher's debugger
    for estimator in clf.estimators_:
        print("Tree " + str(i))
        i += 1
        _ = _debug_decisiontree_matcher(estimator, tuple_1, tuple_2,
                                        feature_table,
                                        table_columns,
                                        exclude_attrs,
                                        ensemble_flag=True)
        print("")
