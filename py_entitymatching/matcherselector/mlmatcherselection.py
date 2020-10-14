"""
This module contains functions for selecting a ML-matcher.
"""
import logging
from collections import OrderedDict

import pandas as pd
import numpy as np
from sklearn.model_selection import KFold, cross_val_score

from py_entitymatching.utils.catalog_helper import check_attrs_present
from py_entitymatching.utils.generic_helper import list_diff, list_drop_duplicates
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def select_matcher(matchers, x=None, y=None, table=None, exclude_attrs=None,
                   target_attr=None,
                   metric_to_select_matcher='precision',
                   metrics_to_display=['precision', 'recall', 'f1'],
                   k=5, n_jobs=-1, random_state=None):
    """
    This function selects a matcher from a given list of matchers based on a
    given metric.
    
    Specifically, this function internally uses scikit-learn's
    cross validation function to select a matcher. There are two ways the
    user can call the fit method. First, interface similar to scikit-learn
    where the feature vectors and target attribute given as projected DataFrame.
    Second, give the DataFrame and explicitly specify the feature vectors
    (by specifying the attributes to be excluded) and the target attribute

    A point to note is all the input parameters have a default value of
    None. This is done to support both the interfaces in a single function.

    Args:
        matchers (MLMatcher): List of ML matchers to be selected from.
        x (DataFrame): Input feature vectors given as pandas DataFrame (
            defaults to None).
        y (DatFrame): Input target attribute given as pandas
            DataFrame with a single column (defaults to None).
        table (DataFrame): Input pandas DataFrame containing feature
            vectors and target attribute (defaults to None).
        exclude_attrs (list): The list of attributes that should be
            excluded from the input table to get the feature vectors.
        target_attr (string): The target attribute in the input table (defaults
            to None).
        metric_to_select_matcher (string): The metric based on which the matchers must be
            selected. The string can be one of 'precision', 'recall',
            'f1' (defaults to 'precision').
        metrics_to_display (list): The metrics that will be displayed to
            the user. It should be a list of any of the strings 'precision',
            'recall', or 'f1' (defaults to ['precision', 'recall', 'f1']).
        k (int): The k value for cross-validation (defaults to 5).
        n_jobs (integer): The number of CPUs to use to do the computation.
            -1 means 'all CPUs (defaults to -1)'.
        random_state (object): Pseudo random number generator that should be
            used for splitting the data into folds (defaults to None).

    Returns:

        A dictionary containing three keys - selected matcher, cv_stats, and drill_down_cv_stats.

        The selected matcher has a value that is a matcher (MLMatcher) object,
        cv_stats is a Dataframe containing average metrics for each matcher,
        and drill_down_cv_stats is a dictionary containing a table for each metric
        the user wants to display containing the score of the matchers for each fold.

     Raises:
        AssertionError: If `metric_to_select_matcher` is not one of 'precision', 'recall',
            or 'f1'.
        AssertionError: If each item in the list `metrics_to_display` is not one of
            'precision', 'recall', or 'f1'.

    Examples:
        >>> dt = em.DTMatcher()
        >>> rf = em.RFMatcher()
        # train is the feature vector containing user labels
        >>> result = em.select_matcher(matchers=[dt, rf], table=train, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels', k=5)

    """
    # Check that metrics_to_display is valid
    if not isinstance(metrics_to_display, list):
        metrics_to_display = [metrics_to_display]
    for met in metrics_to_display:
        if met not in ['precision', 'recall', 'f1']:
            logger.error('Metrics must be either "precision", "recall", or "f1".')
            raise AssertionError('Metrics must be either "precision", "recall", or "f1".')
    # Check that metric_to_select_matcher is valid
    if metric_to_select_matcher not in ['precision', 'recall', 'f1']:
        logger.error('Metric must be either "precision", "recall", or "f1".')
        raise AssertionError('Metric must be either "precision", "recall", or "f1".')

    # Based on the input, get the x, y data that can be used to call the
    # scikit-learn's cross validation method
    x, y = _get_xy_data(x, y, table, exclude_attrs, target_attr)
    max_score = 0
    # Initialize the best matcher. As of now set it to be the first matcher.
    sel_matcher = matchers[0]
    # Fix the header
    header = ['Name', 'Matcher', 'Num folds']
    # # Append the folds
    fold_header = ['Fold ' + str(i + 1) for i in range(k)]
    header.extend(fold_header)
    # Finally, append the score.
    header.append('Mean score')
    # Initialize the drill_down_cv_stats dictionary
    drill_down_cv_stats = OrderedDict()
    # Initialize the cv_stats_dictionary and set the first key to contain the matcher names
    cv_stats_dict = OrderedDict()
    matcher_names = []
    for m in matchers:
        matcher_names.append(m.get_name())
    cv_stats_dict['Matcher'] = matcher_names

    # Run the cross_validation for each metric
    for met in metrics_to_display:
        dict_list = []
        mean_score_list = []
        # Run the cross validation for each matcher
        for m in matchers:
            # Use scikit learn's cross validation to get the matcher and the list
            #  of scores (one for each fold).
            matcher, scores = cross_validation(m, x, y, met, k, random_state, n_jobs)
            # Fill a dictionary based on the matcher and the scores.
            val_list = [matcher.get_name(), matcher, k]
            val_list.extend(scores)
            val_list.append(np.mean(scores))
            d = OrderedDict(zip(header, val_list))
            dict_list.append(d)
            # Create a list of the mean scores for each matcher
            mean_score_list.append(np.mean(scores))
            # Select the matcher based on the mean score, but only for metric_to_select_matcher
            if met == metric_to_select_matcher and np.mean(scores) > max_score:
                sel_matcher = m
                max_score = np.mean(scores)
        # Create a DataFrame based on the list of dictionaries created
        stats = pd.DataFrame(dict_list)
        stats = stats[header]
        drill_down_cv_stats[met] = stats
        # Add the mean scores for this metric to the cv_stats_dictionary
        cv_stats_dict['Average ' + met] = mean_score_list
    res = OrderedDict()
    # Add selected matcher and the stats to a dictionary.
    res['selected_matcher'] = sel_matcher
    res['cv_stats'] = pd.DataFrame(cv_stats_dict)
    res['drill_down_cv_stats'] = drill_down_cv_stats
    # Result the final dictionary containing selected matcher and the CV
    # statistics.
    return res


def cross_validation(matcher, x, y, metric, k, random_state, n_jobs):
    """
    The function does cross validation for a single matcher
    """
    # Use KFold function from scikit learn to create a ms object that can be
    # used for cross_val_score function.
    cv = KFold(k, shuffle=True, random_state=random_state)
    # Call the scikit-learn's cross_val_score function
    scores = cross_val_score(matcher.clf, x, y, scoring=metric, cv=cv,
                             n_jobs=n_jobs)
    # Finally, return the matcher along with the scores.
    return matcher, scores


def _get_xy_data(x, y, table, exclude_attrs, target_attr):
    """
    Gets the X, Y data from the input based on the given table, the
    exclude attributes and target attribute provided.
    """
    # If x and y is given, call appropriate function
    if x is not None and y is not None:
        return _get_xy_data_prj(x, y)
    # If table, exclude attributes and target attribute are given,
    # call appropriate function
    elif table is not None and exclude_attrs is not None \
            and target_attr is not None:
        return _get_xy_data_ex(table, exclude_attrs, target_attr)
    else:
        # Else, raise a syntax error.
        raise SyntaxError('The arguments supplied does not match '
                          'the signatures supported !!!')


def _get_xy_data_prj(x, y):
    """
    Gets X, Y that can be used for scikit-learn function based on given
    projected tables.
    """
    # If the first column is '_id' the remove it
    if x.columns[0] == '_id':
        logger.warning(
            'Input table contains "_id". Removing this column for processing')
        # Get the values  from the DataFrame
        x = x.values
        x = np.delete(x, 0, 1)
    else:
        x = x.values

    if not isinstance(y, pd.Series):
        logger.error('Target attr is expected to be a pandas series')
        raise AssertionError('Target attr is expected to be a pandas series')
    else:
        # Get the values  from the DataFrame
        y = y.values
    # Finally, return x an y
    return x, y


def _get_xy_data_ex(table, exclude_attrs, target_attr):
    # Validate the input parameters
    # # We expect the input table to be of type pandas DataFrame
    validate_object_type(table, pd.DataFrame)
    # We expect exclude attributes to be of type list. If not convert it into
    #  a list.
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    # Check if the exclude attributes are present in the input table
    if not check_attrs_present(table, exclude_attrs):
        logger.error('The attributes mentioned in exclude_attrs '
                     'is not present '
                     'in the input table')
        raise AssertionError(
            'The attributes mentioned in exclude_attrs '
            'is not present '
            'in the input table')
    # Check if the target attribute is present in the input table
    if not check_attrs_present(table, target_attr):
        logger.error('The target_attr is not present in the input table')
        raise AssertionError(
            'The target_attr is not present in the input table')

    # Drop the duplicates from the exclude attributes
    exclude_attrs = list_drop_duplicates(exclude_attrs)

    # Explicitly add the target attribute to exclude attribute (if it is not
    # already present)
    if target_attr not in exclude_attrs:
        exclude_attrs.append(target_attr)

    # Project the list of attributes that should be used for scikit-learn's
    # functions.
    attrs_to_project = list_diff(list(table.columns), exclude_attrs)

    # Get the values for x
    x = table[attrs_to_project].values
    # Get the values for x
    y = table[target_attr].values
    y = y.ravel()  # to mute warnings from svm and cross validation
    # Return x and y
    return x, y
