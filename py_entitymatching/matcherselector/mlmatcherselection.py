"""
This module contains functions for selecting a ML-matcher.
"""
import logging
from collections import OrderedDict

import pandas as pd
from sklearn.model_selection import KFold, cross_val_score

from py_entitymatching.utils.catalog_helper import check_attrs_present
from py_entitymatching.utils.generic_helper import list_diff, list_drop_duplicates

logger = logging.getLogger(__name__)


def select_matcher(matchers, x=None, y=None, table=None, exclude_attrs=None,
                   target_attr=None,
                   metric='precision', k=5,
                   random_state=None):
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
        metric (string): The metric based on which the matchers must be
            selected. The string can be one of 'precision', 'recall',
            'f1' (defaults to 'precision').
        k (int): The k value for cross-validation (defaults to 5).
        random_state (object): Pseudo random number generator that should be
            used for splitting the data into folds (defaults to None).

    Returns:

        A dictionary containing two keys - selected matcher and the cv_stats.

        The selected matcher has a value that is a matcher (MLMatcher) object
        and cv_stats has a value that is a dictionary containing
        cross-validation statistics.

    Examples:
        >>> dt = em.DTMatcher()
        >>> rf = em.RFMatcher()
        # train is the feature vector containing user labels
        >>> result = em.select_matcher(matchers=[dt, rf], table=train, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels', k=5)


    """
    # Based on the input, get the x, y data that can be used to call the
    # scikit-learn's cross validation method
    x, y = _get_xy_data(x, y, table, exclude_attrs, target_attr)
    dict_list = []
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

    for m in matchers:
        # Use scikit learn's cross validation to get the matcher and the list
        #  of scores (one for each fold).
        matcher, scores = cross_validation(m, x, y, metric, k, random_state)
        # Fill a dictionary based on the matcher and the scores.
        val_list = [matcher.get_name(), matcher, k]
        val_list.extend(scores)
        val_list.append(pd.np.mean(scores))
        d = OrderedDict(zip(header, val_list))
        dict_list.append(d)
        # Select the matcher based on the mean scoere.
        if pd.np.mean(scores) > max_score:
            sel_matcher = m
            max_score = pd.np.mean(scores)
    # Create a DataFrame based on the list of dictionaries created
    stats = pd.DataFrame(dict_list)
    stats = stats[header]
    res = OrderedDict()
    # Add selected matcher and the stats to a dictionary.
    res['selected_matcher'] = sel_matcher
    res['cv_stats'] = stats
    # Result the final dictionary containing selected matcher and the CV
    # statistics.
    return res


def cross_validation(matcher, x, y, metric, k, random_state):
    """
    The function does cross validation for a single matcher
    """
    # Use KFold function from scikit learn to create a ms object that can be
    # used for cross_val_score function.
    cv = KFold(k, shuffle=True, random_state=random_state)
    # Call the scikit-learn's cross_val_score function
    scores = cross_val_score(matcher.clf, x, y, scoring=metric, cv=cv)
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
        x = pd.np.delete(x, 0, 1)
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
    if not isinstance(table, pd.DataFrame):
        logger.error('Input table is not of type DataFrame')
        raise AssertionError(
            logger.error('Input table is not of type dataframe'))

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
