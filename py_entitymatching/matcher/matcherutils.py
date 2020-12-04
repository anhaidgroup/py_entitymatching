"""
This module contains some utility functions for the matcher.
"""
import logging
import math
import time
from collections import OrderedDict

import pandas as pd
import numpy as np
import sklearn.model_selection as ms
from  sklearn.impute import SimpleImputer

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
import py_entitymatching.utils.generic_helper as gh

logger = logging.getLogger(__name__)


def split_train_test(labeled_data, train_proportion=0.5,
                     random_state=None, verbose=True):
    """
    This function splits the input data into train and test.

    Specifically, this function is just a wrapper of scikit-learn's
    train_test_split function.

    This function also takes care of copying the metadata from the input
    table to train and test splits.

    Args:
        labeled_data (DataFrame): The input pandas DataFrame that needs to be
            split into train and test.
        train_proportion (float): A number between 0 and 1, indicating the
            proportion of tuples that should be included in the train split (
            defaults to 0.5).
        random_state (object): A number of random number object (as in
            scikit-learn).
        verbose (boolean): A flag to indicate whether the debug information
            should be displayed.

    Returns:

        A Python dictionary containing two keys - train and test.

        The value for the key 'train' is a pandas DataFrame containing tuples
        allocated from the input table based on train_proportion.

        Similarly, the value for the key 'test' is a pandas DataFrame containing
        tuples for evaluation.

        This function sets the output DataFrames (train, test) properties
        same as the input DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # G is the labeled data or the feature vectors that should be split
        >>> train_test = em.split_train_test(G, train_proportion=0.5)
        >>> train, test = train_test['train'], train_test['test']


    """
    # Validate input parameters
    # # We expected labeled data to be of type pandas DataFrame
    if not isinstance(labeled_data, pd.DataFrame):
        logger.error('Input table is not of type DataFrame')
        raise AssertionError('Input table is not of type DataFrame')

    ch.log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                        'fk rtable, '
                        'ltable, rtable, ltable key, rtable key', verbose)

    # # Get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
        cm.get_metadata_for_candset(
            labeled_data,
            logger, verbose)

    # # Validate metadata
    cm._validate_metadata_for_candset(labeled_data, key, fk_ltable, fk_rtable,
                                      ltable, rtable, l_key, r_key,
                                      logger, verbose)

    num_rows = len(labeled_data)
    # We expect the train proportion to be between 0 and 1.
    assert train_proportion >= 0 and train_proportion <= 1, \
        " Train proportion is expected to be between 0 and 1"

    # We expect the number of rows in the table to be non-empty
    assert num_rows > 0, 'The input table is empty'

    # Explicitly get the train and test size in terms of tuples (based on the
    #  given proportion)
    train_size = int(math.floor(num_rows * train_proportion))
    test_size = int(num_rows - train_size)

    # Use sk-learn to split the data
    idx_values = np.array(labeled_data.index.values)
    idx_train, idx_test = ms.train_test_split(idx_values, test_size=test_size,
                                              train_size=train_size,
                                              random_state=random_state)

    # Construct output tables.
    label_train = labeled_data.loc[idx_train]
    label_test = labeled_data.loc[idx_test]

    # Update catalog
    cm.init_properties(label_train)
    cm.copy_properties(labeled_data, label_train)

    cm.init_properties(label_test)
    cm.copy_properties(labeled_data, label_test)

    # Return output tables
    result = OrderedDict()
    result['train'] = label_train
    result['test'] = label_test

    # Finally, return the dictionary.
    return result


def get_ts():
    """
    This is a helper function, to generate a random string based on current
    time.
    """
    t = int(round(time.time() * 1e10))
    # Return the random string.
    return str(t)[::-1]


def impute_table(table, exclude_attrs=None, missing_val='NaN',
                 strategy='mean', fill_value=None, val_all_nans=0, verbose=True):
    """
    Impute table containing missing values.

    Args:
        table (DataFrame): DataFrame which values should be imputed.
        exclude_attrs (List) : list of attribute names to be excluded from
            imputing (defaults to None).
        missing_val (string or int):  The placeholder for the missing values.
            All occurrences of `missing_values` will be imputed.
            For missing values encoded as np.nan, use the string value 'NaN'
            (defaults to 'NaN').
        strategy (string): String that specifies on how to impute values. Valid
            strings: 'mean', 'median', 'most_frequent' (defaults to 'mean').
        fill_value (any):  When strategy == "constant", `fill_value` is used to replace
            all occurrences of missing values.
        val_all_nans (float): Value to fill in if all the values in the column
            are NaN.

    Returns:
        Imputed DataFrame.


    Raises:
        AssertionError: If `table` is not of type pandas DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # H is the feature vector which should be imputed. Specifically, impute the missing values
        >>> # in each column, with the mean of that column
        >>> H = em.impute_table(H, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], strategy='mean')


    """
    # Validate input paramaters
    # # We expect the input table to be of type pandas DataFrame
    if not isinstance(table, pd.DataFrame):
        logger.error('Input table is not of type DataFrame')
        raise AssertionError('Input table is not of type DataFrame')

    ch.log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                        'fk rtable, '
                        'ltable, rtable, ltable key, rtable key', verbose)

    # # Get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
        cm.get_metadata_for_candset(
            table,
            logger, verbose)

    # # Validate metadata
    cm._validate_metadata_for_candset(table, key, fk_ltable, fk_rtable,
                                      ltable, rtable, l_key, r_key,
                                      logger, verbose)

    fv_columns = table.columns

    if exclude_attrs == None:
        feature_names = fv_columns

    else:

        # Check if the exclude attributes are present in the input table
        if not ch.check_attrs_present(table, exclude_attrs):
            logger.error('The attributes mentioned in exclude_attrs '
                         'is not present '
                         'in the input table')
            raise AssertionError(
                'The attributes mentioned in exclude_attrs '
                'is not present '
                'in the input table')
        # We expect exclude attributes to be of type list. If not convert it into
        #  a list.
        if not isinstance(exclude_attrs, list):
            exclude_attrs = [exclude_attrs]

        # Drop the duplicates from the exclude attributes
        exclude_attrs = gh.list_drop_duplicates(exclude_attrs)

        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]
    # print feature_names
    table_copy = table.copy()
    projected_table = table_copy[feature_names]

    projected_table_values = projected_table.values

    imp = SimpleImputer(missing_values=missing_val, strategy=strategy, fill_value=fill_value)
    imp.fit(projected_table_values)
    imp.statistics_[pd.np.isnan(imp.statistics_)] = val_all_nans
    projected_table_values = imp.transform(projected_table_values)
    table_copy[feature_names] = projected_table_values
    # Update catalog
    cm.init_properties(table_copy)
    cm.copy_properties(table, table_copy)

    return table_copy


def get_true_lbl_index(estimator, true_label=1):
    classes = list(estimator.classes_)
    if true_label not in classes:
        raise AssertionError(
            'True label ({0}) not in estimator classes.'.format(true_label))
    else:
        return classes.index(true_label)


def get_false_lbl_index(estimator, false_label=0):
    classes = list(estimator.classes_)
    if false_label not in classes:
        raise AssertionError(
            'False label ({0}) not in estimator classes.'.format(false_label))
    else:
        return classes.index(false_label)


def get_preds_probs(row, false_label=0):
    if row['predictions'] == false_label:
        return (row['predictions'], row['prob_false'])
    else:
        return (row['predictions'], row['prob_true'])


def unpack_preds(s):
    return s[0]


def unpack_probs(s):
    return s[1]


def process_preds_probs(predictions, probs, estimator):

    df = pd.DataFrame()
    df['predictions'] = predictions
    false_index = get_false_lbl_index(estimator)
    true_index = get_true_lbl_index(estimator)
    df['prob_true'] = probs[:, true_index]
    df['prob_false'] = probs[:, false_index]

    preds_probs = df.apply(get_preds_probs, axis=1)
    preds = preds_probs.apply(unpack_preds)
    probs = preds_probs.apply(unpack_probs)
    return preds.values, probs.values
