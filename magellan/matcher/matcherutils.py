"""
This module contains some utility functions for the matcher.
"""
import logging
import math
import time
from collections import OrderedDict

import pandas as pd
import sklearn.cross_validation as cv

import magellan.catalog.catalog_manager as cm
import magellan.utils.catalog_helper as ch

logger = logging.getLogger(__name__)


def train_test_split(labeled_data, train_proportion=0.5,
                     random_state=None, verbose=True):
    """
    This function splits the input data into train and test.

    Specifically, this function is just a wrapper of scikit-learn's
    train_test_split function. This function also takes care to metadata to
    be copied properly to train and test splits.

    Args:
        labeled_data (DataFrame): Input pandas DataFrame that needs to be
            split into train and test.
        train_proportion (float): A number between 0 and 1, indicating the
            proportion of tuples that should be included in the train split (
            defaults to 0.5).
        random_state (object): A number of random number object (as in
            scikit-learn).
        verbose (boolean): A flag to indicate whether the debug information
            should be displayed.

    Returns:
        The command returns a dictionary with two keys: train, and test.
        The value for the key 'train' is an MTable containing tuples
        allocated from the input table based on train_proportion.
        Similarly, the value for the key 'test' is an MTable containing
        tuples for evaluation.

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
    idx_values = pd.np.array(labeled_data.index.values)
    idx_train, idx_test = cv.train_test_split(idx_values, test_size=test_size,
                                              train_size=train_size,
                                              random_state=random_state)

    # Construct output tables.
    label_train = labeled_data.ix[idx_train]
    label_test = labeled_data.ix[idx_test]

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
