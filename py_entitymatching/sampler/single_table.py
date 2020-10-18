# coding=utf-8
"""
This module contains sampling.rst related routines for a single table.
"""
import logging

import pandas as pd
import numpy as np
import six

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


# sample one table using random sampling.rst
def sample_table(table, sample_size, replace=False, verbose=False):
    """
    Samples a candidate set of tuple pairs (for labeling purposes).

    This function samples a DataFrame, typically used for labeling
    purposes. This function expects the input DataFrame containing the
    metadata of a candidate set (such as key, fk_ltable, fk_rtable, ltable,
    rtable). Specifically, this function creates a copy of the input
    DataFrame, samples the data using uniform random sampling (uses 'random'
    function from numpy to sample) and returns the sampled DataFrame.
    Further, also copies the properties from the input DataFrame to the output
    DataFrame.

    Args:
        table (DataFrame): The input DataFrame to be sampled.
            Specifically,
            a DataFrame containing the metadata of a candidate set (such as
            key, fk_ltable, fk_rtable, ltable, rtable) in the catalog.
        sample_size (int): The number of samples to be picked from the input
            DataFrame.
        replace (boolean): A flag to indicate whether sampling should be
            done with replacement or not (defaults to False).
        verbose (boolean): A flag to indicate whether more detailed information
            about the execution steps should be printed out (defaults to False).

    Returns:
        A new DataFrame with 'sample_size' number of rows.

        Further,
        this function sets the output DataFrame's properties same as input
        DataFrame.

    Raises:
        AssertionError: If `table` is not of type pandas DataFrame.
        AssertionError: If the size of `table` is 0.
        AssertionError: If the `sample_size` is greater than the input
            DataFrame size.

    Examples:
        >>> import py_entitymatching as em
        >>> S = em.sample_table(C, sample_size=450) # C is the candidate set to be sampled from.


    Note:
        As mentioned in the above description, the output DataFrame is
        updated (in the catalog) with the properties from the input
        DataFrame. A subtle point to note here is, when the replace flag is
        set to True, then the output  DataFrame can contain duplicate keys.
        In that case, this function  will not set the key and it is up to
        the user to fix it after the function returns.
    """
    # Validate input parameters.

    # # The input DataFrame is expected to be of type pandas DataFrame.
    validate_object_type(table, pd.DataFrame)

    # # There should at least not-zero rows to sample from
    if len(table) == 0:
        logger.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    # # The sample size should be less than or equal to the number of rows in
    #  the input DataFrame
    if len(table) < sample_size:
        logger.error('Sample size is larger than the input table size')
        raise AssertionError('Sample size is larger than the input table size')

    # Now, validate the metadata for the input DataFrame as we have to copy
    # these properties to the output DataFrame

    # # First, display what metadata is required for this function
    ch.log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                        'fk rtable, ltable, rtable, ltable key, rtable key',
                verbose)

    # # Second, get the metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
        cm.get_metadata_for_candset(table, logger, verbose)

    # # Third, validate the metadata
    cm._validate_metadata_for_candset(table, key, fk_ltable, fk_rtable,
                                      ltable, rtable, l_key, r_key,
                                      logger, verbose)

    # Get the sample set for the output table
    sample_indices = np.random.choice(len(table), sample_size,
                                      replace=replace)
    # Sort the indices ordered by index value
    sample_indices = sorted(sample_indices)
    sampled_table = table.iloc[list(sample_indices)]

    # Copy the properties
    cm.init_properties(sampled_table)

    # # If the replace is set to True, then we should check for the validity
    # of key before setting it
    if replace:
        properties = cm.get_all_properties(table)
        for property_name, property_value in six.iteritems(properties):
            if property_name == 'key':
                # Check for the validity of key before setting it
                cm.set_key(sampled_table, property_value)
            else:
                # Copy the other properties as is
                cm.set_property(sampled_table, property_name, property_value)
    else:
        cm.copy_properties(table, sampled_table)

    # Return the sampled table
    return sampled_table
