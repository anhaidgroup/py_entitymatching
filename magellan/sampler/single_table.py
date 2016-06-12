# coding=utf-8
"""
This module contains sampling related routines
"""

import logging

import pandas as pd
import six

import magellan.catalog.catalog_manager as cm
import magellan.utils.catalog_helper as ch

logger = logging.getLogger(__name__)


# sample one table using random sampling
def sample_table(data_frame, sample_size, replace=False, verbose=False):
    """
    Sample a pandas DataFrame (for labeling purposes).

    This function samples a DataFrame, to be typically used for labeling
    purposes. This function expects the input DataFrame containing the
    metadata of a candidate set (such as key, fk_ltable, fk_rtable, ltable,
    rtable). Currently this function  samples the data
    using uniform random sampling. Specifically, this function uses 'random'
    function from numpy to sample the DataFrame.

    Args:
        data_frame (DataFrame): Input DataFrame to be sampled. Specifically,
            a DataFrame containing the metadata of a candidate set (such as
            key, fk_ltable, fk_rtable, ltable, rtable).
        sample_size (int): Number of samples to be picked up from the input
            DataFrame.
        replace (boolean): Flag to indicate whether sampling should be done
            with replacement or not (default value is False).
        verbose (boolean): Flag to indicate whether more detailed information
            about the execution steps should be printed out (default value is
            False).

    Returns:
        A DataFrame with 'sample_size' number of rows. Further, this function
        sets the output DataFrame's properties same as input DataFrame.

    Raises:
        AssertionError: If the input object is not of type pandas DataFrame.
        AssertionError: If the input DataFrame size is 0.
        AssertionError: If the sample_size id greater than the input
            DataFrame size.

    Notes:
        As mentioned in the above description, the output DataFrame is
        updated (in the catalog) with the properties from the input
        DataFrame. A subtle point to note here is, when the replace flag is
        set to True, then the output  DataFrame can contain duplicate keys.
        In that case, this function  will not set the key and it is up to
        the user to fix it after the function returns.
    """
    # Validate input parameters.

    # # The input DataFrame is expected to be of type pandas DataFrame.
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input table is not of type pandas dataframe')
        raise AssertionError('Input table is not of type pandas dataframe')

    # # There should at least not-zero rows to sample from
    if len(data_frame) == 0:
        logger.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    # # The sample size should be less than or equal to the number of rows in
    #  the input DataFrame
    if len(data_frame) < sample_size:
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
        cm.get_metadata_for_candset(data_frame, logger, verbose)

    # # Third, validate the metadata
    cm.validate_metadata_for_candset(data_frame, key, fk_ltable, fk_rtable,
                                     ltable, rtable, l_key, r_key,
                                     logger, verbose)

    # Get the sample set for the output table
    sample_indices = pd.np.random.choice(len(data_frame), sample_size,
                                         replace=replace)
    # Sort the indices ordered by index value
    sample_indices = sorted(sample_indices)
    sampled_table = data_frame.iloc[list(sample_indices)]

    # Copy the properties
    cm.init_properties(sampled_table)

    # # If the replace is set to True, then we should check for the validity
    # of key before setting it
    if replace:
        properties = cm.get_all_properties(data_frame)
        for property_name, property_value in six.iteritems(properties):
            if property_name == 'key':
                # Check for the validity of key before setting it
                cm.set_key(sampled_table, property_value)
            else:
                # Copy the other properties as is
                cm.set_property(sampled_table, property_name, property_value)
    else:
        cm.copy_properties(data_frame, sampled_table)

    # Return the sampled table
    return sampled_table
