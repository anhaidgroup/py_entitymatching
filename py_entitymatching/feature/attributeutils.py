"""
This module contains some utility functions for attributes in the DataFrame.
"""
import logging

import pandas as pd
import numpy as np
import six

from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def get_attr_types(data_frame):
    """
    This function gets the attribute types for a DataFrame.

    Specifically this function gets the attribute types based on the
    statistics of the attributes. These attribute types can be str_eq_1w,
    str_bt_1w_5w, str_bt_5w_10w, str_gt_10w, boolean or numeric.

    The types roughly capture whether the attribute is of type string,
    boolean or numeric. Further, with in the string type the subtypes are
    capture the average number of tokens in the column values. For example,
    str_bt_1w_5w means the average number of tokens in that column is
    greater than one word but less than 5 words.

    Args:
        data_frame (DataFrame): The input DataFrame for which types of
         attributes must be determined.

    Returns:
        A Python dictionary is returned containing the attribute types.

        Specifically, in the dictionary key is an attribute name, value
        is the type of that attribute.

        Further, the dictionary will have a  key _table, and the value of
        that should be a pointer to the input DataFrame.

    Raises:
        AssertionError: If `data_frame` is not of type
            pandas DataFrame.

    Examples:

        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> atypes1 = em.get_attr_types(A)
        >>> atypes2 = em.get_attr_types(B)


    """
    # Validate input paramaters

    # # We expect the input object (data_frame) to be of type pandas DataFrame.
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input table is not of type pandas dataframe')
        raise AssertionError('Input table is not of type pandas dataframe')

    # Now get type for each column
    type_list = [_get_type(data_frame[col]) for col in data_frame.columns]

    # Create a dictionary containing attribute types
    attribute_type_dict = dict(zip(data_frame.columns, type_list))

    # Update the dictionary with the _table key and value set to the input
    # DataFrame
    attribute_type_dict['_table'] = data_frame

    # Return the attribute type dictionary
    return attribute_type_dict


def get_attr_corres(ltable, rtable):
    """
    This function gets the attribute correspondences between the attributes
    of ltable and rtable.

    The user may need to get the correspondences so
    that he/she can generate features based those correspondences.

    Args:
        ltable,rtable (DataFrame): Input DataFrames for which
            the attribute correspondences must be obtained.

    Returns:
        A Python dictionary is returned containing the attribute
        correspondences.

        Specifically, this returns a dictionary with the following key-value
        pairs:

        corres: points to the list correspondences as tuples. Each
        correspondence is a tuple with two attributes: one from ltable
        and the other from rtable.

        ltable: points to ltable.

        rtable: points to rtable.

        Currently, 'corres' contains only pairs of attributes with exact
        names in ltable and rtable.

    Raises:
        AssertionError: If `ltable` is not of type
            pandas DataFrame.
        AssertionError: If `rtable` is not of type
            pandas DataFrame.

    Examples:

        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> match_c = em.get_attr_corres(A, B)


    """
    # Validate input parameters
    # # We expect the input object (ltable) to be of type pandas
    # DataFrame
    validate_object_type(ltable, pd.DataFrame, error_prefix='Input ltable')
    # # We expect the input object (rtable) to be of type pandas
    # DataFrame
    validate_object_type(rtable, pd.DataFrame, error_prefix='Input rtable')

    # Initialize the correspondence list
    correspondence_list = []
    # Check for each column in ltable, if column exists in rtable,
    # If so, add it to the correspondence list.
    # Note: This may not be the fastest way to implement this. We could
    # refactor this later.
    for column in ltable.columns:
        if column in rtable.columns:
            correspondence_list.append((column, column))
    # Initialize a correspondence dictionary.
    correspondence_dict = dict()
    # Fill the corres, ltable and rtable.
    correspondence_dict['corres'] = correspondence_list
    correspondence_dict['ltable'] = ltable
    correspondence_dict['rtable'] = rtable
    # Finally, return the correspondence dictionary
    return correspondence_dict


def _get_type(column):
    """
     Given a pandas Series (i.e column in pandas DataFrame) obtain its type
    """
    # Validate input parameters
    # # We expect the input column to be of type pandas Series

    if not isinstance(column, pd.Series):
        raise AssertionError('Input (column) is not of type pandas series')

    # To get the type first drop all NaNa
    column = column.dropna()

    # Get type for each element and convert it into a set (and for
    # convenience convert the resulting set into a list)
    type_list = list(set(column.map(type).tolist()))

    # If the list is empty, then we cannot decide anything about the column.
    # We will raise a warning and return the type to be numeric.
    # Note: The reason numeric is returned instead of a special type because,
    #  we want to keep the types minimal. Further, explicitly recommend the
    # user to update the returned types later.
    if len(type_list) == 0:
        logger.warning("Column {0} does not seem to qualify as any atomic type. "
                       "It may contain all NaNs. Please update the values of column {0}".format(column.name))
        return 'un_determined'

    # If the column qualifies to be of more than one type (for instance,
    # in a numeric column, some values may be inferred as strings), then we
    # will raise an error for the user to fix this case.
    if len(type_list) > 1:
        logger.warning('Column %s qualifies to be more than one type. \n'
                       'Please explicitly set the column type like this:\n'
                       'A["address"] = A["address"].astype(str) \n'
                       'Similarly use int, float, boolean types.' % column.name)
        raise AssertionError('Column %s qualifies to be more than one type. \n'
                             'Please explicitly set the column type like this:\n'
                             'A["address"] = A["address"].astype(str) \n'
                             'Similarly use int, float, boolean types.' % column.name)
    else:
        # the number of types is 1.
        returned_type = type_list[0]
        # Check if the type is boolean, if so return boolean
        if returned_type == bool or returned_type == np.bool_:
            return 'boolean'

        # Check if the type is string, if so identify the subtype under it.
        # We use average token length to identify the subtypes

        # Consider string and unicode as same
        elif returned_type == str or returned_type == six.unichr or returned_type == six.text_type:
            # get average token length
            average_token_len = \
                pd.Series.mean(column.str.split().apply(_len_handle_nan))
            if average_token_len == 1:
                return "str_eq_1w"
            elif average_token_len <= 5:
                return "str_bt_1w_5w"
            elif average_token_len <= 10:
                return "str_bt_5w_10w"
            else:
                return "str_gt_10w"
        else:
            # Finally, return numeric if it does not qualify for any of the
            # types above.
            return "numeric"


def _len_handle_nan(input_list):
    """
     Get the length of list, handling NaN
    """
    # Check if the input is of type list, if so return the len else return NaN
    if isinstance(input_list, list):
        return len(input_list)
    else:
        return np.NaN
