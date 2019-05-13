# coding=utf-8
"""
This module contains functions for combining outputs from multiple blockers.
"""
import logging

import pandas as pd
import six

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
import py_entitymatching.utils.generic_helper as gh
import py_entitymatching.utils.validation_helper

logger = logging.getLogger(__name__)


def combine_blocker_outputs_via_union(
        blocker_output_list,
        l_prefix='ltable_',
        r_prefix='rtable_',
        verbose=False):
    """
    Combines multiple blocker outputs by doing a union of their tuple pair
    ids (foreign key ltable, foreign key rtable).

    Specifically, this function takes in a list of DataFrames (candidate
    sets, typically the
    output from blockers) and returns a consolidated DataFrame. The output
    DataFrame contains the union of tuple pair ids (foreign key ltable,
    foreign key rtable) and other attributes from the input list of DataFrames.

    This function makes some assumptions about the input DataFrames. First,
    each DataFrame is expected to contain the following metadata in the
    catalog: key, fk_ltable, fk_rtable, ltable, and rtable. Second,
    all the DataFrames must be a result of blocking from the same underlying
    tables. Concretely the ltable and rtable properties must refer to the
    same DataFrame across all the input tables. Third, all the input
    DataFrames must have the same fk_ltable and fk_rtable properties.
    Finally, in each input DataFrame, for the attributes included from the
    ltable or rtable, the attribute names must be prefixed with the given
    l_prefix and r_prefix in the function.

    The input DataFrames may contain different attribute lists and it demands
    the question of how to combine them. Currently py_entitymatching takes an union
    of attribute names that has prefix l_prefix or r_prefix across
    input tables. After taking the union, for each tuple id pair included
    in output, the attribute values (for union-ed attribute names) are
    probed from ltable/rtable and included in the output.

    A subtle point to note here is,  if an input DataFrame has a column
    added by user (say label for some reason), then that column will not
    be present in the output. The reason is, the same column may not be
    present in other candidate sets so it is not clear about how to
    combine them. One possibility is to include label in output for all
    tuple id pairs, but set as NaN for the values not present. Currently
    py_entitymatching does not include such columns and addressing it will be part
    of future work.

    Args:
        blocker_output_list (list of DataFrames): The list of DataFrames that
            should be combined.
        l_prefix (string): The prefix given to the attributes from the ltable.
        r_prefix (string): The prefix given to the attributes from the rtable.
        verbose (boolean): A flag to indicate whether more detailed information
            about the execution steps should be printed out (default value is
            False).

    Returns:
        A new DataFrame with the combined tuple pairs and other attributes from
        all the blocker lists.

    Raises:
        AssertionError: If `l_prefix` is not of type string.
        AssertionError: If `r_prefix` is not of type string.
        AssertionError: If the length of the input DataFrame list is 0.
        AssertionError: If `blocker_output_list` is not a list of
            DataFrames.
        AssertionError: If the ltables are different across the input list of
            DataFrames.
        AssertionError: If the rtables are different across the input list of
            DataFrames.
        AssertionError: If the `fk_ltable` values are different across the
            input list of DataFrames.
        AssertionError: If the `fk_rtable` values are different across the
            input list of DataFrames.

    Examples:

        >>> import py_entitymatching as em
        >>> ab = em.AttrEquivalenceBlocker()
        >>> C = ab.block_tables(A, B, 'zipcode', 'zipcode')
        >>> ob = em.OverlapBlocker()
        >>> D = ob.block_candset(C, 'address', 'address')
        >>> block_f = em.get_features_for_blocking(A, B)
        >>> rb = em.RuleBasedBlocker()
        >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
        >>> rb.add_rule(rule, block_f)
        >>> E = rb.block_tables(A, B)
        >>> F = em.combine_blocker_outputs_via_union([C, E])


    """

    # validate input parameters

    # The l_prefix is expected to be of type string
    py_entitymatching.utils.validation_helper.validate_object_type(l_prefix, six.string_types, 'l_prefix')

    # The r_prefix is expected to be of type string
    py_entitymatching.utils.validation_helper.validate_object_type(r_prefix, six.string_types, 'r_prefix')

    # We cannot combine empty DataFrame list
    if not len(blocker_output_list) > 0:
        logger.error('There no DataFrames to combine')
        raise AssertionError('There are no DataFrames to combine')

    # Validate the assumptions about the input tables.
    # # 1) All the input object must be DataFrames
    # # 2) All the input DataFrames must have the metadata as that of a
    # candidate set
    # # 3) All the input DataFrames must have the same fk_ltable and fk_rtable
    _validate_lr_tables(blocker_output_list)

    # # Get the ltable and rtable. We take it from the first DataFrame as all
    #  the DataFrames contain the same ltables and rtables
    ltable = cm.get_ltable(blocker_output_list[0])
    rtable = cm.get_rtable(blocker_output_list[0])

    # # Get the fk_ltable and fk_rtable. We take it from the first DataFrame as
    #  all the DataFrames contain the same ltables and rtables
    fk_ltable = cm.get_fk_ltable(blocker_output_list[0])
    fk_rtable = cm.get_fk_rtable(blocker_output_list[0])

    # Retrieve the keys for the ltable and rtables.
    l_key = cm.get_key(ltable)
    r_key = cm.get_key(rtable)

    # Check if the fk_ltable is starting with the given prefix, if not its
    # not an error. Just raise a warning.
    if fk_ltable.startswith(l_prefix) is False:
        logger.warning(
            'Foreign key for ltable is not starting with the given prefix ('
            '%s)', l_prefix)

    # Check if the fk_rtable is starting with the given prefix, if not its
    # not an error. Just raise a warning.
    if fk_rtable.startswith(r_prefix) is False:
        logger.warning(
            'Foreign key for rtable is not starting with the given prefix ('
            '%s)', r_prefix)

    # Initialize lists
    # # keep track of projected tuple pair ids
    tuple_pair_ids = []
    # # keep track of output attributes from the left table
    l_output_attrs = []
    # # keep track of output attributes from the right table
    r_output_attrs = []

    # for each DataFrame in the given list, project out tuple pair ids, get the
    #  attributes from the ltable and rtable
    for data_frame in blocker_output_list:
        # Project out the tuple pair ids. A tuple pair id is a fk_ltable,
        # fk_rtable pair
        projected_tuple_pair_ids = data_frame[[fk_ltable, fk_rtable]]
        # Update the list that tracks tuple pair ids
        tuple_pair_ids.append(projected_tuple_pair_ids)

        # Get the columns, which should be segregated into the attributes
        # from the ltable and table
        col_set = (
            gh.list_diff(list(data_frame.columns),
                         [fk_ltable, fk_rtable, cm.get_key(data_frame)]))

        # Segregate the columns as attributes from the ltable and rtable
        l_attrs, r_attrs = _lr_cols(col_set, l_prefix, r_prefix)

        # Update the l_output_attrs, r_output_attrs
        l_output_attrs.extend(l_attrs)
        # the reason we use extend because l_attrs a list
        r_output_attrs.extend(r_attrs)

    ch.log_info(logger, 'Concatenating the tuple pair ids across given '
                        'blockers ...', verbose)

    # concatenate the tuple pair ids from the list of input DataFrames
    concatenated_tuple_pair_ids = pd.concat(tuple_pair_ids)

    ch.log_info(logger, 'Concatenating the tuple pair ids ... DONE', verbose)
    ch.log_info(logger, 'Deduplicating the tuple pair ids ...', verbose)

    # Deduplicate the DataFrame. Now the returned DataFrame will contain
    # unique tuple pair ids.

    # noinspection PyUnresolvedReferences
    deduplicated_tuple_pair_ids = concatenated_tuple_pair_ids.drop_duplicates()

    ch.log_info(logger, 'Deduplicating the tuple pair ids ... DONE', verbose)

    # Construct output table
    # # Get unique list of attributes across different tables
    l_output_attrs = gh.list_drop_duplicates(l_output_attrs)
    r_output_attrs = gh.list_drop_duplicates(r_output_attrs)

    # Reset the index that might have lingered from concatenation.
    deduplicated_tuple_pair_ids.reset_index(inplace=True, drop=True)

    # Add the output attribtues from the ltable and rtable.
    # NOTE: This approach may be inefficient as it probes the ltable, rtable
    # to get the attribute values. A better way would be to fill the
    # attribute values from the input list of DataFrames. This attribute values
    # could be harvested (at the expense of some space) while we iterate the
    # input blocker output list for the first time.

    # noinspection PyProtectedMember
    consolidated_data_frame = gh._add_output_attributes(
        deduplicated_tuple_pair_ids, fk_ltable,
        fk_rtable,
        ltable, rtable, l_key, r_key,
        l_output_attrs, r_output_attrs,
        l_prefix,
        r_prefix,
        validate=False)
    # Sort the DataFrame ordered by fk_ltable and fk_rtable.
    # The function "sort" will be depreciated in the newer versions of
    # pandas DataFrame, and it will replaced by 'sort_values' function. So we
    # will first try to use sort_values if this fails we will use sort.
    try:
        consolidated_data_frame.sort_values([fk_ltable, fk_rtable],
                                            inplace=True)
    except AttributeError:
        consolidated_data_frame.sort([fk_ltable, fk_rtable], inplace=True)

    # update the catalog for the consolidated DataFrame
    # First get a column name for the key
    key = ch.get_name_for_key(consolidated_data_frame.columns)
    # Second, add the column name as the key
    consolidated_data_frame = ch.add_key_column(consolidated_data_frame, key)
    # Third, reset the index to remove any out of order index  values from
    # the sort.
    consolidated_data_frame.reset_index(inplace=True, drop=True)
    # Finally, set the properties for the consolidated DataFrame in the catalog
    cm.set_candset_properties(consolidated_data_frame, key, fk_ltable,
                              fk_rtable, ltable,
                              rtable)

    # Return the consolidated DataFrame
    return consolidated_data_frame


def _validate_lr_tables(blocker_output_list):
    """
    Validate the input blocker output list.
    """
    ltable_ids = []
    rtable_ids = []
    fk_ltable_list = []
    fk_rtable_list = []
    # Iterate through the DataFrame list and keep track the following
    # # 1) Validate whether the input objects are all DataFrames
    # # 2) Update the ltable, rtable (ids), fk_ltable and fk_rtable.
    for data_frame in blocker_output_list:
        py_entitymatching.utils.validation_helper.validate_object_type(data_frame, pd.DataFrame)
        ltable_ids.append(id(cm.get_ltable(data_frame)))
        rtable_ids.append(id(cm.get_rtable(data_frame)))
        fk_ltable_list.append(cm.get_fk_ltable(data_frame))
        fk_rtable_list.append(cm.get_fk_rtable(data_frame))

    # Check whether all the ltables in the catalog are same for the input
    # DataFrame list
    ltable_ids = set(ltable_ids)
    # # We expect all the ltable ids are same (i.e the len. of set should be 1)
    if not len(ltable_ids) == 1:
        logger.error('Candidate set list contains different left tables')
        raise AssertionError('Candidate set list contains different '
                             'left tables')

    # Check whether all the rtables in the catalog are same for the input
    # DataFrame list
    rtable_ids = set(rtable_ids)
    # # We expect all the ltable ids are same (i.e the len. of set should be 1)
    if not len(rtable_ids) == 1:
        logger.error('Candidate set list contains different right tables')
        raise AssertionError('Candidate set list contains different '
                             'right tables')

    # Check whether all the fk_ltable values in the catalog are same for the
    # input DataFrame list
    fk_ltable_set = set(fk_ltable_list)
    if not len(fk_ltable_set) == 1:
        logger.error('Candidate set list contains different foreign key '
                     'for left tables')
        raise AssertionError('Candidate set list contains different '
                             'foreign key for left tables')

    # Check whether all the fk_rtable values in the catalog are same for the
    # input DataFrame list
    fk_rtable_set = set(fk_rtable_list)
    if not len(fk_rtable_set) == 1:
        logger.error('Candidate set list contains different foreign key '
                     'for right tables')
        raise AssertionError('Candidate set list contains different '
                             'foreign key for right tables')

    # If everything looks fine then return True
    return True


def _lr_cols(columns_from_blocker_output_list, l_output_prefix,
             r_output_prefix):
    """
    Get the columns to be retrieved from ltable and rtable based on the
    columns that was observed in the input DataFrames.
    """
    # Get the column name based on the l_output_prefix
    l_columns = [_get_col(column, l_output_prefix)
                 for column in columns_from_blocker_output_list]
    # Remove the column names that are None. The column names will be
    # typically None if the column does not start with the given prefix
    l_columns = [x for x in l_columns if x is not None]

    # Get the column name based on the r_output_prefix
    r_columns = [_get_col(column, r_output_prefix)
                 for column in columns_from_blocker_output_list]

    # Remove the column names that are None. The column names will be
    # typically None if the column does not start with the given prefix
    r_columns = [x for x in r_columns if x is not None]

    # Return the l_columns and r_columns
    return l_columns, r_columns


def _get_col(column, prefix):
    """
    Get the column name after removing the prefix.
    """
    # Check if the column is starting with the given prefix
    if column.startswith(prefix):
        # Return the column name after removing the prefix
        return column[len(prefix):]
    # Return None, if the column does not start with the given prefix
    return None
