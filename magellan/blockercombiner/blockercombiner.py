# coding=utf-8
"""
This module contains functions for combining outputs from multiple blockers.
"""
import logging

import pandas as pd
import six

import magellan.catalog.catalog_manager as cm
import magellan.utils.catalog_helper as ch
import magellan.utils.generic_helper as gh

logger = logging.getLogger(__name__)


def combine_blocker_outputs_via_union(blocker_output_list, l_prefix='ltable_',
                                      r_prefix='rtable_', verbose=False):
    """
    Combine multiple blocker outputs by unioning their tuple pair ids (foreign
    key ltable, foreign key rtable).

    This function combines multiple blocker outputs via union. Specifically,
    this function takes in a list of DataFrames (candidate sets, typically the
    output from blockers) and returns a consolidated DataFrame. The output
    DataFrame contains the union of tuple pair ids (foreign key ltable,
    foreign key rtable) and other attributes from the input list of DataFrames.

    Args:
        blocker_output_list (list of DataFrames): List of DataFrames that
            should be combined. Refer notes section for a detailed
            description of the assumptions made by the function about the
            input list  of blocker outputs.
        l_prefix (string): Prefix given to the attributes from the ltable.
        r_prefix (string): Prefix given to the attributes from the rtable.
        verbose (boolean): Flag to indicate whether more detailed information
            about the execution steps should be printed out (default value is
            False).

    Returns:
        A new DataFrame with the combined tuple pairs and other attributes from
        all the blocker lists . Refer notes section , for a detailed
        discussion on what  attributes are added to the output DataFrame.
        Further, the metadata for the consolidated DataFrame  with the
        metadata (in the catalog) derived from the input blocker list.

    Raises:
        AssertionError: If the l_prefix is not of type string.
        AssertionError: If the r_prefix is not of type string.


    """

    # validate inputs
    if not isinstance(l_prefix, six.string_types):
        logger.error('l_prefix is not of type string')
        raise AssertionError('l_prefix is not of type string')

    if not isinstance(r_prefix, six.string_types):
        logger.error('r_prefix is not of type string')
        raise AssertionError('r_prefix is not of type string')

    _validate_lr_tables(blocker_output_list)

    ltable, rtable = cm.get_property(blocker_output_list[0], 'ltable'), \
                     cm.get_property(blocker_output_list[0], 'rtable')

    fk_ltable, fk_rtable = cm.get_property(blocker_output_list[0], 'fk_ltable'), \
                           cm.get_property(blocker_output_list[0], 'fk_rtable')

    l_key, r_key = cm.get_key(ltable), cm.get_key(rtable)

    if fk_ltable.startswith(l_prefix) is False:
        logger.warning(
            'Foreign key for ltable is not starting with the given prefix')

    if fk_rtable.startswith(r_prefix) is False:
        logger.warning(
            'Foreign key for rtable is not starting with the given prefix')

    # combine the ids
    proj_output_list = []
    l_output_attrs = []
    r_output_attrs = []

    for b in blocker_output_list:
        proj_b = b[[fk_ltable, fk_rtable]]
        proj_output_list.append(proj_b)
        col_set = (
            gh.list_diff(list(b.columns),
                         [fk_ltable, fk_rtable, cm.get_key(b)]))
        l_attrs, r_attrs = _lr_cols(col_set, l_prefix, r_prefix)
        l_output_attrs.extend(l_attrs)
        r_output_attrs.extend(r_attrs)

    ch.log_info(logger, 'Concatenating the output list ...', verbose)
    concat_output_projs = pd.concat(proj_output_list)
    ch.log_info(logger, 'Concatenating the output list ... DONE', verbose)

    ch.log_info(logger, 'Deduplicating the output list ...', verbose)
    deduplicated_candset = concat_output_projs.drop_duplicates()
    ch.log_info(logger, 'Deduplicating the output list ... DONE', verbose)

    # construct output table
    l_output_attrs, r_output_attrs = gh.list_drop_duplicates(
        l_output_attrs), gh.list_drop_duplicates(r_output_attrs)
    deduplicated_candset.reset_index(inplace=True, drop=True)
    candset = gh._add_output_attributes(deduplicated_candset, fk_ltable,
                                        fk_rtable,
                                        ltable, rtable, l_key, r_key,
                                        l_output_attrs, r_output_attrs,
                                        l_prefix,
                                        r_prefix,
                                        validate=False)

    # update catalog

    # candset.sort_values([fk_ltable, fk_rtable], inplace=True) # this seems to fail in py 3.3
    try:  # check if sort_values is defined
        candset.sort_values([fk_ltable, fk_rtable],
                            inplace=True)  # this seems to fail in py 3.3
        # Method exists, and was used.
    except AttributeError:
        candset.sort([fk_ltable, fk_rtable], inplace=True)

    key = ch.get_name_for_key(candset.columns)
    candset = ch.add_key_column(candset, key)
    candset.reset_index(inplace=True, drop=True)
    cm.set_candset_properties(candset, key, fk_ltable, fk_rtable, ltable,
                              rtable)

    # return candidate set
    return candset


def _validate_lr_tables(blocker_output_list):
    # validate blocker output list
    idx = 0
    for c in blocker_output_list:
        if not isinstance(c, pd.DataFrame):
            logger.error(
                'Input object at index %s is not a data frame' % str(c))
            raise AssertionError(
                'Input object at index %s is not a data frame' % str(c))

    # get ids of all ltables from blocker output list
    id_l = [id(cm.get_ltable(c)) for c in blocker_output_list]
    # convert to set
    id_l = set(id_l)
    if not len(id_l) == 1:
        logger.error('Candidate set list contains different left tables')
        raise AssertionError('Candidate set list contains different '
                             'left tables')


    # check foreign key values are same
    id_fk_l = [cm.get_fk_ltable(c) for c in blocker_output_list]
    # convert to set
    id_fk_l = set(id_fk_l)


    assert len(
        id_fk_l) is 1, 'Candidate set list contains different foreign key for ltables'

    id_r = [id(cm.get_rtable(c)) for c in blocker_output_list]
    id_r = set(id_r)
    assert len(id_r) is 1, 'Candidate set list contains different right tables'

    id_fk_r = [cm.get_fk_rtable(c) for c in blocker_output_list]
    # convert to set
    id_fk_r = set(id_fk_r)
    assert len(
        id_fk_r) is 1, 'Candidate set list contains different foreign key for rtables'


def _lr_cols(col_set, l_output_prefix, r_output_prefix):
    cols = col_set
    col_l = [_get_col(s, l_output_prefix) for s in cols]
    col_l = [x for x in col_l if x is not None]

    col_r = [_get_col(s, r_output_prefix) for s in cols]
    col_r = [x for x in col_r if x is not None]

    return col_l, col_r


def _get_col(s, p):
    if s.startswith(p):
        return s[len(p):]
    return None
