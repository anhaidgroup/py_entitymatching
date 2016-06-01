# coding=utf-8
from collections import OrderedDict
import logging

import pandas as pd
import pyprind

import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from magellan.utils.generic_helper import list_diff, _add_output_attributes, list_drop_duplicates

logger = logging.getLogger(__name__)


def combine_blocker_outputs_via_union(blocker_output_list, l_prefix='ltable_', r_prefix='rtable_', verbose=False):
    _validate_lr_tables(blocker_output_list)

    ltable, rtable = cm.get_property(blocker_output_list[0], 'ltable'), \
                     cm.get_property(blocker_output_list[0], 'rtable')

    fk_ltable, fk_rtable = cm.get_property(blocker_output_list[0], 'fk_ltable'), \
                           cm.get_property(blocker_output_list[0], 'fk_rtable')

    l_key, r_key = cm.get_key(ltable), cm.get_key(rtable)

    if fk_ltable.startswith(l_prefix) is False:
        logger.warning('Foreign key for ltable is not starting with the given prefix')

    if fk_rtable.startswith(r_prefix) is False:
        logger.warning('Foreign key for rtable is not starting with the given prefix')

    # combine the ids
    proj_output_list = []
    l_output_attrs = []
    r_output_attrs = []
    for b in blocker_output_list:
        proj_b = b[[fk_ltable, fk_rtable]]
        proj_output_list.append(proj_b)
        col_set = (list_diff(b, [fk_ltable, fk_rtable]))
        l_attrs, r_attrs = _lr_cols(col_set, l_prefix, r_prefix)
        l_output_attrs.extend(l_attrs)
        r_output_attrs.extend(r_attrs)

    log_info(logger, 'Concatenating the output list ...', verbose)
    concat_output_projs = pd.concat(proj_output_list)
    log_info(logger, 'Concatenating the output list ... DONE', verbose)

    log_info(logger, 'Deduplicating the output list ...', verbose)
    deduplicated_candset = concat_output_projs.drop_duplicates()
    log_info(logger, 'Deduplicating the output list ... DONE', verbose)

    # construct output table
    l_output_attrs, r_output_attrs = list_drop_duplicates(l_output_attrs), list_drop_duplicates(r_output_attrs)
    deduplicated_candset.reset_index(inplace=True)
    candset = _add_output_attributes(deduplicated_candset, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                     l_output_attrs, r_output_attrs, l_prefix, r_prefix,
                                     validate=False)

    # update catalog
    key = get_name_for_key(candset.columns)
    candset = add_key_column(candset, key)
    cm.set_candset_properties(candset, key, fk_ltable, fk_rtable, ltable, rtable)

    # return candidate set
    return candset


def _validate_lr_tables(blocker_output_list):
    # validate blocker output list
    idx = 0
    for c in blocker_output_list:
        if not isinstance(c, pd.DataFrame):
            logger.error('Input object at index %s is not a data frame' % str(c))
            raise AssertionError('Input object at index %s is not a data frame' % str(c))

    # get ids of all ltables from blocker output list
    id_l = [id(cm.get_ltable(c)) for c in blocker_output_list]
    # convert to set
    id_l = set(id_l)
    # check its length is 1 == all the ltables are same
    assert len(id_l) is 1, 'Candidate set list contains different left tables'

    # check foreign key values are same
    id_fk_l = [cm.get_fk_ltable(c) for c in blocker_output_list]
    # convert to set
    id_fk_l = set(id_fk_l)
    assert len(id_fk_l) is 1, 'Candidate set list contains different foreign key for ltables'

    id_r = [id(cm.get_rtable(c)) for c in blocker_output_list]
    id_r = set(id_r)
    assert len(id_r) is 1, 'Candidate set list contains different right tables'

    id_fk_r = [cm.get_fk_rtable(c) for c in blocker_output_list]
    # convert to set
    id_fk_r = set(id_fk_r)
    assert len(id_fk_r) is 1, 'Candidate set list contains different foreign key for rtables'


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
