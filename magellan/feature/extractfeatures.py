import logging

import pandas as pd

import magellan.core.catalog_manager as cm

from magellan.utils.catalog_helper import check_attrs_present, log_info
from magellan.utils.generic_helper import list_diff

logger = logging.getLogger(__name__)


def extract_feature_vecs(candset, attrs_before=None, feature_table=None, attrs_after=None, verbose=True):
    # validate input parameters
    if attrs_before != None:
        assert check_attrs_present(candset, attrs_before), 'The attributes mentioned in attrs_before is not present ' \
                                                           'in the input table'
    if attrs_after != None:
        assert check_attrs_present(candset, attrs_after), 'The attributes mentioned in attrs_after is not present ' \
                                                          'in the input table'

    assert feature_table is not None, 'Feature table cannot be None'


    log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                     'ltable, rtable, ltable key, rtable key', verbose)

    # # get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

    # # validate metadata
    cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                     logger, verbose)


    # extract features
    id_list = [(r[fk_ltable], r[fk_rtable]) for i, r in candset.iterrows()]

    # # set index for convenience
    l_df = ltable.set_index(l_key, drop=False)
    r_df = rtable.set_index(r_key, drop=False)


    # # apply feature functions
    feat_vals = [apply_feat_fns(l_df.ix[x[0]], r_df.ix[x[1]], feature_table) for x in id_list]

    # construct output table
    table = pd.DataFrame(feat_vals)

    # # rearrange the feature names in the given order
    feat_names = list(feature_table['feature_name'])
    table = table[feat_names]

    # # insert attrs_before
    if attrs_before:
        if not isinstance(attrs_before, list):
            attrs_before = [attrs_before]
        attrs_before = list_diff(attrs_before, [key, fk_ltable, fk_rtable])
        attrs_before.reverse()
        for a in attrs_before:
            table.insert(0, a, candset[a])

    # # insert keys
    table.insert(0, fk_rtable, candset[fk_rtable])
    table.insert(0, fk_ltable, candset[fk_ltable])
    table.insert(0, key, candset[key])

    # # insert attrs after
    if attrs_after:
        if not isinstance(attrs_after, list):
            attrs_after = [attrs_after]
        attrs_after = list_diff(attrs_after, [key, fk_ltable, fk_rtable])
        attrs_after.reverse()
        for a in attrs_after:
            table.insert(0, a, candset[a])

    # reset the index
    table.reset_index(inplace=True, drop=True)

    # # update the catalog
    cm.init_properties(table)
    cm.copy_properties(candset, table)

    return table









def apply_feat_fns(tuple1, tuple2, feat_dict):
    feat_names = list(feat_dict['feature_name'])
    feat_funcs = list(feat_dict['function'])
    feat_vals = [f(tuple1, tuple2) for f in feat_funcs]
    return dict(zip(feat_names, feat_vals))
