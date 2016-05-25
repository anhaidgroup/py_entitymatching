# coding=utf-8
import logging
import os
import pickle


import cloud
import numpy as np
import pandas as pd

from magellan.utils.catalog_helper import check_fk_constraint
import magellan.core.catalog_manager as cm


from magellan.utils import install_path
from magellan.io.parsers import read_csv

logger = logging.getLogger(__name__)


def get_install_path():
    path_list = install_path.split(os.sep)
    return os.sep.join(path_list[0:len(path_list)-1])


def remove_non_ascii(s):
    s = ''.join(i for i in s if ord(i) < 128)
    s = str(s)
    return str.strip(s)


# find list difference
def list_diff(a_list, b_list):
    b_set = list_drop_duplicates(b_list)
    return [a for a in a_list if a not in b_set]


def load_dataset(file_name, key=None, **kwargs):
    p = get_install_path()
    p = os.sep.join([p, 'datasets', file_name+'.csv'])
    if file_name == 'table_A' or file_name == 'table_B':
        if key is None:
            key = 'ID'
    table = read_csv(p, key=key, **kwargs)
    return table


# remove rows with NaN in a particular attribute
def rem_nan(table, attr):
    l = table.index.values[np.where(table[attr].notnull())[0]]
    return table.ix[l]


def list_drop_duplicates(lst):
    a = []
    for i in lst:
        if i not in a:
            a.append(i)
    return a

# data frame with output attributes
def add_output_attributes(candset, l_output_attrs=None, r_output_attrs=None, l_output_prefix='ltable_',
                          r_output_prefix='rtable_', validate=True, copy_props=True,
                          delete_from_catalog=True, verbose=False):
    # # get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)
    if validate:
        cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)
    index_values = candset.index

    df = _add_output_attributes(candset, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix,
                                validate=False)

    df.set_index(index_values, inplace=True)
    if copy_props:
        cm.init_properties(df)
        cm.copy_properties(candset, df)
        if delete_from_catalog:
            cm.del_all_properties(candset)
    return df



def _add_output_attributes(candset, fk_ltable, fk_rtable, ltable=None, rtable=None,
                          l_key=None, r_key=None,
                          l_output_attrs=None, r_output_attrs=None,
                          l_output_prefix='ltable_', r_output_prefix='rtable_',
                          validate=True):
    if l_output_attrs is not None:
        assert ltable is not None, 'ltable is not given to pull l_output_attrs'
        assert l_key is not None, 'ltable key cannot be None'
        if validate:
            check_fk_constraint(candset, fk_ltable, ltable, l_key)
        col_names = [l_output_prefix+c for c in l_output_attrs]
        l_df = create_proj_dataframe(ltable, l_key, candset[fk_ltable], l_output_attrs, col_names)


    if r_output_attrs is not None:
        assert rtable is not None, 'rtable is not given to pull r_output_attrs'
        assert r_key is not None, 'rtable key cannot be None'
        if validate:
            check_fk_constraint(candset, fk_rtable, rtable, r_key)
        col_names = [r_output_prefix+c for c in r_output_attrs]
        r_df = create_proj_dataframe(rtable, r_key, candset[fk_rtable], r_output_attrs, col_names)

    if l_output_attrs is not None:
        candset = pd.concat([candset, l_df], axis=1)
    if r_output_attrs is not None:
        candset = pd.concat([candset, r_df], axis=1)


    return candset


def create_proj_dataframe(df, key, key_vals, attrs, col_names):
    df = df.set_index(key, drop=False)
    df = df.ix[key_vals, attrs]
    df.reset_index(drop=True, inplace=True)
    df.columns = col_names
    return df
