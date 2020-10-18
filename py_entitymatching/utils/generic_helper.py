# coding=utf-8
import logging
import os

import pandas as pd
import numpy as np
import six

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils import install_path
from py_entitymatching.utils.catalog_helper import check_fk_constraint

logger = logging.getLogger(__name__)


def get_install_path():
    path_list = install_path.split(os.sep)
    return os.sep.join(path_list[0:len(path_list)-1])


def remove_non_ascii(s):
    if not isinstance(s, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')
    s = ''.join(i for i in s if ord(i) < 128)
    s = str(s)
    return str.strip(s)


# find list difference
def list_diff(a_list, b_list):

    if not isinstance(a_list, list) and not isinstance(a_list, set):
        logger.error('a_list is not of type list or set')
        raise AssertionError('a_list is not of type list or set')

    if not isinstance(b_list, list) and not isinstance(b_list, set):
        logger.error('b_list is not of type list or set')
        raise AssertionError('b_list is not of type list or set')

    b_set = list_drop_duplicates(b_list)
    return [a for a in a_list if a not in b_set]


def load_dataset(file_name, key=None, **kwargs):
    if not isinstance(file_name, six.string_types):
        logger.error('file name is not a string')
        raise AssertionError('file name is not a string')
    p = get_install_path()
    p = os.sep.join([p, 'datasets', file_name+'.csv'])
    table = pd.read_csv(p, **kwargs)
    if key is not None:
        cm.set_key(table, key)
    return table


# remove rows with NaN in a particular attribute
def rem_nan(table, attr):
    if not isinstance(table, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')
    if not isinstance(attr, six.string_types):
        logger.error('Input attr. should be of type string')
        raise AssertionError('Input attr. should be of type string')

    if not attr in table.columns:
        logger.error('Input attr not in the table columns')
        raise KeyError('Input attr. not in the table columns')

    l = table.index.values[np.where(table[attr].notnull())[0]]
    return table.loc[l]


def list_drop_duplicates(lst):
    if not isinstance(lst, list) and not isinstance(lst, set):
        logger.error('Input object not of type list or set')
        raise AssertionError('Input object is not of type list or set')

    a = []
    for i in lst:
        if i not in a:
            a.append(i)
    return a

# data frame with output attributes
def add_output_attributes(candset, l_output_attrs=None, r_output_attrs=None, l_output_prefix='ltable_',
                          r_output_prefix='rtable_', validate=True, copy_props=True,
                          delete_from_catalog=True, verbose=False):

    if not isinstance(candset, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    # # get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)
    if validate:
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
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

    if not isinstance(candset, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(fk_ltable, six.string_types):
        logger.error('fk_ltable is not of type string')
        raise AssertionError('fk_ltable is not of type string')

    if not isinstance(fk_rtable, six.string_types):
        logger.error('fk_rtable is not of type string')
        raise AssertionError('fk_rtable is not of type string')

    if l_output_attrs is not None:

        if ltable is None:
            logger.error('ltable is not given to pull l_output_attrs')
            raise AssertionError('ltable is not given to pull l_output_attrs')
        if l_key is None:
            logger.error('ltable key cannot be None')
            raise AssertionError('ltable key cannot be None')

        if validate:
            check_fk_constraint(candset, fk_ltable, ltable, l_key)
        col_names = [l_output_prefix+c for c in l_output_attrs]
        l_df = create_proj_dataframe(ltable, l_key, candset[fk_ltable], l_output_attrs, col_names)

    if r_output_attrs is not None:
        if rtable is None:
            logger.error('rtable is not given to pull r_output_attrs')
            raise AssertionError('rtable is not given to pull r_output_attrs')
        if r_key is None:
            logger.error('rtable key cannot be None')
            raise AssertionError('rtable key cannot be None')
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
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(key, six.string_types):
        logger.error('Input key is not of type string')
        raise AssertionError('Input key is not of type string')

    if not key in df.columns:
        logger.error('Input key is not in the dataframe columns')
        raise KeyError('Input key is not in the dataframe columns')


    df = df.set_index(key, drop=False)
    df = df.loc[key_vals, attrs]
    df.reset_index(drop=True, inplace=True)
    df.columns = col_names
    return df


def del_files_in_dir(dir):
    if os.path.isdir(dir):
        filelist = [ f for f in os.listdir(dir)  ]
        for f in filelist:
            p = os.sep.join([dir, f])
            # print(p)
            os.remove(p)

def creat_dir_ifnot_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def convert_to_str_unicode(input_string):

    if not isinstance(input_string, six.string_types):
        input_string = six.u(str(input_string))

    if isinstance(input_string, bytes):
        input_string = input_string.decode('utf-8', 'ignore')

    return input_string

def parse_conjunct(conjunct, feature_table):
    # TODO: Make parsing more robust using pyparsing
    vals = conjunct.split('(')
    feature_name = vals[0].strip()

    if feature_name not in feature_table.feature_name.values:
        logger.error('Feature ' + feature_name + ' is not present in ' +
                     'supplied feature table. Cannot apply rules.')
        raise AssertionError(
            'Feature ' + feature_name + ' is not present ' +
            'in supplied feature table. Cannot apply rules.')

    vals1 = vals[1].split(')')
    vals2 = vals1[1].strip()
    vals3 = vals2.split()
    operator = vals3[0].strip()
    threshold = vals3[1].strip()
    ft_df = feature_table.set_index('feature_name')

    return (ft_df.loc[feature_name]['is_auto_generated'],
            ft_df.loc[feature_name]['simfunction'],
            ft_df.loc[feature_name]['left_attribute'],
            ft_df.loc[feature_name]['right_attribute'],
            ft_df.loc[feature_name]['left_attr_tokenizer'],
            ft_df.loc[feature_name]['right_attr_tokenizer'],
            operator, threshold)

