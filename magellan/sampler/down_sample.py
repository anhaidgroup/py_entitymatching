# coding=utf-8
"""
This module contains sampling related routines
"""
import os

from random import randint
import math
import logging
import random
import numpy as np
import pandas as pd
import pyprind
from magellan.utils.generic_helper import get_install_path
import magellan.catalog.catalog_manager as cm

LOGGER = logging.getLogger(__name__)
INSTALL_PATH = get_install_path()
DATASET_PATH = os.sep.join([INSTALL_PATH, 'utils'])

def _get_stop_words():
    stop_words_set = set()
    stop_words_file = os.sep.join([DATASET_PATH, 'stop_words.txt'])
    with open(stop_words_file, "rb") as stopwords_file:
        for stop_words in stopwords_file:
            stop_words_set.add(stop_words.rstrip())

    return stop_words_set

# get string column list
def _get_str_cols_list(table):

    if len(table) == 0:
        LOGGER.error('_get_str_cols_list: Size of the input table is 0')
        raise AssertionError('_get_str_cols_list: Size of the input table is 0')

    cols = list(table.columns[table.dtypes == object])
    col_list = []
    for attr_x in cols:
        col_list.append(table.columns.get_loc(attr_x))

    return col_list


# create inverted index from token to position
def _inv_index(table):
    """
    This is inverted index function that builds inverted index of tokens on a table

    Args:
        table: dataframe <input table>

    Returns:
        inverted index of type dictionary

    Example:
        inv_index = _inv_index(table)

    """

    stop_words = _get_stop_words()
    str_cols_ix = _get_str_cols_list(table)
    inv_index = dict()
    pos = 0
    for row in table.itertuples():
        str_val = ''
        for list_item in str_cols_ix:
            str_val += str(row[list_item+1]).lower() + ' '
        str_val = str_val.rstrip()
        # tokenize them
        str_val = set(str_val.split())
        str_val = str_val.difference(stop_words)
        for token in str_val:
            lst = inv_index.get(token, None)
            if lst is None:
                inv_index[token] = [pos]
            else:
                lst.append(pos)
                inv_index[token] = lst
        pos += 1
    return inv_index


def _probe_index(table_b, y_param, s_tbl_sz, s_inv_index):
    """
    This is probe index function that probes the second table into inverted index to get
    good coverage in the down sampled output

    Args:
        table_b (dataframe): input table B
        s_tbl_sz: size of table A
        y_param: down_sampled size of table A should be close to size * y_param
        s_inv_index: inverted index built on Table A

    Returns:
        set with indexes

    """
    y_pos = math.floor(y_param/2)
    h_table = set()
    stop_words = _get_stop_words()
    str_cols_ix = _get_str_cols_list(table_b)

    # Progress Bar
    bar = pyprind.ProgBar(len(table_b))

    for row in table_b.itertuples():
        bar.update()
        str_val = ''
        for list_ix in str_cols_ix:
            str_val += str(row[list_ix+1]).lower() + ' '
        str_val = str_val.rstrip()
        str_val = set(str_val.split())
        str_val = str_val.difference(stop_words)
        match = set()
        for token in str_val:
            ids = s_inv_index.get(token, None)
            if ids is not None:
                match.update(ids)

        # pick y/2 elements from m
        k = min(y_pos, len(match))
        match = list(match)
        smpl_pos_neg = set()

        while len(smpl_pos_neg) < k:
            num = random.choice(match)
            smpl_pos_neg.add(num)

        # remaining y_param/2 items are selected here
        while len(smpl_pos_neg) < y_param:
            rand_item_num = randint(0, s_tbl_sz-1)
            smpl_pos_neg.add(rand_item_num)
        h_table.update(smpl_pos_neg)

    return h_table

# down sample of two tables : based on sanjib's index based solution
def down_sample(table_a, table_b, size, y_param):
    """
    This is down sample table function that down samples 2 tables A and B.

    Args:
        table_a (dataframe): input table A
        table_b (dataframe): input table B
        size (int): down_sampled size of table B
        y_param (int): down_sampled size of table A should be close to size * y_param

    Returns:
        down sampled tables A and B

    Raises:
        AssertionError :
        1) If any of the input tables are empty or not a dataframe
        2) If size or y parameter is empty or 0 or not a valid integer value
        3) If output sampled tables are empty or not as per user defined

    Example:
        C, D = mg.down_sample(A, B, b_size, y_param)

    """

    if not isinstance(table_a, pd.DataFrame):
        LOGGER.error('Input table A is not of type pandas dataframe')
        raise AssertionError('Input table A is not of type pandas dataframe')

    if not isinstance(table_b, pd.DataFrame):
        LOGGER.error('Input table B is not of type pandas dataframe')
        raise AssertionError('Input table B is not of type pandas dataframe')

    if len(table_a) == 0 or len(table_b) == 0:
        LOGGER.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    if size == 0 or y_param == 0:
        LOGGER.error('size or y cannot be zero (3rd and 4th parameter of downsample)')
        raise AssertionError('size or y_param cannot be zero (3rd and 4th parameter of downsample)')

    if len(table_b) < size:
        LOGGER.warning('Size of table B is less than b_size parameter - using entire table B')

    s_inv_index = _inv_index(table_a)

    b_sample_size = min(math.floor(size), len(table_b))
    b_tbl_indices = list(np.random.choice(len(table_b), b_sample_size, replace=False))

    s_tbl_indices = _probe_index(table_b.ix[b_tbl_indices], y_param,
                                 len(table_a), s_inv_index)
    s_tbl_indices = list(s_tbl_indices)
    l_sampled = table_a.iloc[list(s_tbl_indices)]
    r_sampled = table_b.iloc[list(b_tbl_indices)]

    # update catalog
    cm.copy_properties(table_a, l_sampled)
    cm.copy_properties(table_b, r_sampled)

    return l_sampled, r_sampled
