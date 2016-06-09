# coding=utf-8
"""
This module contains sampling related routines
"""
from random import randint
import math
import logging

import numpy as np
import pandas as pd

import sys
from datetime import datetime
import time

import random
import pyprind

import magellan.catalog.catalog_manager as cm

logger = logging.getLogger(__name__)

def _get_stop_words():
    stop_words = [
                "a", "about", "above",
                "across", "after", "afterwards", "again", "against", "all", "almost",
                "alone", "along", "already", "also","although","always","am","among",
                "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow",
                "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at",
                "back","be","became", "because", "become", "becomes", "becoming", "been",
                "before", "beforehand", "behind", "being", "below", "beside", "besides",
                "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can",
                "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe",
                "detail", "do", "done", "down", "due", "during", "each", "eg", "eight",
                "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
                "ever", "every", "everyone", "everything", "everywhere", "except", "few",
                "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former",
                "formerly", "forty", "found", "four", "from", "front", "full", "further",
                "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her",
                "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself",
                "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in",
                "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep",
                "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may",
                "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most",
                "mostly", "move", "much", "must", "my", "myself", "name", "namely",
                "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
                "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off",
                "often", "on", "once", "one", "only", "onto", "or", "other", "others",
                "otherwise", "our", "ours", "ourselves", "out", "over", "own","part",
                "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem",
                "seemed", "seeming", "seems", "serious", "several", "she", "should",
                "show", "side", "since", "sincere", "six", "sixty", "so", "some",
                "somehow", "someone", "something", "sometime", "sometimes", "somewhere",
                "still", "such", "system", "take", "ten", "than", "that", "the", "their",
                "them", "themselves", "then", "thence", "there", "thereafter", "thereby",
                "therefore", "therein", "thereupon", "these", "they", "thickv", "thin",
                "third", "this", "those", "though", "three", "through", "throughout",
                "thru", "thus", "to", "together", "too", "top", "toward", "towards",
                "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
                "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
                "whence", "whenever", "where", "whereafter", "whereas", "whereby",
                "wherein", "whereupon", "wherever", "whether", "which", "while",
                "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
                "with", "within", "without", "would", "yet", "you", "your", "yours",
                "yourself", "yourselves"
    ]
    return stop_words

# get string column list
def _get_str_cols_list(table):
    
    if len(table) == 0:
        logger.error('_get_str_cols_list: Size of the input table is 0')
        raise AssertionError('_get_str_cols_list: Size of the input table is 0')
    
    cols = list(table.columns[table.dtypes==object])
    col_list = []
    for x in cols:
        col_list.append(table.columns.get_loc(x))

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

    stop_words = set(_get_stop_words())
    str_cols_ix = _get_str_cols_list(table)
    n = len(table)
    key_pos = dict(zip(range(n), range(n)))
    inv_index = dict()
    pos = 0
    for row in table.itertuples():
        s = ''
        for ix in str_cols_ix:
            s += str(row[ix+1]).lower() + ' '
        s = s.rstrip()
        # tokenize them
        s = set(s.split())
        s = s.difference(stop_words)
        for token in s:
            lst = inv_index.get(token, None)
            if lst is None:
                inv_index[token] = [pos]
            else:
                lst.append(pos)
                inv_index[token] = lst
        pos += 1
    return inv_index


def _probe_index(b_table, y, s_tbl_sz, s_inv_index):
    """
    This is probe index function that probes the second table into inverted index to get good coverage in the down sampled output

    Args:
        b_table: dataframe <input table B>
        s_tbl_sz: size of table A
        y: down_sampled size of table A should be close to size * y
        s_inv_index: inverted index built on Table A
    
    Returns:
        set with indexes
    
    """
    y_pos = math.floor(y/2)
    h_table = set()
    stop_words = set(_get_stop_words())
    str_cols_ix = _get_str_cols_list(b_table)
    
    # Progress Bar
    bar = pyprind.ProgBar(len(b_table))

    for row in b_table.itertuples():
        bar.update()
        s = ''
        for ix in str_cols_ix:
            s += str(row[ix+1]).lower() + ' '
        s = s.rstrip()
        s = set(s.split())
        s = s.difference(stop_words)
        m = set()
        for token in s:
            ids = s_inv_index.get(token, None)
            if ids is not None:
                m.update(ids)

        # pick y/2 elements from m
        k = min(y_pos, len(m))
        m = list(m)
        smpl_pos_neg = set()

        while(len(smpl_pos_neg) < k):
            num = random.choice(m)
            smpl_pos_neg.add(num)
            
        # remaining y/2 items are selected here
        while (len(smpl_pos_neg) < y):
            rand_item_num = randint(0,s_tbl_sz-1)
            smpl_pos_neg.add(rand_item_num)
        h_table.update(smpl_pos_neg)

    return h_table

# down sample of two tables : based on sanjib's index based solution
def down_sample(s_table, b_table, size, y):
    """
    This is down sample table function that down samples 2 tables A and B.

    Args:
        s_table: dataframe <input table A>
        b_table: dataframe <input table B>
        size: down_sampled size of table B
        y: down_sampled size of table A should be close to size * y
    
    Returns:
        down sampled tables A and B

    Raises:
        AssertionError : 1) If any of the input tables are empty or not a dataframe 2) If size or y parameter is empty or 0 or not a valid integer value 3) If output sampled tables are empty
        or not as per user defined

    Example:
        C, D = mg.down_sample(A, B, b_size, y)
    
    """

    if not isinstance(s_table, pd.DataFrame):
        logger.error('Input table A is not of type pandas dataframe')
        raise AssertionError('Input table A is not of type pandas dataframe')

    if not isinstance(b_table, pd.DataFrame):
        logger.error('Input table B is not of type pandas dataframe')
        raise AssertionError('Input table B is not of type pandas dataframe')

    if len(s_table) == 0 or len(b_table) == 0:
        logger.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    if size == 0 or y == 0:
        logger.error('size or y cannot be zero (3rd and 4th parameter of downsample)')
        raise AssertionError('size or y cannot be zero (3rd and 4th parameter of downsample)')

    if len(b_table) < size:
        logger.warning('Size of table B is less than b_size parameter - using entire table B')

    s_inv_index = _inv_index(s_table)

    b_sample_size = min(math.floor(size), len(b_table))
    b_tbl_indices = list(np.random.choice(len(b_table), b_sample_size, replace=False))

    s_tbl_indices = _probe_index(b_table.ix[b_tbl_indices], y,
                                 len(s_table), s_inv_index)
    s_tbl_indices = list(s_tbl_indices)
    l_sampled = s_table.iloc[list(s_tbl_indices)]
    r_sampled = b_table.iloc[list(b_tbl_indices)]

    # update catalog
    cm.copy_properties(s_table, l_sampled)
    cm.copy_properties(b_table, r_sampled)

    return l_sampled, r_sampled
