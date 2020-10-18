# coding=utf-8
"""
This module contains sampling related routines
"""
from __future__ import division

from joblib import Parallel, delayed
import logging
import math
import multiprocessing
import os
import random
import re
import string
from collections import Counter

import pandas as pd
import numpy as np
import pyprind
from numpy.random import RandomState

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.catalog_helper import log_info
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)
regex = re.compile('[%s]' % re.escape('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'))

def get_num_procs(n_jobs, min_procs):
    # determine number of processes to launch parallely
    n_cpus = multiprocessing.cpu_count()
    n_procs = n_jobs
    if n_jobs < 0:
        n_procs = n_cpus + 1 + n_jobs
    # cannot launch less than min_procs to safeguard against small tables
    return min(n_procs, min_procs)


def _get_stop_words():
    stop_words_set = set()
    install_path = get_install_path()
    dataset_path = os.sep.join([install_path, 'utils'])
    stop_words_file = os.sep.join([dataset_path, 'stop_words.txt'])
    with open(stop_words_file, "rb") as stopwords_file:
        for stop_words in stopwords_file:
            stop_words_set.add(stop_words.rstrip())

    return stop_words_set

def remove_punctuations(s):
    return regex.sub('', s)


# get string column list
def _get_str_cols_list(table):
    if len(table) == 0:
        error_message = 'Size of the input table is 0'
        logger.error(error_message)
        raise AssertionError(error_message)

    cols = list(table.columns[table.dtypes == object])
    col_list = []
    for attr_x in cols:
        col_list.append(table.columns.get_loc(attr_x))
    return col_list


# create inverted index from token to position
def _inv_index(table, rem_stop_words=True, rem_puncs=True):
    """

    This is inverted index function that builds inverted index of tokens on a table

    """

    # Get the stop words listed by user in the file specified by dataset path
    stop_words = _get_stop_words()

    # Extract indices of all string columns (if any) from the input DataFrame
    str_cols_ix = _get_str_cols_list(table)

    inv_index = dict()
    pos = 0

    # First project the input table to include only the string columns
    proj_table = table[table.columns[str_cols_ix]]

    # For each row in the DataFrame of the projected input table, we will fetch all
    # string values from string column indices
    # and will concatenate them. Next step would be to tokenize them using set and
    # remove all the stop words from the list of tokens.
    # Once we have the list of tokens, we will iterate through the list of tokens
    # to identify its position and will create an inverted index.

    for row in proj_table.itertuples(index=False):
        # str_val = ''
        # for list_item in str_cols_ix:
        #     str_val += str(row[list_item]).lower() + ' '

        str_val = ' '.join(col_val.lower().strip() for col_val in row[:] if not
        pd.isnull(col_val))
        if rem_puncs:
            str_val = remove_punctuations(str_val)
        str_val = str_val.rstrip()

        # tokenize them
        str_val = set(str_val.split())
        if rem_stop_words:
            str_val = str_val.difference(stop_words)

        # building inverted index I from set of tokens
        for token in str_val:
            lst = inv_index.get(token, None)
            if lst is None:
                inv_index[token] = [pos]
            else:
                lst += [pos]
        pos += 1
    return inv_index


def _probe_index_split(table_b, y_param, s_tbl_sz, s_inv_index, show_progress=True,
                       seed=None, rem_stop_words=True, rem_puncs=True):
    """
    This is probe index function that probes the second table into inverted index to get
    good coverage in the down sampled output

    """

    y_pos = math.ceil(y_param / 2.0)
    h_table = set()
    stop_words = _get_stop_words()
    str_cols_ix = _get_str_cols_list(table_b)

    # Progress Bar
    if show_progress:
        bar = pyprind.ProgBar(len(table_b))

    proj_table_b = table_b[table_b.columns[str_cols_ix]]
    # For each tuple x ∈ B', we will probe inverted index I built in the previous step to find all tuples in A
    # (inverted index) that share tokens with x. We will rank these tuples in decreasing order of shared tokens, then
    # take (up to) the top k/2 tuples to be the set P.

    for row in proj_table_b.itertuples(index=False):
        id_freq_counter = Counter()  # keeps track of ids->frequency while probing inverted
        # index.
        if show_progress:
            bar.update()

        # For all string column in the table, fetch all string values and concatenate them
        # for list_ix in str_cols_ix:
        #     str_val += str(row[list_ix]).lower() + ' '
        str_val = ' '.join(col_val.lower().strip() for col_val in row[:] if not
        pd.isnull(col_val))
        if rem_puncs:
            str_val = remove_punctuations(str_val)
        str_val = str_val.rstrip()

        # Tokenizing the string value and removing stop words before we start probing into inverted index I
        str_val = set(str_val.split())
        if rem_stop_words:
            str_val = str_val.difference(stop_words)

        # For each token in the set, we will probe the token into inverted index I to get set of y/2 positive matches

        for token in str_val:
            ids = s_inv_index.get(token, None)
            if ids is not None:
                id_freq_counter.update(Counter(ids))


                # ids_dict = {}
                # for token in str_val:
                #     ids = s_inv_index.get(token, None)
                #     if ids is not None:
                #         for id in ids:
                #             if id not in ids_dict:
                #                 ids_dict[id] = 1
                #             else:
                #                 ids_dict[id] = ids_dict[id] + 1

                # match.update(ids)

        # Pick y/2 elements from match
        m = min(y_pos, len(id_freq_counter))
        # match = list(match)

        most_common_id_freqs = id_freq_counter.most_common(int(m))
        smpl_pos_neg = set(key for key, val in most_common_id_freqs)

        # num_pos = 0
        # sorted_key_values = [(k, v) for v, k in sorted(
        #     [(v, k) for k, v in ids_dict.items()], reverse=True
        # )]
        # for t in sorted_key_values:
        #     if num_pos >= m:
        #         break
        #     smpl_pos_neg.add(t[0])
        #     num_pos += 1

        # while len(smpl_pos_neg) < k:
        #     num = random.choice(match)
        #     smpl_pos_neg.add(num)

        # Remaining y_param/2 items are selected here randomly. This is to get better coverage from both the input
        # tables
        if seed is not None:
            random.seed(seed)
        while len(smpl_pos_neg) < y_param:
            rand_item_num = random.randint(0, s_tbl_sz - 1)
            smpl_pos_neg.add(rand_item_num)
        h_table.update(smpl_pos_neg)

    return h_table


# down sample of two tables : based on sanjib's index based solution
def down_sample(table_a, table_b, size, y_param, show_progress=True,
                verbose=False, seed=None, rem_stop_words=True,
                rem_puncs=True, n_jobs=1):
    """
    This function down samples two tables A and B into smaller tables A' and
    B' respectively.

    Specifically, first it randomly selects `size` tuples
    from the table B to be table B'. Next, it builds an inverted index I
    (token, tuple_id) on table A. For each tuple x ∈ B', the algorithm
    finds a set P of k/2 tuples from I that match x,
    and a set Q of k/2 tuples randomly selected from A - P.
    The idea is for A' and B' to share some matches yet be
    as representative of A and B as possible.

    Args:
        table_a,table_b (DataFrame): The input tables A and B.
        size (int): The size that table B should be down sampled to.
        y_param (int): The parameter to control the down sample size of table A.
            Specifically, the down sampled size of table A should be close to
            size * y_param.
        show_progress (boolean): A flag to indicate whether a progress bar
            should be displayed (defaults to True).
        verbose (boolean): A flag to indicate whether the debug information
         should be displayed (defaults to False).
        seed (int): The seed for the pseudo random number generator to select
            the tuples from A and B (defaults to None).
        rem_stop_words (boolean): A flag to indicate whether a default set of stop words 
         must be removed.
        rem_puncs (boolean): A flag to indicate whether the punctuations must be 
         removed from the strings.
        n_jobs (int): The number of parallel jobs to be used for computation
            (defaults to 1). If -1 all CPUs are used. If 0 or 1,
            no parallel computation is used at all, which is useful for
            debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
            used (where n_cpus is the total number of CPUs in the
            machine). Thus, for n_jobs = -2, all CPUs but one are used.
            If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
            computation is used (i.e., equivalent to the default).
            

    Returns:
        Down sampled tables A and B as pandas DataFrames.

    Raises:
        AssertionError: If any of the input tables (`table_a`, `table_b`) are
            empty or not a DataFrame.
        AssertionError: If `size` or `y_param` is empty or 0 or not a
            valid integer value.
        AssertionError: If `seed` is not a valid integer
            value.
        AssertionError: If `verbose` is not of type bool.
        AssertionError: If `show_progress` is not of type bool.
        AssertionError: If `n_jobs` is not of type int.

    Examples:
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> sample_A, sample_B = em.down_sample(A, B, 500, 1, n_jobs=-1)

        # Example with seed = 0. This means the same sample data set will be returned
        # each time this function is run.
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> sample_A, sample_B = em.down_sample(A, B, 500, 1, seed=0, n_jobs=-1)
    """

    if not isinstance(table_a, pd.DataFrame):
        logger.error('Input table A is not of type pandas DataFrame')
        raise AssertionError(
            'Input table A is not of type pandas DataFrame')

    if not isinstance(table_b, pd.DataFrame):
        logger.error('Input table B is not of type pandas DataFrame')
        raise AssertionError(
            'Input table B is not of type pandas DataFrame')

    if len(table_a) == 0 or len(table_b) == 0:
        logger.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    if size == 0 or y_param == 0:
        logger.error(
            'size or y cannot be zero (3rd and 4th parameter of downsample)')
        raise AssertionError(
            'size or y_param cannot be zero (3rd and 4th parameter of downsample)')

    if seed is not None and not isinstance(seed, int):
        logger.error('Seed is not of type integer')
        raise AssertionError('Seed is not of type integer')

    if len(table_b) < size:
        logger.warning(
            'Size of table B is less than b_size parameter - using entire table B')

    validate_object_type(verbose, bool, 'Parameter verbose')
    validate_object_type(show_progress, bool, 'Parameter show_progress')
    validate_object_type(rem_stop_words, bool, 'Parameter rem_stop_words')
    validate_object_type(rem_puncs, bool, 'Parameter rem_puncs')
    validate_object_type(n_jobs, int, 'Parameter n_jobs')

    # get and validate required metadata
    log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

    # # # get metadata
    # l_key, r_key = cm.get_keys_for_ltable_rtable(table_a, table_b, logger,
    #                                              verbose)
    #
    # # # validate metadata
    # cm._validate_metadata_for_table(table_a, l_key, 'ltable', logger,
    #                                 verbose)
    # cm._validate_metadata_for_table(table_b, r_key, 'rtable', logger,
    #                                 verbose)

    # Inverted index built on table A will consist of all tuples in such P's and Q's - central idea is to have
    # good coverage in the down sampled A' and B'.
    s_inv_index = _inv_index(table_a, rem_stop_words, rem_puncs)

    # Randomly select size tuples from table B to be B'
    # If a seed value has been give, use a RandomState with the given seed
    b_sample_size = min(math.floor(size), len(table_b))
    if seed is not None:
        rand = RandomState(seed)
    else:
        rand = RandomState()
    b_tbl_indices = list(rand.choice(len(table_b), int(b_sample_size), replace=False))

    n_jobs = get_num_procs(n_jobs, len(table_b))

    sample_table_b = table_b.loc[b_tbl_indices]
    if n_jobs <= 1:
        # Probe inverted index to find all tuples in A that share tokens with tuples in B'.
        s_tbl_indices = _probe_index_split(sample_table_b, y_param,
                                           len(table_a), s_inv_index, show_progress,
                                           seed, rem_stop_words, rem_puncs)
    else:
        sample_table_splits = np.array_split(sample_table_b, n_jobs)
        results = Parallel(n_jobs=n_jobs)(
            delayed(_probe_index_split)(sample_table_splits[job_index], y_param,
                                       len(table_a), s_inv_index,
                                        (show_progress and (job_index == n_jobs - 1)),
                                       seed, rem_stop_words, rem_puncs)
            for job_index in range(n_jobs)
        )
        results = map(list, results)
        s_tbl_indices = set(sum(results, []))

    s_tbl_indices = list(s_tbl_indices)
    l_sampled = table_a.iloc[list(s_tbl_indices)]
    r_sampled = table_b.iloc[list(b_tbl_indices)]

    # update catalog
    if cm.is_dfinfo_present(table_a):
        cm.copy_properties(table_a, l_sampled)
    if cm.is_dfinfo_present(table_b):
        cm.copy_properties(table_b, r_sampled)

    return l_sampled, r_sampled
