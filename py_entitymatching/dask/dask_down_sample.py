# coding=utf-8
import collections
import logging
import math
import multiprocessing
import os
import random
import re
from collections import Counter

import pandas as pd
import numpy as np
from dask import delayed
from dask.diagnostics import ProgressBar

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.dask.utils import wrap, get_num_cores
from py_entitymatching.utils.generic_helper import get_install_path
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)
regex = re.compile('[%s]' % re.escape('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'))




def dask_down_sample(ltable, rtable, size, y_param, show_progress=True, verbose=False,
                seed=None, rem_stop_words=True, rem_puncs=True, n_ltable_chunks=1,
                n_sample_rtable_chunks=1):


    """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.
         
        This command down samples two tables A and B into smaller tables A' and
        B' respectively.    
        Specifically, first it randomly selects `size` tuples
        from the table B to be table B'. Next, it builds an inverted index I
        (token, tuple_id) on table A. For each tuple x ∈ B', the algorithm
        finds a set P of k/2 tuples from I that match x,
        and a set Q of k/2 tuples randomly selected from A - P.
        The idea is for A' and B' to share some matches yet be
        as representative of A and B as possible.
    
        Args:
            ltable (DataFrame): The left input table, i.e., table A.
            rtable (DataFrame): The right input table, i.e., table B. 
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
            n_ltable_chunks (int): The number of partitions for ltable (defaults to 1). If it 
              is set to -1, the number of partitions will be set to the 
              number of cores in the machine.  
            n_sample_rtable_chunks (int): The number of partitions for the 
              sampled rtable (defaults to 1)
                
    
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
            AssertionError: If `n_ltable_chunks` is not of type int.
            AssertionError: If `n_sample_rtable_chunks` is not of type int.            
    
        Examples:
            >>> from py_entitymatching.dask.dask_down_sample import dask_down_sample
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> sample_A, sample_B = dask_down_sample(A, B, 500, 1, n_ltable_chunks=-1, n_sample_rtable_chunks=-1)
            # Example with seed = 0. This means the same sample data set will be returned
            # each time this function is run.
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> sample_A, sample_B = dask_down_sample(A, B, 500, 1, seed=0, n_ltable_chunks=-1, n_sample_rtable_chunks=-1)
            
        """

    logger.warning(
        "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
        "RISK.")

    # validation checks
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input table A (ltable) is not of type pandas DataFrame')
        raise AssertionError(
            'Input table A (ltable) is not of type pandas DataFrame')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input table B (rtable) is not of type pandas DataFrame')

        raise AssertionError(
            'Input table B (rtable) is not of type pandas DataFrame')

    if len(ltable) == 0 or len(rtable) == 0:
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

    if len(rtable) < size:
        logger.warning(
            'Size of table B is less than b_size parameter - using entire table B')

    validate_object_type(verbose, bool, 'Parameter verbose')
    validate_object_type(show_progress, bool, 'Parameter show_progress')
    validate_object_type(rem_stop_words, bool, 'Parameter rem_stop_words')
    validate_object_type(rem_puncs, bool, 'Parameter rem_puncs')
    validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
    validate_object_type(n_sample_rtable_chunks, int, 'Parameter n_sample_rtable_chunks')


    rtable_sampled = sample_right_table(rtable, size, seed)

    ltbl_str_cols = _get_str_cols_list(ltable)
    proj_ltable = ltable[ltable.columns[ltbl_str_cols]]


    if n_ltable_chunks == -1:
        n_ltable_chunks = get_num_cores()

    ltable_chunks = np.array_split(proj_ltable, n_ltable_chunks)
    preprocessed_tokenized_tbl = []

    # Use Dask to preprocess and tokenize strings.
    start_row_id = 0
    for i in range(len(ltable_chunks)):
        # start_row_id is internally used by process_tokenize_concat strings to map
        # each to its row id in the ltable.
        result = delayed(process_tokenize_concat_strings)(ltable_chunks[i],
                                                             start_row_id,
                                                             rem_puncs, rem_stop_words)
        preprocessed_tokenized_tbl.append(result)

        # update start_row_id
        start_row_id += len(ltable_chunks[i])

    preprocessed_tokenized_tbl = delayed(wrap)(preprocessed_tokenized_tbl)

    # Now execute the DAG
    if show_progress:
        with ProgressBar():
            logger.info('Preprocessing/tokenizing ltable')
            preprocessed_tokenized_tbl_vals = preprocessed_tokenized_tbl.compute(
                scheduler="processes", num_workers=get_num_cores())
    else:
        preprocessed_tokenized_tbl_vals = preprocessed_tokenized_tbl.compute(
            scheduler="processes", num_workers=get_num_cores())

    ltable_processed_dict = {}
    for i in range(len(preprocessed_tokenized_tbl_vals)):
        ltable_processed_dict.update(preprocessed_tokenized_tbl_vals[i])

    # Build an inverted index
    inverted_index = build_inverted_index(ltable_processed_dict)


    # Preprocess/tokenize sampled rtable and probe
    rtbl_str_cols = _get_str_cols_list(rtable_sampled)
    proj_rtable_sampled = rtable_sampled[rtable_sampled.columns[rtbl_str_cols]]


    if n_sample_rtable_chunks == -1:
        n_sample_rtable_chunks = get_num_cores()

    rtable_chunks = np.array_split(proj_rtable_sampled, n_sample_rtable_chunks)
    probe_result = []

    # Create the DAG
    for i in range(len(rtable_chunks)):
        result = delayed(probe)(rtable_chunks[i], y_param, len(proj_ltable),
                                              inverted_index, rem_puncs,
                                rem_stop_words, seed)
        probe_result.append(result)

    probe_result = delayed(wrap)(probe_result)

    # Execute the DAG
    if show_progress:
        with ProgressBar():
            logger.info('Probing using rtable')
            probe_result = probe_result.compute(scheduler="processes",
                                                num_workers=multiprocessing.cpu_count())
    else:
        probe_result = probe_result.compute(scheduler="processes",
                                            num_workers=multiprocessing.cpu_count())

    probe_result = map(list, probe_result)
    l_tbl_indices = set(sum(probe_result, []))

    l_tbl_indices = list(l_tbl_indices)
    ltable_sampled = ltable.iloc[l_tbl_indices]



    # update catalog
    if cm.is_dfinfo_present(ltable):
        cm.copy_properties(ltable, ltable_sampled)

    if cm.is_dfinfo_present(rtable):
        cm.copy_properties(rtable, rtable_sampled)

    return ltable_sampled, rtable_sampled

# --- helper functions ----- #

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

def process_tokenize_concat_strings(table, start_row_id,
                                    should_rem_puncs, should_rem_stop_words):
    result_vals = {}
    row_id = start_row_id
    for row in table.itertuples(index=False):
        str_val = ' '.join(col_val.lower().strip() for col_val in row[:] if not
        pd.isnull(col_val))
        if should_rem_puncs:
            str_val = remove_punctuations(str_val)
        # tokenize them
        str_val = set(str_val.split())
        if should_rem_stop_words:
            stop_words = _get_stop_words()
            str_val = str_val.difference(stop_words)
        result_vals[row_id] = str_val
        row_id += 1
    return result_vals

def build_inverted_index(process_tok_vals):
    index = collections.defaultdict(set)
    for key in process_tok_vals:
        for val in process_tok_vals[key]:
            index[val].add(key)
    return index

def probe(table, y_param, size, inverted_index,  should_rem_puncs, should_rem_stop_words, seed):
    y_pos = math.ceil(y_param/2.0)
    h_table = set()
    stop_words = []
    if should_rem_stop_words:
        stop_words = _get_stop_words()

    for row in table.itertuples(index=False):
        id_freq_counter = Counter()
        str_val = ' '.join(col_val.lower().strip() for col_val in row[:] if not
        pd.isnull(col_val))

        if should_rem_puncs:
            str_val = remove_punctuations(str_val)
        str_val = set(str_val.strip().split())
        if should_rem_stop_words:
            str_val = str_val.difference(stop_words)

        for token in str_val:
            ids = inverted_index.get(token, None)
            if ids is not None:
                id_freq_counter.update(Counter(ids))
            m = min(y_pos, len(id_freq_counter))

            most_common_ids = id_freq_counter.most_common(int(m))
            smpl_pos_neg = set(key for key, val in most_common_ids)

            if seed is not None:
                random.seed(seed)

            while len(smpl_pos_neg) < y_param:
                rand_item_num = random.randint(0, size-1)
                smpl_pos_neg.add(rand_item_num)
        h_table.update(smpl_pos_neg)
    return h_table



def sample_right_table(rtable, size, seed):
    return rtable.sample(size, random_state=seed)
