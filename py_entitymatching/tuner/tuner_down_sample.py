import logging

from dask import delayed
import math
import multiprocessing
import time

import pandas as pd
import numpy as np
from dask import delayed
from dask.diagnostics import ProgressBar

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.dask.dask_down_sample import sample_right_table, \
    _get_str_cols_list, process_tokenize_concat_strings, remove_punctuations, \
    _get_stop_words, build_inverted_index, probe
from py_entitymatching.utils.validation_helper import validate_object_type



logger = logging.getLogger(__name__)

# ----------------------------

def tuner_down_sample(ltable, rtable, size, y_param, seed, rem_stop_words,
                      rem_puncs, n_bins=50, sample_proportion=0.1, repeat=1):
    """
    WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.
    
    Tunes the parameters for down sampling command implemented using Dask. 
    
    Given the input tables and the parameters for Dask-based down sampling command, 
    this command returns the configuration including whether the input tables need to 
    be swapped, the number of left table chunks, and the number of right table chunks. 
    It uses "Staged Tuning" approach to select the configuration setting. The key idea 
    of this approach select the configuration for one parameter at a time.
    
    Conceptually, this command performs the following steps. First, it samples the 
    left table and down sampled rtable using stratified sampling. Next, it uses the 
    sampled tables to decide if they need to be swapped or not (by running the down 
    sample command and comparing the runtimes). Next, it finds the number of rtable 
    partitions using the sampled tables (by trying the a fixed set of partitions and 
    comparing the runtimes). The number of partitions is selected to be the number 
    before which the runtime starts increasing. Then it finds the number of right table 
    partitions similar to selecting the number of left table partitions. while doing 
    this, set the number of right table partitions is set to the value found in the 
    previous step. Finally, it returns the configuration setting back to the user as a 
    triplet (x, y, z) where x indicates if the tables need to be swapped or not, 
    y indicates the number of left table partitions (if the tables need to be swapped, 
    then this indicates the number of left table partitions after swapping), 
    and z indicates the number of down sampled right table partitions. 
    
    Args:
        ltable (DataFrame): The left input table, i.e., table A.
        rtable (DataFrame): The right input table, i.e., table B. 
        size (int): The size that table B should be down sampled to.
        y_param (int): The parameter to control the down sample size of table A.
                Specifically, the down sampled size of table A should be close to
                size * y_param.
        seed (int): The seed for the pseudo random number generator to select
                the tuples from A and B (defaults to None).
        rem_stop_words (boolean): A flag to indicate whether a default set of stop words 
             must be removed.
        rem_puncs (boolean): A flag to indicate whether the punctuations must be 
             removed from the strings.
        n_bins (int): The number of bins to be used for stratified sampling.
        sample_proportion (float): The proportion used to sample the tables. This value
            is expected to be greater than 0 and less thank 1.
        repeat (int): The number of times to execute the down sample command while 
            selecting the values for the parameters.
    
    Returns:
        A tuple containing 3 values. For example if the tuple is represented as (x, y, 
        z) then x indicates if the tables need to be swapped or not, y indicates the number of 
        left table partitions (if the tables need to be swapped, then this indicates the 
        number of left table partitions after swapping), and z indicates the number of 
        down sampled right table partitions. 
            
    Examples:
        >>> from py_entitymatching.tuner.tuner_down_sample import tuner_down_sample
        >>> (swap_or_not, n_ltable_chunks, n_sample_rtable_chunks) = tuner_down_sample(ltable, rtable, size, y_param, seed, rem_stop_words, rem_puncs)
     
    """
    logger.info('WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN '
                'RISK.')

    # Sample the tables

    # # Down sample the right table
    rtable_sampled = sample_right_table(rtable, size, seed)

    # # Sample the tables for selecting the configuration.
    sampled_tables_orig_order = get_sampled_tables(ltable, rtable_sampled,
                                                   rem_puncs, rem_stop_words, n_bins,
                                                   sample_proportion, seed)

    # # Repeat the above two steps, but swap the left and right input tables
    ltable_sampled = sample_right_table(ltable, size, seed)
    sampled_tables_swap_order = get_sampled_tables(rtable, ltable_sampled,
                                                   rem_puncs,
                                                   rem_stop_words, n_bins,
                                                   sample_proportion, seed)

    # select if the tables need to be swapped
    swap_config = should_swap(sampled_tables_orig_order,
                              sampled_tables_swap_order,
                              y_param, seed, rem_puncs,
                              rem_stop_words,
                              repeat=repeat)

    # use the appropriate sampled tables for further processing
    s_ltable, s_rtable = sampled_tables_orig_order
    if swap_config == True:
        s_ltable, s_rtable = sampled_tables_swap_order

    # print('Swapping: {0} done'.format(swap_config))

    # use the sampled tables to find the number of right table partitions
    n_rtable_chunks = find_rtable_chunks(s_ltable, s_rtable, y_param, seed, rem_puncs,
                                         rem_stop_words)

    # use the sampled tables to find the number of left table partitions
    n_ltable_chunks = find_ltable_chunks(s_ltable, s_rtable, y_param, seed, rem_puncs,
                                         rem_stop_words, n_rtable_chunks)


    # return the configuration back to the user
    return (swap_config, n_ltable_chunks, n_rtable_chunks)


# ------ utility functions for tuner ---- #

def get_sampled_tables(ltable, rtable, should_rem_puncs, should_rem_stop_words, n_bins,
                       sample_proportion, seed):
    """
    Function to sample the tables. 
    """

    # sample left table. specifically, sample it by stratifying on the lengths of the
    # concatenated string attributes.
    s_ltable, processed_tokenized_result = sample_ltable(ltable, should_rem_puncs,
                                                         should_rem_stop_words, n_bins, sample_proportion, seed)

    inverted_index = build_inverted_index(processed_tokenized_result)

    # sample left table. specifically, sample it by stratifying on the lnumber of
    # tuples that each tuple in down sampled right table must probe
    s_rtable = sample_rtable(rtable, should_rem_puncs, should_rem_stop_words, n_bins,
                             sample_proportion, inverted_index, seed)

    # return the sampled ltable and rtable
    return s_ltable, s_rtable





def execute(ltable, rtable, y_param, seed, should_rem_puncs,
            should_rem_stop_words, n_ltable_chunks,
            n_rtable_chunks, repeat):
    """
    Function to execute the down sample command and return the run time,     
    """
    times = []
    for i in range(repeat):
        t1 = time.time()
        _, _ = _down_sample(ltable, rtable, y_param, show_progress=False, seed=seed,
                            rem_stop_words=should_rem_stop_words, rem_puncs = should_rem_puncs, n_ltable_chunks=n_ltable_chunks, n_rtable_chunks=n_rtable_chunks)
        t2 = time.time()
        times.append(t2-t1)
    return np.mean(times)


def should_swap(orig_order, swap_order,  y_param, seed, should_rem_puncs,
                should_rem_stop_words, repeat=1,
                epsilon=1):
    """
    Function to decide if we need to swap the input tables or npt    
    """

    # Execute the command using sampled input tables in the original order
    t_orig_order = execute(orig_order[0], orig_order[1], y_param, seed, should_rem_puncs,
                           should_rem_stop_words, -1, -1, repeat)

    # Execute the command using sampled input tables in the swap order
    t_swap_order = execute(swap_order[0], swap_order[1], y_param, seed, should_rem_puncs,
                           should_rem_stop_words, -1, -1, repeat)

    swap_config = False

    # Here epsilon is used to get the config such that the runtime difference greather
    # than espsilon
    if t_swap_order < t_orig_order+epsilon:
        swap_config = True

    return swap_config



def get_chunk_range():
    """
    Get the range of partitions to try.
     
    """
    n_chunks = multiprocessing.cpu_count()
    if n_chunks > 128:
        raise NotImplementedError('Currently we consider the num. procs in machine to '
                                  'be < 128')
    chunk_range = [n_chunks]
    while n_chunks < 128:
        n_chunks *= 2
        chunk_range += [n_chunks]
    return chunk_range



def find_rtable_chunks(ltable, rtable, y_param, seed, should_rem_puncs,
                       should_rem_stop_words, repeat=1, epsilon=0):

    """
    Function to find the number of down sampled right table chunks    
    """

    # Get the chunk range
    chunk_range = get_chunk_range()
    hist_times = []
    hist_chunks = []

    # Select the number of partitions by executing it over the values in the chunk range
    t = execute(ltable, rtable, y_param, seed, should_rem_puncs,
                should_rem_stop_words,
                n_rtable_chunks=chunk_range[0], n_ltable_chunks=-1, repeat=repeat)
    hist_times += [t]
    hist_chunks += [chunk_range[0]]



    for n_chunks in chunk_range[1:]:
        t = execute(ltable, rtable,y_param, seed, should_rem_puncs,
                    should_rem_stop_words,
                    n_rtable_chunks=n_chunks, n_ltable_chunks=-1, repeat=repeat)

        # print('{0} chunk: {1} time: {2}'.format('Rtable', n_chunks, t))


        if t > hist_times[-1]+epsilon:
            return hist_chunks[-1]
        else:
            hist_times += [t]
            hist_chunks += [n_chunks]
    return hist_chunks[-1]


def find_ltable_chunks(ltable, rtable, y_param, seed, should_rem_puncs,
                       should_rem_stop_words, n_rtable_chunks, repeat=1,
                       epsilon=1):
    """
    Function to find the number of left table chunks    
    """

    # Get the chunk range to try
    chunk_range = get_chunk_range()
    hist_times = []
    hist_chunks = []

    # Select the number of partitions by executing it over the values in the chunk range
    t = execute(ltable, rtable, y_param, seed, should_rem_puncs,
                should_rem_stop_words,
                n_ltable_chunks=chunk_range[0], n_rtable_chunks=n_rtable_chunks,
                repeat=repeat)
    hist_times += [t]
    hist_chunks += [chunk_range[0]]



    for n_chunks in chunk_range[1:]:
        t = execute(ltable, rtable, y_param, seed, should_rem_puncs,
                    should_rem_stop_words,
                    n_ltable_chunks=n_chunks, n_rtable_chunks=n_rtable_chunks, repeat=repeat)


        if t > hist_times[-1]+epsilon:
            return hist_chunks[-1]
        else:
            hist_times += [t]
            hist_chunks += [n_chunks]

    return hist_chunks[-1]


def sample_ltable(table, should_rem_puncs, should_rem_stop_words, n_bins,
                  sample_proportion,
                  seed):
    """
    Function to sample the left table    
    """

    tbl_str_cols = _get_str_cols_list(table)
    proj_table = table[table.columns[tbl_str_cols]]

    # preprocess and tokenize the concatenated strings
    processed_concat_result, processed_tokenized_result = _process_tokenize_concat_strings(proj_table, 0,
                                                                  should_rem_puncs,
                                                                 should_rem_stop_words)

    # define temporary columns to store thr concatenated column, their strlengyhs, etc.
    concat_col = '__concat_column__'
    strlen_col = '__strlen_column__'
    key_col = '__key_colum__'
    df = pd.DataFrame.from_dict(processed_concat_result, orient="index", columns=[concat_col])
    df.insert(0, key_col, df.index.values)
    df.reset_index(inplace=True, drop=True)


    # get the number of samples to be selected from each bin
    num_samples = int(math.floor(sample_proportion*len(proj_table)))

    # get the string lengths and then group by the string lengths
    df[strlen_col] = df[concat_col].str.len()
    len_groups = df.groupby(strlen_col)

    group_ids_len = {}
    for group_id, group in len_groups:
        group_ids_len[group_id] = list(group[key_col])

    str_lens = list(df[strlen_col].values)
    str_lens += [max(str_lens) + 1]

    # bin the string lengths
    freq, edges = np.histogram(str_lens, bins=n_bins)

    # compute the bin to which the string length map to
    bins = [[] for _ in range(n_bins)]
    keys = sorted(group_ids_len.keys())
    positions = np.digitize(keys, edges)

    # compute the number of entries in each bin
    for i in range(len(keys)):
        k, p = keys[i], positions[i]
        bins[p-1].extend(group_ids_len[k])
    len_of_bins = [len(bins[i]) for i in range(len(bins))]

    # compute the relative weight of each bin and the number of tuples we need to
    # sample from each bin
    weights = [len_of_bins[i]/float(sum(len_of_bins)) for i in range(len(bins))]
    num_tups_from_each_bin = [int(math.ceil(weights[i]*num_samples)) for i in range(
        len(weights))]

    # sample from each bin
    sampled = []
    for i in range(len(bins)):
        num_tuples = num_tups_from_each_bin[i]
        if len_of_bins[i]:
            np.random.seed(seed)
            tmp_samples = np.random.choice(bins[i], num_tuples, replace=False)
            if len(tmp_samples):
                sampled.extend(tmp_samples)

    # retain the same order of tuples as in the input table
    table_copy = table.copy()
    table_copy[key_col] = range(len(table))
    table_copy = table_copy.set_index(key_col)
    table_copy['_pos'] = list(range(len(table)))
    s_table = table_copy.loc[sampled]
    s_table = s_table.sort_values('_pos')

    # reset the index and drop the pos column
    s_table.reset_index(drop=False, inplace=True)
    s_table.drop(['_pos'], axis=1, inplace=True)

    return s_table, processed_tokenized_result


#--------------- sample rtable -----------------
def sample_rtable(table, should_rem_puncs, should_rem_stop_words, n_bins,
                  sample_proportion,
                  inverted_index, seed):
    """
    Function to sample the down sampled right table.    
    """

    tbl_str_cols = _get_str_cols_list(table)
    proj_table = table[table.columns[tbl_str_cols]]
    processed_tokenized_vals = process_tokenize_concat_strings(proj_table, 0, should_rem_puncs,
                                                               should_rem_stop_words)
    key = '__key_colum__'
    proj_table.insert(0, key, range(len(proj_table)))
    proj_table.reset_index(inplace=True, drop=True)

    token_cnt = {}
    token_map = {}

    for row_id in processed_tokenized_vals.keys():
        tuple_id = proj_table.iloc[row_id][key]
        tokens = processed_tokenized_vals[row_id]
        cnt = 0
        for token in tokens:
            if token not in token_map:
                token_map[token] = len(inverted_index[token])
            cnt += token_map[token]
        token_cnt[tuple_id] = cnt
    # cnt_df = pd.DataFrame(token_cnt.items(), columns=[key, 'count'])
    cnt_df = pd.DataFrame.from_dict(token_cnt, orient='index', columns=['count'])
    cnt_df.insert(0, key, cnt_df.index.values)
    cnt_df.reset_index(drop=False, inplace=True)


    count_groups = cnt_df.groupby('count')
    cnt_ids = {}

    for group_id, group in count_groups:
        cnt_ids[group_id] = list(group[key].values)

    counts = list(cnt_df['count'].values)
    counts += [max(counts) + 1]

    freq, edges = np.histogram(counts, bins=n_bins)


    # get the number of samples to be selected from each bin
    num_samples = int(math.floor(sample_proportion * len(table)))

    # compute the bin to which the string length map to
    bins = [[] for _ in range(n_bins)]
    keys = sorted(cnt_ids.keys())
    positions = np.digitize(keys, edges)

    # compute the number of entries in each bin
    for i in range(len(keys)):
        k, p = keys[i], positions[i]
        bins[p - 1].extend(cnt_ids[k])
    len_of_bins = [len(bins[i]) for i in range(len(bins))]

    # compute the relative weight of each bin and the number of tuples we need to
    # sample from each bin
    weights = [len_of_bins[i] / float(sum(len_of_bins)) for i in range(len(bins))]
    num_tups_from_each_bin = [int(math.ceil(weights[i] * num_samples)) for i in range(
        len(weights))]

    # sample from each bin
    sampled = []
    for i in range(len(bins)):
        num_tuples = num_tups_from_each_bin[i]
        if len_of_bins[i]:
            np.random.seed(seed)
            tmp_samples = np.random.choice(bins[i], num_tuples, replace=False)
            if len(tmp_samples):
                sampled.extend(tmp_samples)


    # retain the same order of tuples as in the input table
    key_col = '__key_col__'
    table_copy = table.copy()
    table_copy[key_col] = range(len(table))
    table_copy = table_copy.set_index(key_col)
    table_copy['_pos'] = list(range(len(table)))
    s_table = table_copy.loc[sampled]
    s_table = s_table.sort_values('_pos')

    # reset the index and drop the pos column
    s_table.reset_index(drop=False, inplace=True)
    s_table.drop(['_pos'], axis=1, inplace=True)

    return s_table

#---------------- down sample command ----------------#

def _process_tokenize_concat_strings(table, start_row_id,
                                    should_rem_puncs, should_rem_stop_words):
    """
    Function to process and tokenize the concatenated strings.
    """
    result_concat_vals = {}
    result_tokenized_vals = {}
    row_id = start_row_id
    stop_words = _get_stop_words()
    for row in table.itertuples(index=False):
        str_val = ' '.join(col_val.lower().strip() for col_val in row[:] if not
        pd.isnull(col_val))
        if should_rem_puncs:
            str_val = remove_punctuations(str_val)
        # tokenize them
        str_val = set(str_val.split())
        if should_rem_stop_words:
            str_val = str_val.difference(stop_words)
        result_tokenized_vals[row_id] = str_val
        result_concat_vals[row_id] = ' '.join(str_val)
        row_id += 1
    return result_concat_vals, result_tokenized_vals

def _down_sample(ltable, rtable, y_param, show_progress=True, verbose=False,
                 seed=None,  rem_puncs=True, rem_stop_words=True, n_ltable_chunks=-1,
                 n_rtable_chunks=-1):
    """
    Down sampling command implementation. We have reproduced the down sample command 
    because the input to the down sample command is the down sampled right table.   
    """

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

    if y_param == 0:
        logger.error(
            'y cannot be zero (3rd and 4th parameter of downsample)')
        raise AssertionError(
            'y_param cannot be zero (3rd and 4th parameter of downsample)')

    if seed is not None and not isinstance(seed, int):
        logger.error('Seed is not of type integer')
        raise AssertionError('Seed is not of type integer')


    validate_object_type(verbose, bool, 'Parameter verbose')
    validate_object_type(show_progress, bool, 'Parameter show_progress')
    validate_object_type(rem_stop_words, bool, 'Parameter rem_stop_words')
    validate_object_type(rem_puncs, bool, 'Parameter rem_puncs')
    validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
    validate_object_type(n_rtable_chunks, int, 'Parameter n_rtable_chunks')

    # rtable_sampled = sample_right_table(rtable, size)
    rtable_sampled = rtable

    ltbl_str_cols = _get_str_cols_list(ltable)
    proj_ltable = ltable[ltable.columns[ltbl_str_cols]]

    if n_ltable_chunks == -1:
        n_ltable_chunks = multiprocessing.cpu_count()

    ltable_chunks = np.array_split(proj_ltable, n_ltable_chunks)
    preprocessed_tokenized_tbl = []
    start_row_id = 0
    for i in range(len(ltable_chunks)):
        result = delayed(process_tokenize_concat_strings)(ltable_chunks[i],
                                                          start_row_id,
                                                          rem_puncs, rem_stop_words)
        preprocessed_tokenized_tbl.append(result)
        start_row_id += len(ltable_chunks[i])
    preprocessed_tokenized_tbl = delayed(wrap)(preprocessed_tokenized_tbl)
    if show_progress:
        with ProgressBar():
            logger.info('Preprocessing/tokenizing ltable')
            preprocessed_tokenized_tbl_vals = preprocessed_tokenized_tbl.compute(
                scheduler="processes", num_workers=multiprocessing.cpu_count())
    else:
        preprocessed_tokenized_tbl_vals = preprocessed_tokenized_tbl.compute(
            scheduler="processes", num_workers=multiprocessing.cpu_count())

    ltable_processed_dict = {}
    for i in range(len(preprocessed_tokenized_tbl_vals)):
        ltable_processed_dict.update(preprocessed_tokenized_tbl_vals[i])

    inverted_index = build_inverted_index(ltable_processed_dict)

    rtbl_str_cols = _get_str_cols_list(rtable_sampled)
    proj_rtable_sampled = rtable_sampled[rtable_sampled.columns[rtbl_str_cols]]


    if n_rtable_chunks == -1:
        n_rtable_chunks = multiprocessing.cpu_count()

    rtable_chunks = np.array_split(proj_rtable_sampled, n_rtable_chunks)
    probe_result = []

    for i in range(len(rtable_chunks)):
        result = delayed(probe)(rtable_chunks[i], y_param, len(proj_ltable),
                                inverted_index, rem_puncs,
                                rem_stop_words, seed)
        probe_result.append(result)

    probe_result = delayed(wrap)(probe_result)
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

def wrap(object):
    return object
