import math
import time

import pandas as pd
import multiprocessing
import numpy as np


import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.dask.dask_overlap_blocker import DaskOverlapBlocker
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer


def tuner_overlap_blocker(ltable, rtable, l_key, r_key, l_overlap_attr, r_overlap_attr,
                          rem_stop_words, q_val, word_level, overlap_size, ob_obj,
                          n_bins=50, sample_proportion=0.1, seed=0, repeat=1
                          ):
    """
    WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.
    
    Tunes the parameters for blocking two tables command implemented using Dask. 

    Given the input tables and the parameters for Dask-based overlap blocker command, 
    this command returns the configuration including whether the input tables need to 
    be swapped, the number of left table chunks, and the number of right table chunks. 
    It uses "Staged Tuning" approach to select the configuration setting. The key idea 
    of this approach select the configuration for one parameter at a time.
    
    Conceptually, this command performs the following steps. First, it samples the 
    left table and rtable using stratified sampling. Next, it uses the 
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
    and z indicates the number of right table partitions. 
    
    Args:        
        ltable (DataFrame): The left input table.

        rtable (DataFrame): The right input table.

        l_overlap_attr (string): The overlap attribute in left table.

        r_overlap_attr (string): The overlap attribute in right table.

        rem_stop_words (boolean): A flag to indicate whether stop words
             (e.g., a, an, the) should be removed from the token sets of the
             overlap attribute values (defaults to False).

        q_val (int): The value of q to use if the overlap attributes
             values are to be tokenized as qgrams (defaults to None).

        word_level (boolean): A flag to indicate whether the overlap
             attributes should be tokenized as words (i.e, using whitespace
             as delimiter) (defaults to True).
            
        overlap_size (int):  The minimum number of tokens that must overlap.
     
        ob_obj (OverlapBlocker): The object used to call commands to block two tables 
            and a candidate set

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
        right table partitions. 
       
    Examples:
        >>> from py_entitymatching.tuner.tuner_overlap_blocker import tuner_overlap_blocker
        >>> from py_entitymatching.dask.dask_overlap_blocker import DaskOverlapBlocker
        >>> obj = DaskOverlapBlocker()
        >>> (swap_or_not, n_ltable_chunks, n_sample_rtable_chunks) = tuner_overlap_blocker(ltable, rtable, 'id', 'id', "title", "title", rem_stop_words=True, q_val=None, word_level=True, overlap_size=1, ob_obj=obj)
        """
    logger.warning("WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
          "RISK.")

    # Select the tokenizer
    if word_level:
        tokenizer = WhitespaceTokenizer()
    else:
        tokenizer = QgramTokenizer()


    # Same the input tables, given in the original order
    sampled_tables_orig_order = get_sampled_tables(ltable, rtable, l_key, r_key,
                                                   l_overlap_attr, r_overlap_attr,
                                                   rem_stop_words, tokenizer, ob_obj,
                                                   n_bins, sample_proportion, seed)

    # Same the input tables, given in the swapped order
    sampled_tables_swap_order = get_sampled_tables(rtable, ltable, r_key, l_key,
                                                   r_overlap_attr, l_overlap_attr,
                                                   rem_stop_words, tokenizer, ob_obj,
                                                   n_bins, sample_proportion, seed)

    #  Select if the tables need to be swapped
    swap_config = should_swap(ob_obj, sampled_tables_orig_order,
                              sampled_tables_swap_order,
                               l_overlap_attr, r_overlap_attr, rem_stop_words,
                              q_val, word_level, overlap_size, repeat)
    # Use the sampled tables
    s_ltable, s_rtable = sampled_tables_orig_order
    if swap_config == True:
        s_ltable, s_rtable = sampled_tables_swap_order


    # Find the number of right table partitions
    n_rtable_chunks = find_rtable_chunks(ob_obj, s_ltable, s_rtable, l_overlap_attr,
                                         r_overlap_attr, rem_stop_words, q_val,
                                         word_level, overlap_size)

    # Find the number of left table partitions
    n_ltable_chunks = find_ltable_chunks(ob_obj, s_ltable, s_rtable, l_overlap_attr,
                                         r_overlap_attr, rem_stop_words, q_val,
                                         word_level, overlap_size, n_rtable_chunks)

    # Return the configuration
    return (swap_config, n_ltable_chunks, n_rtable_chunks)



def get_sampled_tables(ltable, rtable, l_key, r_key, l_overlap_attr, r_overlap_attr,
                       should_rem_stop_words, tokenizer, ob_obj, n_bins,
                       sample_proportion, seed):

    """
    Function to return the sampled tables.    
    """
    s_ltable = sample_ltable(ltable, l_key, l_overlap_attr, should_rem_stop_words,
                             ob_obj, n_bins, sample_proportion, seed)

    cm.set_key(s_ltable, l_key)
    result =  ob_obj.process_tokenize_block_attr(ltable[l_overlap_attr], 0,
                                                 should_rem_stop_words, tokenizer)
    inverted_index = ob_obj.build_inverted_index(result)

    s_rtable = sample_rtable(rtable, r_key, r_overlap_attr, tokenizer,
                             should_rem_stop_words, ob_obj, n_bins, sample_proportion,
                             inverted_index, seed)
    cm.set_key(s_rtable, r_key)

    return (s_ltable, s_rtable)


def execute(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr, rem_stop_words,
            q_val, word_level, overlap_size, n_ltable_chunks, n_rtable_chunks, repeat):
    """
    Function to execute the command and return the runtime. 
    """
    times = []
    for i in range(repeat):
        t1 = time.time()
        _ = ob_obj.block_tables(ltable, rtable, l_overlap_attr, r_overlap_attr,
                                rem_stop_words=rem_stop_words, q_val=q_val,
                                word_level=word_level, overlap_size=overlap_size,
                                n_ltable_chunks=n_ltable_chunks,
                                n_rtable_chunks=n_rtable_chunks, show_progress=False)
        t2 = time.time()
        times.append(t2-t1)
    return np.mean(times)



def should_swap(ob_obj, orig_order, swap_order,  l_overlap_attr, r_overlap_attr,
                rem_stop_words, q_val, word_level, overlap_size, repeat=1,
                epsilon=1):

    " Function to select if the tables need to be swapped"

    # Execute the command in the original order
    t_orig_order = execute(ob_obj, orig_order[0], orig_order[1], l_overlap_attr,
                           r_overlap_attr, rem_stop_words, q_val, word_level,
                           overlap_size, -1, -1, repeat)

    # Execute the command in the swapped order
    t_swap_order = execute(ob_obj, swap_order[0], swap_order[1], r_overlap_attr,
                           l_overlap_attr, rem_stop_words, q_val, word_level,
                           overlap_size, -1, -1, repeat)

    swap_config = False
    print(t_swap_order, t_orig_order)
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



def find_rtable_chunks(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                       rem_stop_words,
                       q_val, word_level, overlap_size, repeat=1, epsilon=1):
    """
    Function to select the number of right table partitions
    """

    chunk_range = get_chunk_range()
    hist_times = []
    hist_chunks = []
    t = execute(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                       rem_stop_words,
                       q_val, word_level, overlap_size,
                n_rtable_chunks=chunk_range[0], n_ltable_chunks=-1, repeat=repeat)
    hist_times += [t]
    hist_chunks += [chunk_range[0]]

    print('{0} chunk: {1} time: {2}'.format('Rtable', chunk_range[0], t))


    for n_chunks in chunk_range[1:]:
        t = execute(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                    rem_stop_words,
                    q_val, word_level, overlap_size,
                    n_rtable_chunks=n_chunks, n_ltable_chunks=-1, repeat=repeat)

        # print('{0} chunk: {1} time: {2}'.format('Rtable', n_chunks, t))


        if t > hist_times[-1]+epsilon:
            return hist_chunks[-1]
        else:
            hist_times += [t]
            hist_chunks += [n_chunks]
    return hist_chunks[-1]


def find_ltable_chunks(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                       rem_stop_words,
                       q_val, word_level, overlap_size, n_rtable_chunks, repeat=1,
                       epsilon=1):
    """
    Function to select the number of left table partitions
    """

    chunk_range = get_chunk_range()
    hist_times = []
    hist_chunks = []
    t = execute(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                rem_stop_words,
                q_val, word_level, overlap_size,
                n_ltable_chunks=chunk_range[0], n_rtable_chunks=n_rtable_chunks,
                repeat=repeat)
    hist_times += [t]
    hist_chunks += [chunk_range[0]]
    # print('{0} chunk: {1} time: {2}'.format('Ltable', chunk_range[0], t))


    for n_chunks in chunk_range[1:]:
        t = execute(ob_obj, ltable, rtable, l_overlap_attr, r_overlap_attr,
                    rem_stop_words,
                    q_val, word_level, overlap_size,
                    n_ltable_chunks=n_chunks, n_rtable_chunks=n_rtable_chunks, repeat=repeat)
        # print('{0} chunk: {1} time: {2}'.format('Ltable', n_chunks, t))


        if t > hist_times[-1]+epsilon:
            return hist_chunks[-1]
        else:
            hist_times += [t]
            hist_chunks += [n_chunks]
    return hist_chunks[-1]



def process_column(column, ob_obj, should_rem_stop_words):
    """
    Function to process the strings in the blocking attribute
    """
    processed_vals = []
    for s in column:
        if not s or pd.isnull(s):
            return s
        if isinstance(s, bytes):
            s = s.decode('utf-8', 'ignore')
        s = s.lower()
        s = ob_obj.regex_punctuation.sub('', s)
        tokens = list(set(s.strip().split()))
        if should_rem_stop_words:
            tokens = [token for token in tokens if token not in ob_obj.stop_words]
        s = ' '.join(tokens)
        processed_vals.append(s)
    return processed_vals


def sample_ltable(table, key, block_attr, should_rem_stop_words, ob_obj, n_bins,
                  sample_proportion,
                  seed):
    """
    Function to sample the left table.
    """
    df = table[[key, block_attr]]
    # remove NaNs
    df = df[~df[block_attr].isnull()]

    df[block_attr] = process_column(df[block_attr], ob_obj, should_rem_stop_words)

    # get the number of samples to be selected from each bin
    num_samples = int(math.floor(sample_proportion*len(table)))

    # get the string lengths
    df['_str_len_'] = df[block_attr].str.len()
    len_groups = df.groupby('_str_len_')

    group_ids_len = {}
    for group_id, group in len_groups:
        group_ids_len[group_id] = list(group[key])

    str_lens = list(df['_str_len_'].values)
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
    table_copy = table.set_index(key)
    table_copy['_pos'] = list(range(len(table)))
    s_table = table_copy.loc[sampled]
    s_table = s_table.sort_values('_pos')

    # reset the index and drop the pos column
    s_table.reset_index(drop=False, inplace=True)
    s_table.drop(['_pos'], axis=1, inplace=True)

    return s_table


def sample_rtable(table, key, overlap_attr, tokenizer,
                             should_rem_stop_words, ob_obj, n_bins, sample_proportion,
                             inverted_index, seed):
    table = table[[key, overlap_attr]]
    processed_tokenized_vals = ob_obj.process_tokenize_block_attr(table[overlap_attr], 0,
                                           should_rem_stop_words, tokenizer)
    token_cnt = {}
    token_map = {}

    for tid in processed_tokenized_vals.keys():
        tokens = processed_tokenized_vals[tid]
        cnt = 0
        for token in tokens:
            if token not in token_map:
                token_map[token] = len(inverted_index.get(token, []))
            cnt += token_map[token]
        token_cnt[tid] = cnt

    cnt_df = pd.DataFrame.from_dict(token_cnt, orient='index', columns=['count'])
    cnt_df.insert(0, key, cnt_df.index.values)
    cnt_df.reset_index(drop=False, inplace=True)

    cnt_groups = cnt_df.groupby('count')
    cnt_ids = {}

    for group_id, group in cnt_groups:
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

    table_copy = table.copy()
    table_copy['_pos'] = list(range(len(table)))
    s_table = table_copy.loc[sampled]
    s_table = s_table.sort_values('_pos')

    # reset the index and drop the pos column
    s_table.reset_index(drop=False, inplace=True)
    s_table.drop(['_pos'], axis=1, inplace=True)

    # print(len(s_table))

    return s_table

