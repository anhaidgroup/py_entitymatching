# edit distance join
from math import floor

from joblib import delayed
from joblib import Parallel
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
import pandas as pd
import pyprind

from py_stringsimjoin.filter.prefix_filter import PrefixFilter, _find_candidates
from py_stringsimjoin.index.prefix_index import PrefixIndex
from py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list, \
    find_output_attribute_indices, get_output_header_from_tables, \
    get_output_row_from_tables, split_table
from py_stringsimjoin.utils.simfunctions import get_sim_function
from py_stringsimjoin.utils.token_ordering import \
    gen_token_ordering_for_tables, order_using_token_ordering
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs


def edit_distance_join(ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_join_attr, r_join_attr,
                       threshold,
                       l_out_attrs=None, r_out_attrs=None,
                       l_out_prefix='l_', r_out_prefix='r_',
                       out_sim_score=True, n_jobs=1,
                       tokenizer=QgramTokenizer(qval=2)):
    """Join two tables using edit distance measure.

    Finds tuple pairs from left table and right table such that the edit distance between
    the join attributes is less than or equal to the input threshold.

    Note:
        Currently, this method only computes an approximate join result. This is because, to perform the join we
        transform a edit distance measure between strings into an overlap measure between qgrams of the
        strings. Hence, we need atleast one qgram to be in common between two input strings, to appear in the join
        output. For smaller strings, where all qgrams of the strings differ, we cannot process them.
        
        We plan to make this method compute exact join result, in the next release.

    Args:
        ltable (dataframe): left input table.

        rtable (dataframe): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

        threshold (float): edit distance threshold to be satisfied.

        l_out_attrs (list): list of attributes to be included in the output table from
                            left table (defaults to None).

        r_out_attrs (list): list of attributes to be included in the output table from
                            right table (defaults to None).

        l_out_prefix (string): prefix to use for the attribute names coming from left
                               table (defaults to 'l\_').

        r_out_prefix (string): prefix to use for the attribute names coming from right
                               table (defaults to 'r\_').

        out_sim_score (boolean): flag to indicate if similarity score needs to be
                                 included in the output table (defaults to True).

        n_jobs (int): The number of jobs to use for the computation (defaults to 1).                                                                                            
            If -1 all CPUs are used. If 1 is given, no parallel computing code is used at all, 
            which is useful for debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used. 
            Thus for n_jobs = -2, all CPUs but one are used. If (n_cpus + 1 + n_jobs) becomes less than 1,
            then n_jobs is set to 1.
        
        tokenizer (Tokenizer object): tokenizer to be used for filtering, when edit distance
                                      measure is transformed into an overlap measure. This must be
                                      a q-gram tokenizer (defaults to 2-gram tokenizer).

    Returns:
        output table (dataframe)
    """

    # check if the input tables are dataframes
    validate_input_table(ltable, 'left table')
    validate_input_table(rtable, 'right table')

    # check if the key attributes and join attributes exist
    validate_attr(l_key_attr, ltable.columns,
                  'key attribute', 'left table')
    validate_attr(r_key_attr, rtable.columns,
                  'key attribute', 'right table')
    validate_attr(l_join_attr, ltable.columns,
                  'join attribute', 'left table')
    validate_attr(r_join_attr, rtable.columns,
                  'join attribute', 'right table')

    # check if the input tokenizer is valid
    validate_tokenizer(tokenizer)

    # check if the input threshold is valid
    validate_threshold(threshold, 'EDIT_DISTANCE')

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

    # convert threshold to integer (incase if it is float)
    threshold = int(floor(threshold))

    if n_jobs == 1:
        output_table = _edit_distance_join_split(ltable, rtable,
                               l_key_attr, r_key_attr,
                               l_join_attr, r_join_attr,
                               tokenizer,
                               threshold,
                               l_out_attrs, r_out_attrs,
                               l_out_prefix, r_out_prefix,
                               out_sim_score)
        output_table.insert(0, '_id', range(0, len(output_table)))
        return output_table
    else:
        r_splits = split_table(rtable, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(_edit_distance_join_split)(
                                             ltable, s,
                                             l_key_attr, r_key_attr,
                                             l_join_attr, r_join_attr,
                                             tokenizer,
                                             threshold,
                                             l_out_attrs, r_out_attrs,
                                             l_out_prefix, r_out_prefix,
                                             out_sim_score) for s in r_splits)
        output_table = pd.concat(results)
        output_table.insert(0, '_id', range(0, len(output_table)))
        return output_table


def _edit_distance_join_split(ltable, rtable,
                              l_key_attr, r_key_attr,
                              l_join_attr, r_join_attr,
                              tokenizer,
                              threshold,
                              l_out_attrs, r_out_attrs,
                              l_out_prefix, r_out_prefix,
                              out_sim_score):
    """Perform edit distance join for a split of ltable and rtable"""
    # find column indices of key attr, join attr and output attrs in ltable
    l_columns = list(ltable.columns.values)
    l_key_attr_index = l_columns.index(l_key_attr)
    l_join_attr_index = l_columns.index(l_join_attr)
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs)

    # find column indices of key attr, join attr and output attrs in rtable
    r_columns = list(rtable.columns.values)
    r_key_attr_index = r_columns.index(r_key_attr)
    r_join_attr_index = r_columns.index(r_join_attr)
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs)

    # convert ltable into a list of tuples
    ltable_list = convert_dataframe_to_list(ltable, l_join_attr_index)

    # convert rtable into a list of tuples
    rtable_list = convert_dataframe_to_list(rtable, r_join_attr_index)

    sim_measure_type = 'EDIT_DISTANCE'
    # generate token ordering using tokens in l_join_attr
    # and r_join_attr
    token_ordering = gen_token_ordering_for_tables(
                         [ltable_list, rtable_list],
                         [l_join_attr_index, r_join_attr_index],
                         tokenizer, sim_measure_type)

    # cache l_join_attr lengths
    l_join_attr_list = []
    for row in ltable_list:
        l_join_attr_list.append(len(str(row[l_join_attr_index])))

    # Build prefix index on l_join_attr
    prefix_index = PrefixIndex(ltable_list, l_join_attr_index,
                               tokenizer, sim_measure_type, threshold,
                               token_ordering)
    prefix_index.build()

    prefix_filter = PrefixFilter(tokenizer, sim_measure_type, threshold)
    sim_fn = get_sim_function(sim_measure_type)
    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    prog_bar = pyprind.ProgBar(len(rtable))

    for r_row in rtable_list:
        r_string = str(r_row[r_join_attr_index])
        r_len = len(r_string)

        r_ordered_tokens = order_using_token_ordering(
                tokenizer.tokenize(r_string), token_ordering)

        candidates = _find_candidates(r_ordered_tokens, len(r_ordered_tokens),
                                      prefix_filter, prefix_index)
        for cand in candidates:
            if r_len - threshold <= l_join_attr_list[cand] <= r_len + threshold:
                l_row = ltable_list[cand]
                edit_dist = sim_fn(str(l_row[l_join_attr_index]), r_string)
                if edit_dist <= threshold:
                    if has_output_attributes:
                        output_row = get_output_row_from_tables(
                                         l_row, r_row,
                                         l_key_attr_index, r_key_attr_index,
                                         l_out_attrs_indices,
                                         r_out_attrs_indices)
                    else:
                        output_row = [l_row[l_key_attr_index],
                                      r_row[r_key_attr_index]]

                    if out_sim_score:
                        output_row.append(edit_dist)
                    output_rows.append(output_row)

        prog_bar.update()

    output_header = get_output_header_from_tables(
                        l_key_attr, r_key_attr,
                        l_out_attrs, r_out_attrs,
                        l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")

    # generate a dataframe from the list of output rows
    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table
