"""Candidate set utilities"""

import operator

from joblib import delayed
from joblib import Parallel
import pandas as pd
import pyprind

from py_stringsimjoin.utils.helper_functions import build_dict_from_table, \
    find_output_attribute_indices, get_output_header_from_tables, \
    get_output_row_from_tables, split_table
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_tokenizer, \
    validate_output_attrs


def apply_candset(candset,
                  candset_l_key_attr, candset_r_key_attr,
                  ltable, rtable,
                  l_key_attr, r_key_attr,
                  l_join_attr, r_join_attr,
                  tokenizer, sim_function, threshold,
                  comparison=operator.ge,
                  l_out_attrs=None, r_out_attrs=None,
                  l_out_prefix='l_', r_out_prefix='r_',
                  out_sim_score=True, n_jobs = 1):
    """Computes similarity measure on string pairs in candidate set to find matching pairs.

    Specifically, computes the input similarity function on string pairs in the candidate set
    and checks if the score satisfies the input threshold (depending on the comparison operator).

    Args:
        candset (dataframe): input candidate set.

        candset_l_key_attr (string): attribute in candidate set that is a key in left table.

        candset_r_key_attr (string): attribute in candidate set that is a key in right table.

        ltable (dataframe): left input table.

        rtable (dataframe): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table, on which similarity function is computed.

        r_join_attr (string): join attribute in right table, on which similarity function is computed.

        tokenizer (Tokenizer object): tokenizer to be used to tokenize join attributes.

        sim_function (function): similarity function to be computed.

        threshold (float): similarity threshold to be satisfied.

        comparison (function): comparison function to be used (defaults to greater than or equal to).

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

    Returns:
        output table (dataframe)
    """

    # check if the input candset is a dataframe
    validate_input_table(ltable, 'candset')

    # check if the candset key attributes exist
    validate_attr(candset_l_key_attr, candset.columns,
                  'left key attribute', 'candset')
    validate_attr(candset_r_key_attr, candset.columns,
                  'right key attribute', 'candset')

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

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the input tokenizer is valid
    validate_tokenizer(tokenizer)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

    # check for empty candset
    if candset.empty:
        return candset

    if n_jobs == 1:
        return _apply_candset_split(candset,
                                        candset_l_key_attr, candset_r_key_attr,
                                        ltable, rtable,
                                        l_key_attr, r_key_attr,
                                        l_join_attr, r_join_attr,
                                        tokenizer, sim_function, threshold,
                                        comparison,
                                        l_out_attrs, r_out_attrs,
                                        l_out_prefix, r_out_prefix,
                                        out_sim_score)
    else:
        candset_splits = split_table(candset, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(_apply_candset_split)(
                                      candset_split,
                                      candset_l_key_attr, candset_r_key_attr,
                                      ltable, rtable,
                                      l_key_attr, r_key_attr,
                                      l_join_attr, r_join_attr,
                                      tokenizer, sim_function, threshold,
                                      comparison,
                                      l_out_attrs, r_out_attrs,
                                      l_out_prefix, r_out_prefix,
                                      out_sim_score)
                                  for candset_split in candset_splits)
        return pd.concat(results)


def _apply_candset_split(candset,
                         candset_l_key_attr, candset_r_key_attr,
                         ltable, rtable,
                         l_key_attr, r_key_attr,
                         l_join_attr, r_join_attr,
                         tokenizer, sim_function, threshold,
                         comparison,
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix,
                         out_sim_score):
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

    # Build a dictionary on ltable
    ltable_dict = build_dict_from_table(ltable, l_key_attr_index,
                                        l_join_attr_index)

    # Build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_join_attr_index)

    # Find indices of l_key_attr and r_key_attr in candset
    candset_columns = list(candset.columns.values)
    candset_l_key_attr_index = candset_columns.index(candset_l_key_attr)
    candset_r_key_attr_index = candset_columns.index(candset_r_key_attr)

    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None) 

    output_rows = []
    prog_bar = pyprind.ProgBar(len(candset))
    for candset_row in candset.itertuples(index = False):
        l_id = candset_row[candset_l_key_attr_index]
        r_id = candset_row[candset_r_key_attr_index]

        l_row = ltable_dict[l_id]
        r_row = rtable_dict[r_id]
        
        l_apply_col_value = str(l_row[l_join_attr_index])
        r_apply_col_value = str(r_row[r_join_attr_index])  
        if tokenizer is not None:
            l_apply_col_value = tokenizer.tokenize(l_apply_col_value)
            r_apply_col_value = tokenizer.tokenize(r_apply_col_value)       
        
        sim_score = sim_function(l_apply_col_value, r_apply_col_value)

        if comparison(sim_score, threshold):
            if has_output_attributes:
                output_row = get_output_row_from_tables(
                                             l_row, r_row,
                                             l_id, r_id,
                                             l_out_attrs_indices,
                                             r_out_attrs_indices)
                output_row.insert(0, candset_row[0])
                if out_sim_score:
                    output_row.append(sim_score)
                output_rows.append(output_row)
            else:
                output_row = [candset_row[0], l_id, r_id]
                if out_sim_score:
                    output_row.append(sim_score)
                output_rows.append(output_row)
                    
        prog_bar.update()

    output_header = get_output_header_from_tables(
                        l_key_attr, r_key_attr,
                        l_out_attrs, r_out_attrs,
                        l_out_prefix, r_out_prefix)
    output_header.insert(0, '_id')
    if out_sim_score:
        output_header.append("_sim_score")

    # generate a dataframe from the list of output rows
    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table
