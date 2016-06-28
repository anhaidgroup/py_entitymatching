
import operator
import types

from joblib import delayed, Parallel
from six.moves import copyreg
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.utils.helper_functions import build_dict_from_table, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_redundant_attrs, split_table, COMP_OP_MAP
from magellan.externals.py_stringsimjoin.utils.pickle import pickle_instance_method, \
                                          unpickle_instance_method
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_comp_op, validate_key_attr, validate_input_table, \
    validate_tokenizer, validate_output_attrs


# Register pickle and unpickle methods for handling instance methods.
# This is because joblib doesn't pickle instance methods, by default.
# Hence, if the sim_function supplied to apply_matcher is an instance
# method, it will result in an error. To avoid this, we register custom
# functions to pickle and unpickle instance methods. 
copyreg.pickle(types.MethodType,
               pickle_instance_method,
               unpickle_instance_method)


def apply_matcher(candset,
                  candset_l_key_attr, candset_r_key_attr,
                  ltable, rtable,
                  l_key_attr, r_key_attr,
                  l_join_attr, r_join_attr,
                  tokenizer, sim_function,
                  threshold, comp_op='>=',
                  allow_missing=False,
                  l_out_attrs=None, r_out_attrs=None,
                  l_out_prefix='l_', r_out_prefix='r_',
                  out_sim_score=True, n_jobs = 1, show_progress=True):
    """Find matching string pairs from the candidate set by applying a matcher of form (sim_function comp_op threshold).

    Specifically, this method computes the input similarity function on string pairs in the candidate set
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

        comp_op (string): Comparison operator. Supported values are '>=', '>', '<=', '<', '=' and '!='
                          (defaults to '>=').

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

        show_progress (boolean): flag to indicate if task progress need to be shown (defaults to True).

    Returns:
        output table (dataframe)
    """

    # check if the input candset is a dataframe
    validate_input_table(candset, 'candset')

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

    # check if the comparison operator is valid
    validate_comp_op(comp_op)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

    # check for empty candset
    if candset.empty:
        return candset

    # convert the join attributes to string type, in case it is int or float.
    revert_l_join_attr_type = False
    orig_l_join_attr_type = ltable[l_join_attr].dtype
    if (orig_l_join_attr_type == pd.np.int64 or
        orig_l_join_attr_type == pd.np.float64):
        ltable[l_join_attr] = ltable[l_join_attr].astype(str)
        revert_l_join_attr_type = True

    revert_r_join_attr_type = False
    orig_r_join_attr_type = rtable[r_join_attr].dtype
    if (orig_r_join_attr_type == pd.np.int64 or
        orig_r_join_attr_type == pd.np.float64):
        rtable[r_join_attr] = rtable[r_join_attr].astype(str)
        revert_r_join_attr_type = True

    # remove redundant attrs from output attrs.
    l_out_attrs = remove_redundant_attrs(l_out_attrs, l_key_attr)
    r_out_attrs = remove_redundant_attrs(r_out_attrs, r_key_attr)

    # get attributes to project.  
    l_proj_attrs = get_attrs_to_project(l_out_attrs, l_key_attr, l_join_attr)
    r_proj_attrs = get_attrs_to_project(r_out_attrs, r_key_attr, r_join_attr)

    # do a projection on the input dataframes. Note that this doesn't create a copy
    # of the dataframes. It only creates a view on original dataframes.
    ltable_projected = ltable[l_proj_attrs]
    rtable_projected = rtable[r_proj_attrs]

    # computes the actual number of jobs to launch.
    n_jobs = get_num_processes_to_launch(n_jobs)

    if n_jobs == 1:
        output_table =  _apply_matcher_split(candset,
                                    candset_l_key_attr, candset_r_key_attr,
                                    ltable_projected, rtable_projected,
                                    l_key_attr, r_key_attr,
                                    l_join_attr, r_join_attr,
                                    tokenizer, sim_function,
                                    threshold, comp_op, allow_missing,
                                    l_out_attrs, r_out_attrs,
                                    l_out_prefix, r_out_prefix,
                                    out_sim_score, show_progress)
    else:
        candset_splits = split_table(candset, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(_apply_matcher_split)(
                                      candset_splits[job_index],
                                      candset_l_key_attr, candset_r_key_attr,
                                      ltable_projected, rtable_projected,
                                      l_key_attr, r_key_attr,
                                      l_join_attr, r_join_attr,
                                      tokenizer, sim_function,
                                      threshold, comp_op, allow_missing,
                                      l_out_attrs, r_out_attrs,
                                      l_out_prefix, r_out_prefix,
                                      out_sim_score,
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))
        output_table =  pd.concat(results)

    # revert the type of join attributes to their original type, in case it
    # was converted to string type.
    if revert_l_join_attr_type:
        ltable[l_join_attr] = ltable[l_join_attr].astype(orig_l_join_attr_type)

    if revert_r_join_attr_type:
        rtable[r_join_attr] = rtable[r_join_attr].astype(orig_r_join_attr_type)

    return output_table


def _apply_matcher_split(candset,
                         candset_l_key_attr, candset_r_key_attr,
                         ltable, rtable,
                         l_key_attr, r_key_attr,
                         l_join_attr, r_join_attr,
                         tokenizer, sim_function,
                         threshold, comp_op, allow_missing,
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix,
                         out_sim_score, show_progress):
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
                                        l_join_attr_index, remove_null=False)

    # Build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_join_attr_index, remove_null=False)

    # Find indices of l_key_attr and r_key_attr in candset
    candset_columns = list(candset.columns.values)
    candset_l_key_attr_index = candset_columns.index(candset_l_key_attr)
    candset_r_key_attr_index = candset_columns.index(candset_r_key_attr)

    comp_fn = COMP_OP_MAP[comp_op]
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None) 

    output_rows = []

    if show_progress:
        prog_bar = pyprind.ProgBar(len(candset))

    for candset_row in candset.itertuples(index = False):
        l_id = candset_row[candset_l_key_attr_index]
        r_id = candset_row[candset_r_key_attr_index]

        l_row = ltable_dict[l_id]
        r_row = rtable_dict[r_id]
        
        l_apply_col_value = l_row[l_join_attr_index]
        r_apply_col_value = r_row[r_join_attr_index]

        allow_pair = False
        # Check if one of the inputs is missing. If yes, check the allow_missing
        # flag. If it is True, then add the pair to output. Else, continue.
        # If none of the input is missing, then proceed to apply the sim_function. 
        if pd.isnull(l_apply_col_value) or pd.isnull(r_apply_col_value):
            if allow_missing:
                allow_pair = True
                sim_score = pd.np.NaN
            else:
                continue   
        else:
            if tokenizer is not None:
                l_apply_col_value = tokenizer.tokenize(l_apply_col_value)
                r_apply_col_value = tokenizer.tokenize(r_apply_col_value)       
        
            sim_score = sim_function(l_apply_col_value, r_apply_col_value)
            allow_pair = comp_fn(sim_score, threshold)

        if allow_pair: 
            if has_output_attributes:
                output_row = get_output_row_from_tables(
                                 l_row, r_row,
                                 l_key_attr_index, r_key_attr_index,
                                 l_out_attrs_indices,
                                 r_out_attrs_indices)
                output_row.insert(0, candset_row[0])
            else:
                output_row = [candset_row[0], l_id, r_id]
            if out_sim_score:
                output_row.append(sim_score)
            output_rows.append(output_row)

        if show_progress:                    
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
