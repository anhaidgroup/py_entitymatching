# overlap coefficient join
from joblib import delayed
from joblib import Parallel
from six import iteritems
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.filter.overlap_filter import _find_candidates
from magellan.externals.py_stringsimjoin.index.inverted_index import InvertedIndex
from magellan.externals.py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_redundant_attrs, split_table, COMP_OP_MAP
from magellan.externals.py_stringsimjoin.utils.missing_value_handler import \
    get_pairs_with_missing_value
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_comp_op_for_sim_measure, validate_key_attr, validate_input_table, \
    validate_threshold, validate_tokenizer, validate_output_attrs


def overlap_coefficient_join(ltable, rtable,
                             l_key_attr, r_key_attr,
                             l_join_attr, r_join_attr,
                             tokenizer, threshold, comp_op='>=',
                             allow_missing=False,
                             l_out_attrs=None, r_out_attrs=None,
                             l_out_prefix='l_', r_out_prefix='r_',
                             out_sim_score=True, n_jobs=1, show_progress=True):
    """Join two tables using overlap coefficient.

    Finds tuple pairs from left table and right table such that the overlap coefficient between
    the join attributes satisfies the condition on input threshold. That is, if the comparison
    operator is '>=', finds tuples pairs whose overlap coefficient on the join attributes is
    greater than or equal to the input threshold.

    Args:
        ltable (dataframe): left input table.

        rtable (dataframe): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

        tokenizer (Tokenizer object): tokenizer to be used to tokenize join attributes.

        threshold (float): overlap coefficient threshold to be satisfied.

        comp_op (string): Comparison operator. Supported values are '>=', '>' and '='
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
    validate_threshold(threshold, 'OVERLAP_COEFFICIENT')

    # check if the comparison operator is valid
    validate_comp_op_for_sim_measure(comp_op, 'OVERLAP_COEFFICIENT')

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

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
        output_table = _overlap_coefficient_join_split(
                                           ltable_projected, rtable_projected,
                                           l_key_attr, r_key_attr,
                                           l_join_attr, r_join_attr,
                                           tokenizer, threshold, comp_op,
                                           l_out_attrs, r_out_attrs,
                                           l_out_prefix, r_out_prefix,
                                           out_sim_score, show_progress)
    else:
        r_splits = split_table(rtable_projected, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(_overlap_coefficient_join_split)(
                                              ltable_projected, r_splits[job_index],
                                              l_key_attr, r_key_attr,
                                              l_join_attr, r_join_attr,
                                              tokenizer, threshold, comp_op,
                                              l_out_attrs, r_out_attrs,
                                              l_out_prefix, r_out_prefix,
                                              out_sim_score,
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))
        output_table = pd.concat(results)

    if allow_missing:
        missing_pairs = get_pairs_with_missing_value(
                                            ltable_projected, rtable_projected,
                                            l_key_attr, r_key_attr,
                                            l_join_attr, r_join_attr,
                                            l_out_attrs, r_out_attrs,
                                            l_out_prefix, r_out_prefix,
                                            out_sim_score, show_progress)
        output_table = pd.concat([output_table, missing_pairs])

    output_table.insert(0, '_id', range(0, len(output_table)))

    # revert the type of join attributes to their original type, in case it
    # was converted to string type.
    if revert_l_join_attr_type:
        ltable[l_join_attr] = ltable[l_join_attr].astype(orig_l_join_attr_type)

    if revert_r_join_attr_type:
        rtable[r_join_attr] = rtable[r_join_attr].astype(orig_r_join_attr_type)

    return output_table


def _overlap_coefficient_join_split(ltable, rtable,
                                    l_key_attr, r_key_attr,
                                    l_join_attr, r_join_attr,
                                    tokenizer, threshold, comp_op,
                                    l_out_attrs, r_out_attrs,
                                    l_out_prefix, r_out_prefix,
                                    out_sim_score, show_progress):
    """Perform overlap coefficient join for a split of ltable and rtable"""
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

    # Build inverted index over ltable
    inverted_index = InvertedIndex(ltable_list, l_join_attr_index,
                                   tokenizer, cache_size_flag=True)
    inverted_index.build()

    comp_fn = COMP_OP_MAP[comp_op]

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)

    if show_progress:
        prog_bar = pyprind.ProgBar(len(rtable_list))

    for r_row in rtable_list:
        r_string = r_row[r_join_attr_index]

        r_join_attr_tokens = tokenizer.tokenize(r_string)
        r_num_tokens = len(r_join_attr_tokens)

        # probe inverted index and find overlap of candidates 
        candidate_overlap = _find_candidates(r_join_attr_tokens,
                                             inverted_index)

        for cand, overlap in iteritems(candidate_overlap):
            sim_score = (float(overlap) /
                         float(min(r_num_tokens,
                                   inverted_index.size_cache[cand])))
            if comp_fn(sim_score, threshold):
                if has_output_attributes:
                    output_row = get_output_row_from_tables(
                                     ltable_list[cand], r_row,
                                     l_key_attr_index, r_key_attr_index,
                                     l_out_attrs_indices, r_out_attrs_indices)
                else:
                    output_row = [ltable_list[cand][l_key_attr_index],
                                  r_row[r_key_attr_index]]
                if out_sim_score:
                    output_row.append(sim_score)
                output_rows.append(output_row)

        if show_progress:
            prog_bar.update()

    output_header = get_output_header_from_tables(l_key_attr, r_key_attr,
                                                  l_out_attrs, r_out_attrs,
                                                  l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")

    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table
