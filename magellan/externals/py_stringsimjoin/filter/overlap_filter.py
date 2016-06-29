# Overlap Filter

from joblib import delayed
from joblib import Parallel
from six import iteritems
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.filter.filter import Filter
from magellan.externals.py_stringsimjoin.index.inverted_index import InvertedIndex
from magellan.externals.py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_redundant_attrs, split_table, COMP_OP_MAP
from magellan.externals.py_stringsimjoin.utils.missing_value_handler import \
    get_pairs_with_missing_value
from magellan.externals.py_stringsimjoin.utils.simfunctions import overlap
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_comp_op_for_sim_measure, validate_key_attr, validate_input_table, \
    validate_threshold, validate_tokenizer, validate_output_attrs


class OverlapFilter(Filter):
    """Finds candidate matching pairs of strings using overlap filtering technique.

    A string pair is allowed by overlap filter only if the number of common tokens
    in the strings satisfy the condition on overlap size threshold. That is, if the 
    comparison operator is '>=', a string pair is allowed if the number of common tokens is
    greater than or equal to the overlap size threshold.


    Args:
        tokenizer (Tokenizer object): tokenizer to be used.
        overlap_size (int): overlap threshold to be used by the filter.
        comp_op (string): Comparison operator. Supported values are '>=', '>' and '='
                          (defaults to '>=').  

    Attributes:
        tokenizer (Tokenizer object): An attribute to store the tokenizer.
        overlap_size (int): An attribute to store the overlap threshold value.
        comp_op (string): An attribute to store the comparison operator.
    """

    def __init__(self, tokenizer, overlap_size=1, comp_op='>=',
                 allow_missing=False):
        # check if the input tokenizer is valid
        validate_tokenizer(tokenizer)

        # check if the overlap size is valid
        validate_threshold(overlap_size, 'OVERLAP')

        # check if the comparison operator is valid
        validate_comp_op_for_sim_measure(comp_op, 'OVERLAP')

        self.tokenizer = tokenizer
        self.overlap_size = overlap_size
        self.comp_op = comp_op

        super(self.__class__, self).__init__(allow_missing)

    def filter_pair(self, lstring, rstring):
        """Checks if the input strings get dropped by the overlap filter.

        Args:
            lstring,rstring (str): input strings

        Returns:
            A flag indicating whether the string pair is dropped (boolean).
        """

        # If one of the inputs is missing, then check the allow_missing flag.
        # If it is set to True, then pass the pair. Else drop the pair.
        if pd.isnull(lstring) or pd.isnull(rstring):
            return (not self.allow_missing)

        # check for empty string
        if (not lstring) or (not rstring):
            return True

        ltokens = self.tokenizer.tokenize(lstring)
        rtokens = self.tokenizer.tokenize(rstring)
 
        num_overlap = overlap(ltokens, rtokens) 

        if COMP_OP_MAP[self.comp_op](num_overlap, self.overlap_size):
            return False
        else:
            return True

    def filter_tables(self, ltable, rtable,
                      l_key_attr, r_key_attr,
                      l_filter_attr, r_filter_attr,
                      l_out_attrs=None, r_out_attrs=None,
                      l_out_prefix='l_', r_out_prefix='r_',
                      out_sim_score=False, n_jobs=1, show_progress=True):
        """Finds candidate matching pairs of strings from the input tables.

        Args:
            ltable (dataframe): left input table.

            rtable (dataframe): right input table.

            l_key_attr (string): key attribute in left table.

            r_key_attr (string): key attribute in right table.

            l_filter_attr (string): attribute to be used by the filter, in left table.

            r_filter_attr (string): attribute to be used by the filter,  in right table.

            l_out_attrs (list): list of attributes to be included in the output table from
                                left table (defaults to None).

            r_out_attrs (list): list of attributes to be included in the output table from
                                right table (defaults to None).

            l_out_prefix (string): prefix to use for the attribute names coming from left
                                   table (defaults to 'l\_').

            r_out_prefix (string): prefix to use for the attribute names coming from right
                                   table (defaults to 'r\_').

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

        # check if the key attributes and filter attributes exist
        validate_attr(l_key_attr, ltable.columns,
                      'key attribute', 'left table')
        validate_attr(r_key_attr, rtable.columns,
                      'key attribute', 'right table')
        validate_attr(l_filter_attr, ltable.columns,
                      'filter attribute', 'left table')
        validate_attr(r_filter_attr, rtable.columns,
                      'filter attribute', 'right table')

        # check if the output attributes exist
        validate_output_attrs(l_out_attrs, ltable.columns,
                              r_out_attrs, rtable.columns)

        # check if the key attributes are unique and do not contain missing values
        validate_key_attr(l_key_attr, ltable, 'left table')
        validate_key_attr(r_key_attr, rtable, 'right table')

        # convert the filter attributes to string type, in case it is int or float.
        revert_l_filter_attr_type = False
        orig_l_filter_attr_type = ltable[l_filter_attr].dtype
        if (orig_l_filter_attr_type == pd.np.int64 or
            orig_l_filter_attr_type == pd.np.float64):
            ltable[l_filter_attr] = ltable[l_filter_attr].astype(str)
            revert_l_filter_attr_type = True

        revert_r_filter_attr_type = False
        orig_r_filter_attr_type = rtable[r_filter_attr].dtype
        if (orig_r_filter_attr_type == pd.np.int64 or
            orig_r_filter_attr_type == pd.np.float64):
            rtable[r_filter_attr] = rtable[r_filter_attr].astype(str)
            revert_r_filter_attr_type = True

        # remove redundant attrs from output attrs.
        l_out_attrs = remove_redundant_attrs(l_out_attrs, l_key_attr)
        r_out_attrs = remove_redundant_attrs(r_out_attrs, r_key_attr)

        # get attributes to project.  
        l_proj_attrs = get_attrs_to_project(l_out_attrs,
                                            l_key_attr, l_filter_attr)
        r_proj_attrs = get_attrs_to_project(r_out_attrs,
                                            r_key_attr, r_filter_attr)

        # do a projection on the input dataframes. Note that this doesn't create
        # a copy of the dataframes. It only creates a view on original dataframes.
        ltable_projected = ltable[l_proj_attrs]
        rtable_projected = rtable[r_proj_attrs]

        # computes the actual number of jobs to launch.
        n_jobs = get_num_processes_to_launch(n_jobs)

        if n_jobs == 1:
            output_table = _filter_tables_split(
                                           ltable_projected, rtable_projected,
                                           l_key_attr, r_key_attr,
                                           l_filter_attr, r_filter_attr,
                                           self,
                                           l_out_attrs, r_out_attrs,
                                           l_out_prefix, r_out_prefix,
                                           out_sim_score, show_progress)
        else:
            r_splits = split_table(rtable_projected, n_jobs)
            results = Parallel(n_jobs=n_jobs)(delayed(_filter_tables_split)(
                                              ltable_projected, r_splits[job_index],
                                              l_key_attr, r_key_attr,
                                              l_filter_attr, r_filter_attr,
                                              self,
                                              l_out_attrs, r_out_attrs,
                                              l_out_prefix, r_out_prefix,
                                              out_sim_score, 
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))
            output_table = pd.concat(results)

        if self.allow_missing:
            missing_pairs = get_pairs_with_missing_value(
                                            ltable_projected, rtable_projected,
                                            l_key_attr, r_key_attr,
                                            l_filter_attr, r_filter_attr,
                                            l_out_attrs, r_out_attrs,
                                            l_out_prefix, r_out_prefix,
                                            out_sim_score, show_progress)
            output_table = pd.concat([output_table, missing_pairs])

        output_table.insert(0, '_id', range(0, len(output_table)))

        # revert the type of filter attributes to their original type, in case
        # it was converted to string type.
        if revert_l_filter_attr_type:
            ltable[l_filter_attr] = ltable[l_filter_attr].astype(
                                                        orig_l_filter_attr_type)

        if revert_r_filter_attr_type:
            rtable[r_filter_attr] = rtable[r_filter_attr].astype(
                                                        orig_r_filter_attr_type)

        return output_table


def _filter_tables_split(ltable, rtable,
                         l_key_attr, r_key_attr,
                         l_filter_attr, r_filter_attr,
                         overlap_filter,
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix,
                         out_sim_score, show_progress):
    # Find column indices of key attr, filter attr and output attrs in ltable
    l_columns = list(ltable.columns.values)
    l_key_attr_index = l_columns.index(l_key_attr)
    l_filter_attr_index = l_columns.index(l_filter_attr)
    l_out_attrs_indices = []
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs)

    # Find column indices of key attr, filter attr and output attrs in rtable
    r_columns = list(rtable.columns.values)
    r_key_attr_index = r_columns.index(r_key_attr)
    r_filter_attr_index = r_columns.index(r_filter_attr)
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs)

    # convert ltable into a list of tuples
    ltable_list = convert_dataframe_to_list(ltable, l_filter_attr_index)

    # convert rtable into a list of tuples
    rtable_list = convert_dataframe_to_list(rtable, r_filter_attr_index)

    # Build inverted index over ltable
    inverted_index = InvertedIndex(ltable_list, l_filter_attr_index,
                                   overlap_filter.tokenizer)
    inverted_index.build()

    comp_fn = COMP_OP_MAP[overlap_filter.comp_op]

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)

    if show_progress:
        prog_bar = pyprind.ProgBar(len(rtable_list))

    for r_row in rtable_list:
        r_string = r_row[r_filter_attr_index]
        r_filter_attr_tokens = overlap_filter.tokenizer.tokenize(r_string)

        # probe inverted index and find overlap of candidates          
        candidate_overlap = _find_candidates(r_filter_attr_tokens,
                                             inverted_index)

        for cand, overlap in iteritems(candidate_overlap):
            if comp_fn(overlap, overlap_filter.overlap_size):
                if has_output_attributes:
                    output_row = get_output_row_from_tables(
                                     ltable_list[cand], r_row,
                                     l_key_attr_index, r_key_attr_index, 
                                     l_out_attrs_indices, r_out_attrs_indices)
                else:
                    output_row = [ltable_list[cand][l_key_attr_index],
                                  r_row[r_key_attr_index]]

                if out_sim_score:
                    output_row.append(overlap)
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


def _find_candidates(tokens, inverted_index):
    candidate_overlap = {}
    for token in tokens:
        for cand in inverted_index.probe(token):
            candidate_overlap[cand] = candidate_overlap.get(cand, 0) + 1
    return candidate_overlap
