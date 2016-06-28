# Size Filter

from joblib import delayed
from joblib import Parallel
from six.moves import xrange
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.filter.filter import Filter
from magellan.externals.py_stringsimjoin.filter.filter_utils import get_size_lower_bound
from magellan.externals.py_stringsimjoin.filter.filter_utils import get_size_upper_bound
from magellan.externals.py_stringsimjoin.index.size_index import SizeIndex
from magellan.externals.py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list, \
    find_output_attribute_indices, get_attrs_to_project, \
    get_num_processes_to_launch, get_output_header_from_tables, \
    get_output_row_from_tables, remove_redundant_attrs, split_table
from magellan.externals.py_stringsimjoin.utils.missing_value_handler import \
    get_pairs_with_missing_value
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs, validate_sim_measure_type


class SizeFilter(Filter):
    """Finds candidate matching pairs of strings using size filtering technique.

    For similarity measures such as cosine, Dice, Jaccard and overlap, the filter finds candidate
    string pairs that may have similarity score greater than or equal to the input threshold.
    Where as for distance measure such as edit distance, the filter finds candidate string pairs 
    that may have distance score less than or equal to the threshold.

    To know about size filtering, refer the `string matching chapter <http://pages.cs.wisc.edu/~anhai/py_stringmatching/dibook-string-matching.pdf>`_ 
    of the "Principles of Data Integration" book.

    Args:
        tokenizer (Tokenizer object): tokenizer to be used.
        sim_measure_type (str): similarity measure type. Supported types are 'COSINE',
                                'DICE', 'EDIT_DISTANCE', 'JACCARD' and 'OVERLAP'.
        threshold (float): threshold to be used by the filter.

    Attributes:
        tokenizer (Tokenizer object): An attribute to store the tokenizer.
        sim_measure_type (str): An attribute to store the similarity measure type.
        threshold (float): An attribute to store the threshold value.
    """

    def __init__(self, tokenizer, sim_measure_type, threshold,
                 allow_missing=False):
        # check if the input tokenizer is valid
        validate_tokenizer(tokenizer)

        # check if the sim_measure_type is valid
        validate_sim_measure_type(sim_measure_type)

        # check if the threshold is valid
        validate_threshold(threshold, sim_measure_type)

        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        super(self.__class__, self).__init__(allow_missing)

    def filter_pair(self, lstring, rstring):
        """Checks if the input strings get dropped by the size filter.

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

        l_num_tokens = len(self.tokenizer.tokenize(lstring))
        r_num_tokens = len(self.tokenizer.tokenize(rstring))

        size_lower_bound = get_size_lower_bound(l_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold)
        size_upper_bound = get_size_upper_bound(l_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold)

        if size_lower_bound <= r_num_tokens <= size_upper_bound:
            return False
        else:
            return True

    def filter_tables(self, ltable, rtable,
                      l_key_attr, r_key_attr,
                      l_filter_attr, r_filter_attr,
                      l_out_attrs=None, r_out_attrs=None,
                      l_out_prefix='l_', r_out_prefix='r_',
                      n_jobs=1, show_progress=True):
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
                                           show_progress)
        else:
            r_splits = split_table(rtable_projected, n_jobs)
            results = Parallel(n_jobs=n_jobs)(delayed(_filter_tables_split)(
                                              ltable_projected, r_splits[job_index],
                                              l_key_attr, r_key_attr,
                                              l_filter_attr, r_filter_attr,
                                              self,
                                              l_out_attrs, r_out_attrs,
                                              l_out_prefix, r_out_prefix,
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
                                            False, show_progress)
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
                         size_filter, 
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix, show_progress):
    # find column indices of key attr, filter attr and output attrs in ltable
    l_columns = list(ltable.columns.values)
    l_key_attr_index = l_columns.index(l_key_attr)
    l_filter_attr_index = l_columns.index(l_filter_attr)
    l_out_attrs_indices = []
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs)

    # find column indices of key attr, filter attr and output attrs in rtable
    r_columns = list(rtable.columns.values)
    r_key_attr_index = r_columns.index(r_key_attr)
    r_filter_attr_index = r_columns.index(r_filter_attr)
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs)

    # convert ltable into a list of tuples
    ltable_list = convert_dataframe_to_list(ltable, l_filter_attr_index)

    # convert rtable into a list of tuples
    rtable_list = convert_dataframe_to_list(rtable, r_filter_attr_index)

    # Build size index over ltable
    size_index = SizeIndex(ltable_list, l_filter_attr_index,
                           size_filter.tokenizer)
    size_index.build()

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)

    if show_progress:
        prog_bar = pyprind.ProgBar(len(rtable_list))

    for r_row in rtable_list:
        r_string = r_row[r_filter_attr_index]

        r_num_tokens = len(size_filter.tokenizer.tokenize(r_string))
           
        size_lower_bound = get_size_lower_bound(r_num_tokens,
                                                size_filter.sim_measure_type,
                                                size_filter.threshold)
        size_upper_bound = get_size_upper_bound(r_num_tokens,
                                                size_filter.sim_measure_type,
                                                size_filter.threshold)

        size_lower_bound = (size_index.min_length if
                            size_lower_bound < size_index.min_length else 
                            size_lower_bound) 

        size_upper_bound = (size_index.max_length if
                            size_upper_bound > size_index.max_length else 
                            size_upper_bound) 

        # probe size index and find candidates
        candidates = _find_candidates(size_lower_bound, size_upper_bound,
                                      size_index)

        for cand in candidates:
            if has_output_attributes:
                output_row = get_output_row_from_tables(
                                     ltable_list[cand], r_row,
                                     l_key_attr_index, r_key_attr_index, 
                                     l_out_attrs_indices, r_out_attrs_indices)
            else:
                output_row = [ltable_list[cand][l_key_attr_index],
                              r_row[r_key_attr_index]]

            output_rows.append(output_row)

        if show_progress:
            prog_bar.update()

    output_header = get_output_header_from_tables(l_key_attr, r_key_attr,
                                                  l_out_attrs, r_out_attrs, 
                                                  l_out_prefix, r_out_prefix)

    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table

def _find_candidates(size_lower_bound, size_upper_bound, size_index):
    candidates = set()
    for cand_size in xrange(size_lower_bound, size_upper_bound + 1):
        for cand in size_index.probe(cand_size):
            candidates.add(cand)
    return candidates
