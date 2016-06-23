# Prefix Filter

from joblib import delayed
from joblib import Parallel
import pandas as pd
import pyprind

from py_stringsimjoin.filter.filter import Filter
from py_stringsimjoin.filter.filter_utils import get_prefix_length
from py_stringsimjoin.index.prefix_index import PrefixIndex
from py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list
from py_stringsimjoin.utils.helper_functions import \
                                                 find_output_attribute_indices
from py_stringsimjoin.utils.helper_functions import \
                                                 get_output_header_from_tables
from py_stringsimjoin.utils.helper_functions import get_output_row_from_tables
from py_stringsimjoin.utils.helper_functions import split_table
from py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_lists
from py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_tables
from py_stringsimjoin.utils.token_ordering import order_using_token_ordering
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs, validate_sim_measure_type


class PrefixFilter(Filter):
    """Finds candidate matching pairs of strings using prefix filtering technique.

    For similarity measures such as cosine, Dice, Jaccard and overlap, the filter finds candidate
    string pairs that may have similarity score greater than or equal to the input threshold.
    Where as for distance measure such as edit distance, the filter finds candidate string pairs 
    that may have distance score less than or equal to the threshold.

    To know about prefix filtering, refer the `string matching chapter <http://pages.cs.wisc.edu/~anhai/py_stringmatching/dibook-string-matching.pdf>`_ 
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

    def __init__(self, tokenizer, sim_measure_type, threshold):
        # check if the input tokenizer is valid
        validate_tokenizer(tokenizer)

        # check if the sim_measure_type is valid
        validate_sim_measure_type(sim_measure_type)

        # check if the threshold is valid
        validate_threshold(threshold, sim_measure_type)

        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        super(self.__class__, self).__init__()

    def filter_pair(self, lstring, rstring):
        """Checks if the input strings get dropped by the prefix filter.

        Args:
            lstring,rstring (str): input strings

        Returns:
            A flag indicating whether the string pair is dropped (boolean).
        """

        # check for empty string
        if (not lstring) or (not rstring):
            return True

        ltokens = self.tokenizer.tokenize(lstring)
        rtokens = self.tokenizer.tokenize(rstring)

        token_ordering = gen_token_ordering_for_lists([ltokens, rtokens])
        ordered_ltokens = order_using_token_ordering(ltokens, token_ordering)
        ordered_rtokens = order_using_token_ordering(rtokens, token_ordering)

        l_prefix_length = get_prefix_length(len(ordered_ltokens),
                                            self.sim_measure_type,
                                            self.threshold,
                                            self.tokenizer) 
        r_prefix_length = get_prefix_length(len(ordered_rtokens),
                                            self.sim_measure_type,
                                            self.threshold,
                                            self.tokenizer)
        prefix_overlap = set(ordered_ltokens[0:l_prefix_length]).intersection(
                         set(ordered_rtokens[0:r_prefix_length]))

        if len(prefix_overlap) > 0:
            return False
        else:
            return True

    def filter_tables(self, ltable, rtable,
                      l_key_attr, r_key_attr,
                      l_filter_attr, r_filter_attr,
                      l_out_attrs=None, r_out_attrs=None,
                      l_out_prefix='l_', r_out_prefix='r_',
                      n_jobs=1):
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

        if n_jobs == 1:
            output_table = _filter_tables_split(ltable, rtable,
                                                l_key_attr, r_key_attr,
                                                l_filter_attr, r_filter_attr,
                                                self,
                                                l_out_attrs, r_out_attrs,
                                                l_out_prefix, r_out_prefix)
            output_table.insert(0, '_id', range(0, len(output_table)))
            return output_table
        else:
            rtable_splits = split_table(rtable, n_jobs)
            results = Parallel(n_jobs=n_jobs)(delayed(_filter_tables_split)(
                                                  ltable, rtable_split,
                                                  l_key_attr, r_key_attr,
                                                  l_filter_attr, r_filter_attr,
                                                  self,
                                                  l_out_attrs, r_out_attrs,
                                                  l_out_prefix, r_out_prefix)
                                              for rtable_split in rtable_splits)
            output_table = pd.concat(results)
            output_table.insert(0, '_id', range(0, len(output_table)))
            return output_table


def _filter_tables_split(ltable, rtable,
                         l_key_attr, r_key_attr,
                         l_filter_attr, r_filter_attr,
                         prefix_filter,
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix):
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

    # generate token ordering using tokens in l_filter_attr and r_filter_attr
    token_ordering = gen_token_ordering_for_tables(
                                 [ltable_list, rtable_list],
                                 [l_filter_attr_index, r_filter_attr_index],
                                 prefix_filter.tokenizer,
                                 prefix_filter.sim_measure_type)

    # Build prefix index on l_filter_attr
    prefix_index = PrefixIndex(ltable_list, l_filter_attr_index, 
                       prefix_filter.tokenizer, prefix_filter.sim_measure_type,
                       prefix_filter.threshold, token_ordering)
    prefix_index.build()

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    prog_bar = pyprind.ProgBar(len(rtable))

    for r_row in rtable_list:
        r_string = str(r_row[r_filter_attr_index])

        r_filter_attr_tokens = prefix_filter.tokenizer.tokenize(r_string)
        r_ordered_tokens = order_using_token_ordering(r_filter_attr_tokens,
                                                      token_ordering)
           
        # probe prefix index and find candidates
        candidates = _find_candidates(r_ordered_tokens, len(r_ordered_tokens),
                                      prefix_filter, prefix_index)

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
 
        prog_bar.update()

    output_header = get_output_header_from_tables(l_key_attr, r_key_attr,
                                                  l_out_attrs, r_out_attrs, 
                                                  l_out_prefix, r_out_prefix)

    # generate a dataframe from the list of output rows
    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table


def _find_candidates(tokens, num_tokens, prefix_filter, prefix_index):
    prefix_length = get_prefix_length(num_tokens,
                                      prefix_filter.sim_measure_type,
                                      prefix_filter.threshold,
                                      prefix_filter.tokenizer)
    candidates = set()
    for token in tokens[0:prefix_length]:
        for cand in prefix_index.probe(token):
            candidates.add(cand)
    return candidates
