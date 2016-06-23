# Overlap Filter

from joblib import delayed
from joblib import Parallel
from six import iteritems
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.filter.filter import Filter
from magellan.externals.py_stringsimjoin.index.inverted_index import InvertedIndex
from magellan.externals.py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list
from magellan.externals.py_stringsimjoin.utils.helper_functions import \
                                                 find_output_attribute_indices
from magellan.externals.py_stringsimjoin.utils.helper_functions import \
                                                 get_output_header_from_tables
from magellan.externals.py_stringsimjoin.utils.helper_functions import get_output_row_from_tables
from magellan.externals.py_stringsimjoin.utils.helper_functions import split_table
from magellan.externals.py_stringsimjoin.utils.simfunctions import overlap
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs


class OverlapFilter(Filter):
    """Finds candidate matching pairs of strings using overlap filtering technique.

    Args:
        tokenizer (Tokenizer object): tokenizer to be used.
        overlap_size (int): overlap threshold to be used by the filter. A string pair is
                            allowed by overlap filter only if the strings share atleast
                            overlap_size number of tokens.

    Attributes:
        tokenizer (Tokenizer object): An attribute to store the tokenizer.
        overlap_size (int): An attribute to store the overlap threshold value.
    """

    def __init__(self, tokenizer, overlap_size=1):
        # check if the input tokenizer is valid
        validate_tokenizer(tokenizer)

        # check if the overlap size is valid
        validate_threshold(overlap_size, 'OVERLAP')

        self.tokenizer = tokenizer
        self.overlap_size = overlap_size
        super(self.__class__, self).__init__()

    def filter_pair(self, lstring, rstring):
        """Checks if the input strings get dropped by the overlap filter.

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
 
        num_overlap = overlap(ltokens, rtokens) 

        if num_overlap < self.overlap_size:
            return True
        else:
            return False

    def filter_tables(self, ltable, rtable,
                      l_key_attr, r_key_attr,
                      l_filter_attr, r_filter_attr,
                      l_out_attrs=None, r_out_attrs=None,
                      l_out_prefix='l_', r_out_prefix='r_',
                      out_sim_score=False, n_jobs=1):
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
                                                l_out_prefix, r_out_prefix,
                                                out_sim_score)
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
                                                  l_out_prefix, r_out_prefix,
                                                  out_sim_score)
                                              for rtable_split in rtable_splits)
            output_table = pd.concat(results)
            output_table.insert(0, '_id', range(0, len(output_table)))
            return output_table


def _filter_tables_split(ltable, rtable,
                         l_key_attr, r_key_attr,
                         l_filter_attr, r_filter_attr,
                         overlap_filter,
                         l_out_attrs, r_out_attrs,
                         l_out_prefix, r_out_prefix,
                         out_sim_score):
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

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    prog_bar = pyprind.ProgBar(len(rtable))

    for r_row in rtable_list:
        r_string = str(r_row[r_filter_attr_index])
        r_filter_attr_tokens = overlap_filter.tokenizer.tokenize(r_string)

        # probe inverted index and find overlap of candidates          
        candidate_overlap = _find_candidates(r_filter_attr_tokens,
                                             inverted_index)

        for cand, overlap in iteritems(candidate_overlap):
            if overlap >= overlap_filter.overlap_size:
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
