from math import floor

import pandas as pd
import pyprind

from py_stringsimjoin.filter.filter import Filter
from py_stringsimjoin.filter.filter_utils import get_overlap_threshold
from py_stringsimjoin.filter.filter_utils import get_prefix_length
from py_stringsimjoin.utils.helper_functions import convert_dataframe_to_list
from py_stringsimjoin.utils.helper_functions import \
                                                 find_output_attribute_indices
from py_stringsimjoin.utils.helper_functions import \
                                                 get_output_header_from_tables
from py_stringsimjoin.utils.helper_functions import get_output_row_from_tables
from py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_tables
from py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_lists
from py_stringsimjoin.utils.token_ordering import order_using_token_ordering
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs, validate_sim_measure_type


class SuffixFilter(Filter):
    """Finds candidate matching pairs of strings using suffix filtering technique.

    For similarity measures such as cosine, Dice, Jaccard and overlap, the filter finds candidate
    string pairs that may have similarity score greater than or equal to the input threshold.
    Where as for distance measure such as edit distance, the filter finds candidate string pairs 
    that may have distance score less than or equal to the threshold.

    To know about suffix filtering, refer the paper `Efficient Similarity Joins for Near Duplicate Detection <http://www.cse.unsw.edu.au/~weiw/files/WWW08-PPJoin-Final.pdf>`_.

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
        self.max_depth = 2
        super(self.__class__, self).__init__()

    def filter_pair(self, lstring, rstring):
        """Checks if the input strings get dropped by the suffix filter.

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

        l_num_tokens = len(ordered_ltokens)
        r_num_tokens = len(ordered_rtokens)
        l_prefix_length = get_prefix_length(l_num_tokens,
                                            self.sim_measure_type,
                                            self.threshold,
                                            self.tokenizer)
        r_prefix_length = get_prefix_length(r_num_tokens,
                                            self.sim_measure_type,
                                            self.threshold,
                                            self.tokenizer)
        return self._filter_suffix(ordered_ltokens[l_prefix_length:],
                             ordered_rtokens[r_prefix_length:],
                             l_prefix_length,
                             r_prefix_length,
                             len(ltokens), len(rtokens))
    
    def _filter_suffix(self, l_suffix, r_suffix,
                       l_prefix_num_tokens, r_prefix_num_tokens,
                       l_num_tokens, r_num_tokens):
        overlap_threshold = get_overlap_threshold(l_num_tokens, r_num_tokens,
                                                  self.sim_measure_type,
                                                  self.threshold,
                                                  self.tokenizer)

        hamming_dist_prefix = r_prefix_num_tokens - l_prefix_num_tokens
        if l_num_tokens >= r_num_tokens:
            hamming_dist_prefix = l_prefix_num_tokens - r_prefix_num_tokens
        hamming_dist_max = (l_num_tokens + r_num_tokens -
                            2 * overlap_threshold - hamming_dist_prefix)

        hamming_dist = self._est_hamming_dist_lower_bound(
                                l_suffix, r_suffix,
                                l_num_tokens - l_prefix_num_tokens,
                                r_num_tokens - r_prefix_num_tokens,
                                hamming_dist_max, 1)
        if hamming_dist <= hamming_dist_max:
            return False
        return True

    def filter_tables(self, ltable, rtable,
                      l_key_attr, r_key_attr,
                      l_filter_attr, r_filter_attr,
                      l_out_attrs=None, r_out_attrs=None,
                      l_out_prefix='l_', r_out_prefix='r_'):
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

        # find column indices of key attr, filter attr and
        # output attrs in ltable
        l_columns = list(ltable.columns.values)
        l_key_attr_index = l_columns.index(l_key_attr)
        l_filter_attr_index = l_columns.index(l_filter_attr)
        l_out_attrs_indices = find_output_attribute_indices(l_columns,
                                                            l_out_attrs)

        # find column indices of key attr, filter attr and
        # output attrs in rtable
        r_columns = list(rtable.columns.values)
        r_key_attr_index = r_columns.index(r_key_attr)
        r_filter_attr_index = r_columns.index(r_filter_attr)
        r_out_attrs_indices = find_output_attribute_indices(r_columns,
                                                            r_out_attrs)
        
        # convert ltable into a list of tuples
        ltable_list = convert_dataframe_to_list(ltable, l_filter_attr_index)

        # convert rtable into a list of tuples
        rtable_list = convert_dataframe_to_list(rtable, r_filter_attr_index)

        # generate token ordering using tokens in l_filter_attr
        # and r_filter_attr
        token_ordering = gen_token_ordering_for_tables(
                                    [ltable_list, rtable_list],
                                    [l_filter_attr_index, r_filter_attr_index],
                                    self.tokenizer, self.sim_measure_type)

        output_rows = []
        has_output_attributes = (l_out_attrs is not None or
                                 r_out_attrs is not None)
        prog_bar = pyprind.ProgBar(len(ltable))

        for l_row in ltable_list:
            l_id = l_row[l_key_attr_index]
            l_string = str(l_row[l_filter_attr_index])

            ltokens = self.tokenizer.tokenize(l_string)
            ordered_ltokens = order_using_token_ordering(ltokens,
                                                         token_ordering)
            l_num_tokens = len(ordered_ltokens)
            l_prefix_length = get_prefix_length(l_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold,
                                                self.tokenizer)
            l_suffix = ordered_ltokens[l_prefix_length:]
            for r_row in rtable_list:
                r_id = r_row[r_key_attr_index]
                r_string = str(r_row[r_filter_attr_index])

                rtokens = self.tokenizer.tokenize(r_string)
                ordered_rtokens = order_using_token_ordering(rtokens,
                                                             token_ordering)
                r_num_tokens = len(ordered_rtokens)
                r_prefix_length = get_prefix_length(r_num_tokens,
                                                    self.sim_measure_type,
                                                    self.threshold,
                                                    self.tokenizer)
                if not self._filter_suffix(l_suffix,
                                           ordered_rtokens[r_prefix_length:],
                                           l_prefix_length,
                                           r_prefix_length,
                                           l_num_tokens, r_num_tokens):
                    if has_output_attributes:
                        output_row = get_output_row_from_tables(
                                         l_row, r_row,
                                         l_key_attr_index, r_key_attr_index, 
                                         l_out_attrs_indices,
                                         r_out_attrs_indices)
                    else:
                        output_row = [l_row[l_key_attr_index],
                                      r_row[r_key_attr_index]]

                    output_rows.append(output_row)

            prog_bar.update()

        output_header = get_output_header_from_tables(
                            l_key_attr, r_key_attr,
                            l_out_attrs, r_out_attrs, 
                            l_out_prefix, r_out_prefix)

        # generate a dataframe from the list of output rows
        output_table = pd.DataFrame(output_rows, columns=output_header)
        output_table.insert(0, '_id', range(0, len(output_table)))
        return output_table

    def _est_hamming_dist_lower_bound(self, l_suffix, r_suffix,
                                      l_suffix_num_tokens,
                                      r_suffix_num_tokens,
                                      hamming_dist_max, depth):
        abs_diff = abs(l_suffix_num_tokens - r_suffix_num_tokens)
        if (depth > self.max_depth or
            l_suffix_num_tokens == 0 or
            r_suffix_num_tokens == 0):
            return abs_diff

        r_mid = int(floor(r_suffix_num_tokens / 2))
        r_mid_token = r_suffix[r_mid]
        o = (hamming_dist_max - abs_diff) / 2

        if l_suffix_num_tokens <= r_suffix_num_tokens:
            o_l = 1
            o_r = 0
        else:
            o_l = 0
            o_r = 1

        (r_l, r_r, flag, diff) = self._partition(
                                  r_suffix, r_mid_token, r_mid, r_mid)
        (l_l, l_r, flag, diff) = self._partition(l_suffix, r_mid_token, 
                                  max(0, int(r_mid - o - abs_diff * o_l)),
                                  min(l_suffix_num_tokens - 1,
                                      int(r_mid + o + abs_diff * o_r)))

        r_l_num_tokens = len(r_l)
        r_r_num_tokens = len(r_r)
        l_l_num_tokens = len(l_l)
        l_r_num_tokens = len(l_r)
        hamming_dist = (abs(l_l_num_tokens - r_l_num_tokens) +
                        abs(l_r_num_tokens - r_r_num_tokens) + diff)

        if hamming_dist > hamming_dist_max:
            return hamming_dist
        else:
            hamming_dist_l = self._est_hamming_dist_lower_bound(
                             l_l, r_l, l_l_num_tokens, r_l_num_tokens,
                             hamming_dist_max -
                                 abs(l_r_num_tokens - r_r_num_tokens) - diff,
                             depth + 1)
            hamming_dist = (hamming_dist_l +
                            abs(l_r_num_tokens - r_r_num_tokens) + diff)
            if hamming_dist <= hamming_dist_max:
                hamming_dist_r = self._est_hamming_dist_lower_bound(
                                 l_r, r_r, l_r_num_tokens, r_r_num_tokens,
                                 hamming_dist_max - hamming_dist_l - diff,
                                 depth + 1)
                return hamming_dist_l + hamming_dist_r + diff
            else:
                return hamming_dist
    
    def _partition(self, tokens, probe_token, left, right):
        right = min(right, len(tokens) - 1)

        if right < left:
            return [], [], 0, 1

        if tokens[left] > probe_token:
            return [], tokens, 0, 1

        if tokens[right] < probe_token:
            return tokens, [], 0, 1

        pos = self._binary_search(tokens, probe_token, left, right)
        tokens_left = tokens[0:pos]

        if tokens[pos] == probe_token:
            tokens_right = tokens[pos+1:len(tokens)]
            diff = 0
        else:
            tokens_right = tokens[pos:len(tokens)]
            diff = 1

        return tokens_left, tokens_right, 1, diff

    def _binary_search(self, tokens, probe_token, left, right):
        if left == right:
            return left

        mid = int(floor((left + right) / 2))
        mid_token = tokens[mid]

        if mid_token == probe_token:
            return mid
        elif mid_token < probe_token:
            return self._binary_search(tokens, probe_token, mid+1, right)
        else:
            return self._binary_search(tokens, probe_token, left, mid)
