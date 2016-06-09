"""Prefix Filter"""

from joblib import delayed
from joblib import Parallel
import pandas as pd
import pyprind

from magellan.externals.py_stringsimjoin.filter.filter import Filter
from magellan.externals.py_stringsimjoin.filter.filter_utils import get_prefix_length
from magellan.externals.py_stringsimjoin.index.prefix_index import PrefixIndex
from magellan.externals.py_stringsimjoin.utils.helper_functions import build_dict_from_table
from magellan.externals.py_stringsimjoin.utils.helper_functions import \
                                                 find_output_attribute_indices
from magellan.externals.py_stringsimjoin.utils.helper_functions import \
                                                 get_output_header_from_tables
from magellan.externals.py_stringsimjoin.utils.helper_functions import get_output_row_from_tables
from magellan.externals.py_stringsimjoin.utils.helper_functions import split_table
from magellan.externals.py_stringsimjoin.utils.tokenizers import tokenize
from magellan.externals.py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_lists
from magellan.externals.py_stringsimjoin.utils.token_ordering import gen_token_ordering_for_tables
from magellan.externals.py_stringsimjoin.utils.token_ordering import order_using_token_ordering
from magellan.externals.py_stringsimjoin.utils.validation import validate_attr, \
    validate_key_attr, validate_input_table, validate_threshold, \
    validate_tokenizer, validate_output_attrs, validate_sim_measure_type


class PrefixFilter(Filter):
    """Prefix filter class.

    Attributes:
        tokenizer: Tokenizer object.
        sim_measure_type: String, similarity measure type.
        threshold: float, similarity threshold to be used by the filter.
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
        """Filter two strings with prefix filter.

        Args:
        lstring, rstring : input strings

        Returns:
        result : boolean, True if the tuple pair is dropped.
        """
        # check for empty string
        if (not lstring) or (not rstring):
            return True

        ltokens = tokenize(lstring, self.tokenizer, self.sim_measure_type)
        rtokens = tokenize(rstring, self.tokenizer, self.sim_measure_type)

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
        """Filter tables with prefix filter.

        Args:
        ltable, rtable : Pandas data frame
        l_key_attr, r_key_attr : String, key attribute from ltable and rtable
        l_filter_attr, r_filter_attr : String, filter attribute from ltable and rtable
        l_out_attrs, r_out_attrs : list of attribtues to be included in the output table from ltable and rtable
        l_out_prefix, r_out_prefix : String, prefix to be used in the attribute names of the output table 

        Returns:
        result : Pandas data frame
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

    # build a dictionary on ltable
    ltable_dict = build_dict_from_table(ltable, l_key_attr_index,
                                        l_filter_attr_index)

    # build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_filter_attr_index)

    # generate token ordering using tokens in l_filter_attr and r_filter_attr
    token_ordering = gen_token_ordering_for_tables(
                                 [ltable_dict.values(), rtable_dict.values()],
                                 [l_filter_attr_index, r_filter_attr_index],
                                 prefix_filter.tokenizer,
                                 prefix_filter.sim_measure_type)

    # Build prefix index on l_filter_attr
    prefix_index = PrefixIndex(ltable_dict.values(), l_key_attr_index,
                               l_filter_attr_index, prefix_filter.tokenizer,
                               prefix_filter.sim_measure_type,
                               prefix_filter.threshold, token_ordering)
    prefix_index.build()

    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    prog_bar = pyprind.ProgBar(len(rtable))

    for r_row in rtable_dict.values():
        r_id = r_row[r_key_attr_index]
        r_string = str(r_row[r_filter_attr_index])
        # check for empty string
        if not r_string:
            continue
        r_filter_attr_tokens = tokenize(r_string,
                                        prefix_filter.tokenizer,
                                        prefix_filter.sim_measure_type)
        r_ordered_tokens = order_using_token_ordering(r_filter_attr_tokens,
                                                      token_ordering)
           
        # probe prefix index and find candidates
        candidates = _find_candidates(r_ordered_tokens, len(r_ordered_tokens),
                                      prefix_filter, prefix_index)

        for cand in candidates:
            if has_output_attributes:
                output_row = get_output_row_from_tables(
                                     ltable_dict[cand], r_row,
                                     cand, r_id, 
                                     l_out_attrs_indices, r_out_attrs_indices)
                output_rows.append(output_row)
            else:
                output_rows.append([cand, r_id])
 
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
