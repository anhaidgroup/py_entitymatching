# coding=utf-8
import logging
import re
import string
from collections import OrderedDict, Counter

import pandas as pd
import pyprind
import six

import magellan.catalog.catalog_manager as cm
from magellan.blocker.blocker import Blocker

# from magellan.externals.py_stringmatching.tokenizers import qgram
import py_stringmatching.tokenizer as tok
from magellan.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column

from magellan.utils.generic_helper import remove_non_ascii, rem_nan

logger = logging.getLogger(__name__)


class OverlapBlocker(Blocker):
    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to',
                           'was', 'were', 'will', 'with']
        self.regex_punctuation = re.compile(
            '[%s]' % re.escape(string.punctuation))
        super(OverlapBlocker, self).__init__()

    def block_tables(self, ltable, rtable, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, show_progress=True):
        # validations
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr,
                                    r_overlap_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs,
                                   r_output_attrs)

        # required metadata; keys from ltable and rtable
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger,
                                                     verbose)

        # do blocking

        if word_level == True and q_val != None:
            raise SyntaxError(
                'Parameters word_level and q_val cannot be set together; Note that word_level is '
                'set to True by default, so explicity set word_level=false to use qgram with the '
                'specified q_val')

        # #rem nans
        l_df = rem_nan(ltable, l_overlap_attr)
        r_df = rem_nan(rtable, r_overlap_attr)

        # #reset indexes in the dataframe
        l_df.reset_index(inplace=True, drop=True)
        r_df.reset_index(inplace=True, drop=True)

        # #create a dummy column with all values set to 1.
        l_dummy_col_name = self.get_dummy_col_name(l_df.columns)
        r_dummy_col_name = self.get_dummy_col_name(r_df.columns)
        l_df[
            l_dummy_col_name] = 1  # need to fix this - should be a name that does not occur in the col. names
        r_df[r_dummy_col_name] = 1

        # #case the column to string if required.
        if l_df.dtypes[l_overlap_attr] != object:
            logger.warning(
                'Left overlap attribute is not of type string; coverting to string temporarily')
            l_df[l_overlap_attr] = l_df[l_overlap_attr].astype(str)

        if r_df.dtypes[r_overlap_attr] != object:
            logger.warning(
                'Right overlap attribute is not of type string; coverting to string temporarily')
            r_df[r_overlap_attr] = r_df[r_overlap_attr].astype(str)

        l_dict = {}
        r_dict = {}

        # #create a lookup table for quick access
        for k, r in l_df.iterrows():
            l_dict[k] = r

        for k, r in r_df.iterrows():
            r_dict[k] = r

        l_colvalues_chopped = self.process_table(l_df, l_overlap_attr, q_val,
                                                 rem_stop_words)
        zipped_l_colvalues = zip(l_colvalues_chopped,
                                 range(0, len(l_colvalues_chopped)))
        appended_l_colidx_values = [self.append_index_values(val[0], val[1]) for
                                    val in zipped_l_colvalues]

        inv_idx = {}
        sink = [self.compute_inv_index(t, inv_idx) for c in
                appended_l_colidx_values for t in c]

        r_colvalues_chopped = self.process_table(r_df, r_overlap_attr, q_val,
                                                 rem_stop_words)
        r_idx = 0

        white_list = []
        if show_progress:
            bar = pyprind.ProgBar(len(r_colvalues_chopped))

        df_list = []
        for col_values in r_colvalues_chopped:
            if show_progress:
                bar.update()

            qualifying_ltable_indices = self.get_potential_match_indices(
                col_values, inv_idx, overlap_size)
            r_row = r_dict[r_idx]
            r_row_dict = r_row.to_frame().T

            l_rows_dict = l_df.iloc[qualifying_ltable_indices]
            df = l_rows_dict.merge(r_row_dict, left_on=l_dummy_col_name,
                                   right_on=r_dummy_col_name,
                                   suffixes=('_ltable', '_rtable'))

            if len(df) > 0:
                df_list.append(df)
            r_idx += 1

        # Construct the output table
        candset = pd.concat(df_list)
        l_output_attrs = self.process_output_attrs(ltable, l_key,
                                                   l_output_attrs, 'left')
        r_output_attrs = self.process_output_attrs(rtable, r_key,
                                                   r_output_attrs, 'right')

        # retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs, r_output_attrs,
        #                                        l_output_prefix, r_output_prefix)
        retain_cols, final_cols = self.output_columns(l_key, r_key,
                                                      list(candset.columns),
                                                      l_output_attrs,
                                                      r_output_attrs,
                                                      l_output_prefix,
                                                      r_output_prefix)

        if len(candset) > 0:
            candset = candset[retain_cols]
            candset.columns = final_cols
        else:
            candset = pd.DataFrame(columns=final_cols)

        # Update metadata in the catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return the candidate set
        return candset

    def block_candset(self, candset, l_overlap_attr, r_overlap_attr,
                      rem_stop_words=False, q_val=None, word_level=True,
                      overlap_size=1,
                      verbose=True, show_progress=True):

        # get and validate metadata
        log_info(logger,
                 'Required metadata: cand.set key, fk ltable, fk rtable, '
                 'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(
            candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable,
                                          ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # validate overlap attrs
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr,
                                    r_overlap_attr)

        # do blocking
        # # initialize the progress bar

        if show_progress:
            bar = pyprind.ProgBar(len(candset))

        # # set the index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # create lookup table for faster processing
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # # list to keep track of valid ids
        valid = []
        l_id_pos = list(candset.columns).index(fk_ltable)
        r_id_pos = list(candset.columns).index(fk_rtable)

        # # iterate candset
        for row in candset.itertuples(index=False):
            # # update progress bar
            if show_progress:
                bar.update()

            ltuple = l_dict[row[l_id_pos]]
            rtuple = r_dict[row[r_id_pos]]

            num_overlap = self.get_token_overlap_bt_two_tuples(ltuple, rtuple,
                                                               l_overlap_attr,
                                                               r_overlap_attr,
                                                               q_val,
                                                               rem_stop_words)

            if num_overlap >= overlap_size:
                valid.append(True)
            else:
                valid.append(False)

        if len(candset) > 0:
            candset = candset[valid]
        else:
            candset = pd.DataFrame(columns=candset.columns)

        # update catalog
        cm.set_candset_properties(candset, key, fk_ltable, fk_rtable, ltable,
                                  rtable)

        # return candidate set
        return candset

    def block_tuples(self, ltuple, rtuple, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1):

        num_overlap = self.get_token_overlap_bt_two_tuples(ltuple, rtuple,
                                                           l_overlap_attr,
                                                           r_overlap_attr,
                                                           q_val,
                                                           rem_stop_words)
        if num_overlap < overlap_size:
            return True
        else:
            return False

    # helper functions
    # validate the blocking attrs
    def validate_overlap_attrs(self, ltable, rtable, l_overlap_attr,
                               r_overlap_attr):
        if not isinstance(l_overlap_attr, list):
            l_overlap_attr = [l_overlap_attr]
        assert set(l_overlap_attr).issubset(
            ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_overlap_attr, list):
            r_overlap_attr = [r_overlap_attr]
        assert set(r_overlap_attr).issubset(
            rtable.columns) is True, 'Right block attribute is not in the right table'

    def get_token_overlap_bt_two_tuples(self, l_tuple, r_tuple, l_overlap_attr,
                                        r_overlap_attr,
                                        q_val, rem_stop_words):
        l_val = l_tuple[l_overlap_attr]
        r_val = r_tuple[r_overlap_attr]

        if l_val == None and r_val == None:
            return 0

        if not isinstance(l_val, six.string_types):
            l_val = str(l_val)

        if not isinstance(r_val, six.string_types):
            r_val = str(r_val)

        l_val_lst = set(self.process_val(l_val, q_val, rem_stop_words))
        r_val_lst = set(self.process_val(r_val, q_val, rem_stop_words))

        return len(l_val_lst.intersection(r_val_lst))

    def process_val(self, val, q_val, rem_stop_words):
        val = remove_non_ascii(val)
        val = self.rem_punctuations(val).lower()
        chopped_vals = val.split()
        if rem_stop_words == True:
            chopped_vals = self.rem_stopwords(chopped_vals)
        if q_val != None:
            values = ' '.join(chopped_vals)
            measure = tok.qgram_tokenizer.QgramTokenizer(qval=q_val)
            chopped_vals = measure.tokenize(values)
        return list(set(chopped_vals))

    def get_row_dict_with_output_attrs(self, l_tuple, r_tuple, l_key, r_key,
                                       l_output_attrs, r_output_attrs,
                                       l_output_prefix, r_output_prefix):
        d = OrderedDict()

        ltable_id = l_output_prefix + l_key
        d[ltable_id] = l_tuple[l_key]

        rtable_id = r_output_prefix + r_key
        d[rtable_id] = r_tuple[r_key]

        # add ltable attrs
        if l_output_attrs:
            l_out = l_tuple[l_output_attrs]
            l_out.index += l_output_prefix
            d.update(l_out)

        if r_output_attrs:
            r_out = r_tuple[r_output_attrs]
            r_out.index += r_output_prefix
            d.update(r_out)

        return d

    def process_table(self, table, overlap_attr, q_val, rem_stop_words):

        # get overlap_attr column
        attr_col_values = table[overlap_attr]

        # remove non-ascii chars
        attr_col_values = [remove_non_ascii(val) for val in attr_col_values]

        # remove special characters
        attr_col_values = [self.rem_punctuations(val).lower() for val in
                           attr_col_values]

        # chop the attribute values
        col_values_chopped = [val.split() for val in attr_col_values]

        # convert the chopped values into a set
        col_values_chopped = [list(set(val)) for val in col_values_chopped]

        # remove stop words
        if rem_stop_words == True:
            col_values_chopped = [self.rem_stopwords(val) for val in
                                  col_values_chopped]

        if q_val is not None:
            values = [' '.join(val) for val in col_values_chopped]
            col_values_chopped = [qgram(val, q_val) for val in values]

        return col_values_chopped

    def rem_punctuations(self, s):
        return self.regex_punctuation.sub('', s)

    def rem_stopwords(self, lst):
        return [t for t in lst if t not in self.stop_words]

    def append_index_values(self, lst, idx):
        lst = set(lst)
        return [(val, idx) for val in lst]

    def compute_inv_index(self, tok_idx, idx):
        lst = idx.pop(tok_idx[0], None)

        if lst is None:
            lst = list()
            lst.append(tok_idx[1])
            idx[tok_idx[0]] = lst
        else:
            lst.append(tok_idx[1])
            idx[tok_idx[0]] = lst

    def probe_inv_index_for_a_token(self, token, inv_index):
        return inv_index.get(token, None)

    def probe_inv_index(self, lst, inv_index):
        return [self.probe_inv_index_for_a_token(tok, inv_index) for tok in lst]

    def get_freq_count(self, lst):
        p = list()
        dummy = [p.extend(k) for k in lst if k is not None]
        d = dict(Counter(p))
        return d

    def get_qualifying_indices(self, freq_dict, overlap_size):
        q_indices = []
        for k, v in freq_dict.iteritems():
            if v >= overlap_size:
                q_indices.append(k)
        return q_indices

    def get_potential_match_indices(self, lst, inv_index, overlap_size):
        indices = self.probe_inv_index(lst, inv_index)
        freq_dict = self.get_freq_count(indices)
        qualifying_indices = self.get_qualifying_indices(freq_dict,
                                                         overlap_size)
        return qualifying_indices

    def get_dummy_col_name(self, columns):
        k = '_dummy_'
        i = 0
        # try attribute name of the form "_dummy_, _dummy_0,.. and
        # return the first available name
        while True:
            if k not in columns:
                break
            else:
                k = '_dummy_' + str(i)
            i += 1
        return k
        # get output columns

    def output_columns(self, l_key, r_key, col_names, l_output_attrs,
                       r_output_attrs, l_output_prefix, r_output_prefix):

        ret_cols = []
        fin_cols = []

        # retain id columns from merge
        ret_l_id = [self.retain_names(x, col_names, '_ltable') for x in [l_key]]
        ret_r_id = [self.retain_names(x, col_names, '_rtable') for x in [r_key]]
        ret_cols.extend(ret_l_id)
        ret_cols.extend(ret_r_id)

        # retain output attrs from merge
        if l_output_attrs:
            ret_l_col = [self.retain_names(x, col_names, '_ltable') for x in
                         l_output_attrs]
            ret_cols.extend(ret_l_col)
        if r_output_attrs:
            ret_r_col = [self.retain_names(x, col_names, '_rtable') for x in
                         r_output_attrs]
            ret_cols.extend(ret_r_col)

        # final columns in the output
        fin_l_id = [self.final_names(x, l_output_prefix) for x in [l_key]]
        fin_r_id = [self.final_names(x, r_output_prefix) for x in [r_key]]
        fin_cols.extend(fin_l_id)
        fin_cols.extend(fin_r_id)

        # final output attrs from merge
        if l_output_attrs:
            fin_l_col = [self.final_names(x, l_output_prefix) for x in
                         l_output_attrs]
            fin_cols.extend(fin_l_col)
        if r_output_attrs:
            fin_r_col = [self.final_names(x, r_output_prefix) for x in
                         r_output_attrs]
            fin_cols.extend(fin_r_col)

        return ret_cols, fin_cols

    def retain_names(self, x, col_names, suffix):
        if x in col_names:
            return x
        else:
            return str(x) + suffix

    def final_names(self, x, prefix):
        return prefix + str(x)
