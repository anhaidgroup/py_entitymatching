import collections
import dask
from dask import delayed
from dask.diagnostics import ProgressBar
import logging
import multiprocessing
import pandas as pd
import re
import six
import string

import py_stringsimjoin as ssj
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
from py_stringsimjoin.utils.missing_value_handler import get_pairs_with_missing_value

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column

from py_entitymatching.blocker.blocker import Blocker
import py_entitymatching.utils.generic_helper as gh
from py_entitymatching.utils.validation_helper import validate_object_type


logger = logging.getLogger(__name__)

class DaskOverlapBlocker(Blocker):

    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to',
                           'was', 'were', 'will', 'with']
        self.regex_punctuation = re.compile('[%s]' % re.escape(string.punctuation))
        super(DaskOverlapBlocker, self).__init__()

    def validate_types_params_tables(self, ltable, rtable,
                                     l_output_attrs, r_output_attrs, l_output_prefix,
                                     r_output_prefix, verbose, n_ltable_chunks,
                                     n_rtable_chunks):

        validate_object_type(ltable, pd.DataFrame, error_prefix='Input left table')
        validate_object_type(rtable, pd.DataFrame, error_prefix='Input right table')

        if l_output_attrs:
            validate_object_type(l_output_attrs, list, 'Output attributes of left table')
            for x in l_output_attrs:
                validate_object_type(x, six.string_types, 'An output attribute name of left table')

        if r_output_attrs:
            validate_object_type(r_output_attrs, list, 'Output attributes of right table')
            for x in r_output_attrs:
                validate_object_type(x, six.string_types, 'An output attribute name of right table')

        validate_object_type(l_output_prefix, six.string_types, 'Output prefix of left table')
        validate_object_type(r_output_prefix, six.string_types, 'Output prefix of right table')

        validate_object_type(verbose, bool, 'Parameter verbose')
        validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
        validate_object_type(n_rtable_chunks, int, 'Parameter n_rtable_chunks')

    def validate_types_other_params(self, l_overlap_attr, r_overlap_attr,
                                    rem_stop_words, q_val,
                                    word_level, overlap_size):
        validate_object_type(l_overlap_attr, six.string_types, error_prefix='Overlap attribute name of left table')
        validate_object_type(r_overlap_attr, six.string_types, error_prefix='Overlap attribute name of right table')

        validate_object_type(rem_stop_words, bool, error_prefix='Parameter rem_stop_words')

        if q_val != None and not isinstance(q_val, int):
            logger.error('Parameter q_val is not of type int')
            raise AssertionError('Parameter q_val is not of type int')

        validate_object_type(word_level, bool, error_prefix='Parameter word_level')
        validate_object_type(overlap_size, int, error_prefix='Parameter overlap_size')


    # validate the overlap attrs
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

    # validate word_level and q_val
    def validate_word_level_qval(self, word_level, q_val):
        if word_level == True and q_val != None:
            raise SyntaxError(
                'Parameters word_level and q_val cannot be set together; Note that word_level is '
                'set to True by default, so explicity set word_level=false to use qgram with the '
                'specified q_val')

        if word_level == False and q_val == None:
            raise SyntaxError(
                'Parameters word_level and q_val cannot be unset together; Note that q_val is '
                'set to None by default, so if you want to use qgram then '
                'explictiy set word_level=False and specify the q_val')



    def block_tables(self, ltable, rtable, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True, overlap_size=1,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     allow_missing=False, verbose=False, show_progress=True,
                     n_ltable_chunks=-1, n_rtable_chunks=-1):

        self.validate_types_params_tables(ltable, rtable, l_output_attrs,
                                          r_output_attrs, l_output_prefix,
                                          r_output_prefix, verbose, n_ltable_chunks,
                                          n_rtable_chunks)
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val, word_level, overlap_size)
        self.validate_allow_missing(allow_missing)
        self.validate_show_progress(show_progress)
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr, r_overlap_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)
        self.validate_word_level_qval(word_level, q_val)

        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        if n_ltable_chunks == -1:
            n_ltable_chunks = multiprocessing.cpu_count()


        ltable_chunks = pd.np.array_split(ltable, n_ltable_chunks)

        # preprocess/tokenize ltable
        if word_level == True:
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            tokenizer = QgramTokenizer(qval=q_val, return_set=True)

        preprocessed_tokenized_ltbl = []
        start_row_id = 0
        for i in range(len(ltable_chunks)):
            result = delayed(self.process_tokenize_block_attr)(ltable_chunks[i][
                                                                  l_overlap_attr],
                                                              start_row_id,
                                                              rem_stop_words, tokenizer)
            preprocessed_tokenized_ltbl.append(result)
            start_row_id += len(ltable_chunks[i])
        preprocessed_tokenized_ltbl = delayed(wrap)(preprocessed_tokenized_ltbl)
        if show_progress:
            with ProgressBar():
                logger.info('Preprocessing/tokenizing ltable')
                preprocessed_tokenized_ltbl_vals = preprocessed_tokenized_ltbl.compute(
                scheduler="processes", num_workers=multiprocessing.cpu_count())
        else:
            preprocessed_tokenized_ltbl_vals = preprocessed_tokenized_ltbl.compute(
                scheduler="processes", num_workers=multiprocessing.cpu_count())

        ltable_processed_dict = {}
        for i in range(len(preprocessed_tokenized_ltbl_vals)):
            ltable_processed_dict.update(preprocessed_tokenized_ltbl_vals[i])

        # build inverted index
        inverted_index = self.build_inverted_index(ltable_processed_dict)

        if n_rtable_chunks == -1:
            n_rtable_chunks = multiprocessing.cpu_count()

        rtable_chunks = pd.np.array_split(rtable, n_rtable_chunks)

        probe_result = []
        start_row_id = 0
        for i in range(len(rtable_chunks)):
            result = delayed(self.probe)(rtable_chunks[i][r_overlap_attr],
                                         inverted_index, start_row_id, rem_stop_words,
                                         tokenizer, overlap_size)
            probe_result.append(result)
            start_row_id += len(rtable_chunks[i])
        probe_result = delayed(wrap)(probe_result)
        if show_progress:
            with ProgressBar():
                logger.info('Probing using rtable')
                probe_result = probe_result.compute(scheduler="processes",
                                            num_workers=multiprocessing.cpu_count())
        else:
            probe_result = probe_result.compute(scheduler="processes",
                                                num_workers=multiprocessing.cpu_count())

        # construct a minimal dataframe that can be used to add more attributes
        flat_list = [item for sublist in probe_result for item in sublist]
        tmp = pd.DataFrame(flat_list, columns=['fk_ltable_rid', 'fk_rtable_rid'])
        fk_ltable = ltable.iloc[tmp.fk_ltable_rid]['id'].values
        fk_rtable = rtable.iloc[tmp.fk_rtable_rid]['id'].values
        id_vals = list(range(len(flat_list)))

        candset = pd.DataFrame.from_dict(
            {'_id': id_vals, 'ltable_'+l_key: fk_ltable, 'rtable_'+r_key: fk_rtable})


        cm.set_key(candset, '_id')
        cm.set_fk_ltable(candset, 'ltable_'+l_key)
        cm.set_fk_rtable(candset, 'rtable_'+r_key)
        cm.set_ltable(candset, ltable)
        cm.set_rtable(candset, rtable)

        ret_candset = gh.add_output_attributes(candset, l_output_attrs=l_output_attrs,
                                               r_output_attrs=r_output_attrs,
                                               l_output_prefix=l_output_prefix,
                                               r_output_prefix=r_output_prefix,
                                               validate=False)



        # handle missing values
        if allow_missing:
            missing_value_pairs = get_pairs_with_missing_value(ltable, rtable, l_key,
                                                           r_key, l_overlap_attr,
                                                           r_overlap_attr,
                                                           l_output_attrs,
                                                           r_output_attrs,
                                                           l_output_prefix,
                                                           r_output_prefix, False, False)
            missing_value_pairs.insert(0, '_id', range(len(ret_candset),
                                                       len(ret_candset)+len(missing_value_pairs)))

            if len(missing_value_pairs) > 0:
                ret_candset = pd.concat([ret_candset, missing_value_pairs], ignore_index=True, sort=False)
                cm.set_key(ret_candset, '_id')
                cm.set_fk_ltable(ret_candset, 'ltable_' + l_key)
                cm.set_fk_rtable(ret_candset, 'rtable_' + r_key)
                cm.set_ltable(ret_candset, ltable)
                cm.set_rtable(ret_candset, rtable)
        return ret_candset


    def block_candset(self, candset, l_overlap_attr, r_overlap_attr,
                      rem_stop_words=False, q_val=None, word_level=True,
                      overlap_size=1, allow_missing=False,
                      verbose=False, show_progress=True, n_chunks=-1):


        self.validate_types_params_candset(candset, verbose, show_progress, n_chunks)
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val, word_level, overlap_size)

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

        # validate word_level and q_val
        self.validate_word_level_qval(word_level, q_val)

        # # do projection before merge
        l_df = ltable[[l_key, l_overlap_attr]]
        r_df = rtable[[r_key, r_overlap_attr]]

        # # set index for convenience
        l_df = l_df.set_index(l_key, drop=False)
        r_df = r_df.set_index(r_key, drop=False)

        # # case the overlap attribute to string if required.
        l_df.is_copy, r_df.is_copy = False, False  # to avoid setwithcopy warning
        ssj.dataframe_column_to_str(l_df, l_overlap_attr, inplace=True)
        ssj.dataframe_column_to_str(r_df, r_overlap_attr, inplace=True)

        if word_level == True:
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            tokenizer = QgramTokenizer(return_set=True)

        n_procs = multiprocessing.cpu_count()

        c_splits = pd.np.array_split(candset, n_chunks)
        valid_splits = []
        for i in range(n_chunks):
            result = delayed(self._block_candset_split)(c_splits[i], l_df, r_df, l_key,
                                                       r_key, l_overlap_attr,
                                                       r_overlap_attr, fk_rtable,
                                                       fk_rtable, allow_missing,
                                                       rem_stop_words, tokenizer, overlap_size)
            valid_splits.append(result)
        valid_splits = delayed(wrap)(valid_splits)
        if show_progress:
            with ProgressBar():
                valid_splits = valid_splits.compute(scheduler="processes",
                                                    num_workers=n_procs)
        else:
            valid_splits = valid_splits.compute(scheduler="processes",
                                                num_workers=n_procs)

        valid = sum(valid_splits, [])

        # construct output table
        if len(candset) > 0:
            out_table = candset[valid]
        else:
            out_table = pd.DataFrame(columns=candset.columns)

        # update the catalog
        cm.set_candset_properties(out_table, key, fk_ltable, fk_rtable,
                                  ltable, rtable)

        # return the output table
        return out_table

    def _block_candset_split(self, c_df, l_df, r_df, l_key, r_key, l_block_attr,
                             r_block_attr, fk_ltable, fk_rtable, allow_missing,
                             rem_stop_words, tokenizer, overlap_threshold):
        valid = []

        col_names = list(c_df.columns)
        lkey_idx = col_names.index(fk_ltable)
        rkey_idx = col_names.index(fk_rtable)

        l_dict = {}
        r_dict = {}

        for row in c_df.itertuples(index=False):
            row_lkey = row[lkey_idx]
            if row_lkey not in l_dict:
                l_dict[row_lkey] = l_df.ix[row_lkey, l_block_attr]
            l_val = l_dict[row_lkey]

            row_rkey = row[rkey_idx]
            if row_rkey not in r_dict:
                r_dict[row_rkey] = r_df.ix[row_rkey, r_block_attr]
            r_val = r_dict[row_rkey]
            if allow_missing:
                if pd.isnull(l_val) or pd.isnull(r_val) or l_val == r_val:
                    valid.append(True)
                else:
                    valid.append(False)
            else:
                if pd.notnull(l_val) and pd.notnull(r_val):
                    l_tokens = self._process_tokenize_block_str(l_val, rem_stop_words,
                                                                tokenizer)
                    r_tokens = self._process_tokenize_block_str(r_val, rem_stop_words,
                                                                tokenizer)
                    overlap = len(set(l_tokens).intersection(r_tokens))
                    if overlap > overlap_threshold:
                        valid.append(True)
                    else:
                        valid.append(False)
                else:
                    valid.append(False)
        return valid

    def process_tokenize_block_attr(self, block_column_values,
                                    start_row_id, should_rem_stop_words, tokenizer):
        result_vals = {}
        row_id = start_row_id
        for s in block_column_values:
            if not s or pd.isnull(s):
                row_id += 1
                continue
            val = self._process_tokenize_block_str(s, should_rem_stop_words, tokenizer)
            result_vals[row_id] = val
            row_id += 1
        return result_vals

    def build_inverted_index(self, process_tok_vals):
        index = collections.defaultdict(set)
        for key in process_tok_vals:
            for val in process_tok_vals[key]:
                index[val].add(key)
        return index


    def _find_candidates(self, probe_tokens, inverted_index):
        overlap_candidates = {}
        for token in probe_tokens:
            candidates = inverted_index.get(token, [])
            for candidate in candidates:
                overlap_candidates[candidate] = overlap_candidates.get(candidate, 0) + 1
        return overlap_candidates

    def probe(self, block_column_values, inverted_index, start_row_id,
              should_rem_stop_words, tokenizer, overlap_threshold):
        result = []
        row_id = start_row_id
        for s in block_column_values:
            if not s:
                row_id += 1
                continue
            probe_tokens = self._process_tokenize_block_str(s, should_rem_stop_words,
                                                            tokenizer)
            candidates = self._find_candidates(probe_tokens, inverted_index)
            for cand, overlap in candidates.items():
                if overlap > overlap_threshold:
                    result.append((cand, row_id))
                    row_id += 1
        return result

    def _process_tokenize_block_str(self, s, should_rem_stop_words, tokenizer):
        if not s or pd.isnull(s):
            return s
        if isinstance(s, bytes):
            s = s.decode('utf-8', 'ignore')
        s = s.lower()
        s = self.regex_punctuation.sub('', s)
        tokens = list(set(s.strip().split()))
        if should_rem_stop_words:
            tokens = [token for token in tokens if token not in self.stop_words]
        s = ' '.join(tokens)
        tokenized_str = tokenizer.tokenize(s)
        return tokenized_str



def wrap(object):
    return object
