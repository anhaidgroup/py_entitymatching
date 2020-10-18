import collections
import dask
from dask import delayed
from dask.diagnostics import ProgressBar
import logging
import multiprocessing
import pandas as pd
import numpy as np
import re
import six
import string

import py_stringsimjoin as ssj
from py_stringsimjoin.filter.overlap_filter import OverlapFilter
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
from py_stringsimjoin.utils.missing_value_handler import get_pairs_with_missing_value

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column

from py_entitymatching.blocker.blocker import Blocker
import py_entitymatching.utils.generic_helper as gh
from py_entitymatching.utils.validation_helper import validate_object_type
from py_entitymatching.dask.utils import validate_chunks, get_num_partitions, \
    get_num_cores, wrap


logger = logging.getLogger(__name__)

class DaskOverlapBlocker(Blocker):

    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to',
                           'was', 'were', 'will', 'with']
        logger.warning(
            "WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
            "RISK.")

        self.regex_punctuation = re.compile('[%s]' % re.escape(string.punctuation))
        super(DaskOverlapBlocker, self).__init__()


    def block_tables(self, ltable, rtable, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True, overlap_size=1,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     allow_missing=False, verbose=False, show_progress=True,
                     n_ltable_chunks=1, n_rtable_chunks=1):

        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.

        Blocks two tables based on the overlap of token sets of attribute
        values. Finds tuple pairs from left and right tables such that the overlap
        between (a) the set of tokens obtained by tokenizing the value of
        attribute l_overlap_attr of a tuple from the left table, and (b) the
        set of tokens obtained by tokenizing the value of attribute
        r_overlap_attr of a tuple from the right table, is above a certain
        threshold.

        Args:
            ltable (DataFrame): The left input table.

            rtable (DataFrame): The right input table.

            l_overlap_attr (string): The overlap attribute in left table.

            r_overlap_attr (string): The overlap attribute in right table.

            rem_stop_words (boolean): A flag to indicate whether stop words
             (e.g., a, an, the) should be removed from the token sets of the
             overlap attribute values (defaults to False).

            q_val (int): The value of q to use if the overlap attributes
             values are to be tokenized as qgrams (defaults to None).

            word_level (boolean): A flag to indicate whether the overlap
             attributes should be tokenized as words (i.e, using whitespace
             as delimiter) (defaults to True).

            overlap_size (int): The minimum number of tokens that must
             overlap (defaults to 1).
            l_output_attrs (list): A list of attribute names from the left
                table to be included in the output candidate set (defaults
                to None).
            r_output_attrs (list): A list of attribute names from the right
                table to be included in the output candidate set  (defaults
                to None).

            l_output_prefix (string): The prefix to be used for the attribute names
                                   coming from the left table in the output
                                   candidate set (defaults to 'ltable\_').
            r_output_prefix (string): The prefix to be used for the attribute names
                                   coming from the right table in the output
                                   candidate set (defaults to 'rtable\_').
            allow_missing (boolean): A flag to indicate whether tuple pairs
                                     with missing value in at least one of the
                                     blocking attributes should be included in
                                     the output candidate set (defaults to
                                     False). If this flag is set to True, a
                                     tuple in ltable with missing value in the
                                     blocking attribute will be matched with
                                     every tuple in rtable and vice versa.

            verbose (boolean): A flag to indicate whether the debug
                information should be logged (defaults to False).

            show_progress (boolean): A flag to indicate whether progress should
                be displayed to the user (defaults to True).

            n_ltable_chunks (int): The number of partitions to split the left table (
                                    defaults to 1). If it is set to -1, then the number of 
                                    partitions is set to the number of cores in the 
                                    machine.                                      
            n_rtable_chunks (int): The number of partitions to split the right table (
                                    defaults to 1). If it is set to -1, then the number of 
                                    partitions is set to the number of cores in the 
                                    machine.            


        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).
        Raises:
            AssertionError: If `ltable` is not of type pandas
                DataFrame.

            AssertionError: If `rtable` is not of type pandas
                DataFrame.

            AssertionError: If `l_overlap_attr` is not of type string.

            AssertionError: If `r_overlap_attr` is not of type string.

            AssertionError: If `l_output_attrs` is not of type of
             list.

            AssertionError: If `r_output_attrs` is not of type of
             list.

            AssertionError: If the values in `l_output_attrs` is not of type
             string.

            AssertionError: If the values in `r_output_attrs` is not of type
             string.

            AssertionError: If `l_output_prefix` is not of type
             string.

            AssertionError: If `r_output_prefix` is not of type
             string.

            AssertionError: If `q_val` is not of type int.

            AssertionError: If `word_level` is not of type boolean.

            AssertionError: If `overlap_size` is not of type int.

            AssertionError: If `verbose` is not of type
             boolean.

            AssertionError: If `allow_missing` is not of type boolean.

            AssertionError: If `show_progress` is not of type
             boolean.

            AssertionError: If `n_ltable_chunks` is not of type
             int.

            AssertionError: If `n_rtable_chunks` is not of type
             int.

            AssertionError: If `l_overlap_attr` is not in the ltable
             columns.

            AssertionError: If `r_block_attr` is not in the rtable columns.

            AssertionError: If `l_output_attrs` are not in the ltable.

            AssertionError: If `r_output_attrs` are not in the rtable.

            SyntaxError: If `q_val` is set to a valid value and
                `word_level` is set to True.

            SyntaxError: If `q_val` is set to None and
                `word_level` is set to False.

        Examples:
            >>> from py_entitymatching.dask.dask_overlap_blocker import DaskOverlapBlocker
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ob = DaskOverlapBlocker()
            # Use all cores
            # # Use word-level tokenizer
            >>> C1 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], word_level=True, overlap_size=1, n_ltable_chunks=-1, n_rtable_chunks=-1)
            # # Use q-gram tokenizer
            >>> C2 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], word_level=False, q_val=2, n_ltable_chunks=-1, n_rtable_chunks=-1)
            # # Include all possible missing values
            >>> C3 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], allow_missing=True, n_ltable_chunks=-1, n_rtable_chunks=-1)
        """
        logger.warning(
            "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
            "RISK.")

        # Input validations
        self.validate_types_params_tables(ltable, rtable, l_output_attrs,
                                          r_output_attrs, l_output_prefix,
                                          r_output_prefix, verbose, n_ltable_chunks, n_rtable_chunks)
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


        # validate input table chunks
        validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
        validate_object_type(n_rtable_chunks, int,
                             'Parameter n_rtable_chunks')
        validate_chunks(n_ltable_chunks)
        validate_chunks(n_rtable_chunks)

        if n_ltable_chunks == -1:
            n_ltable_chunks = multiprocessing.cpu_count()


        ltable_chunks = np.array_split(ltable, n_ltable_chunks)

        # preprocess/tokenize ltable
        if word_level == True:
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            tokenizer = QgramTokenizer(qval=q_val, return_set=True)

        preprocessed_tokenized_ltbl = []

        # Construct DAG for preprocessing/tokenizing ltable chunks
        start_row_id = 0
        for i in range(len(ltable_chunks)):
            result = delayed(self.process_tokenize_block_attr)(ltable_chunks[i][
                                                                  l_overlap_attr],
                                                              start_row_id,
                                                              rem_stop_words, tokenizer)
            preprocessed_tokenized_ltbl.append(result)
            start_row_id += len(ltable_chunks[i])
        preprocessed_tokenized_ltbl = delayed(wrap)(preprocessed_tokenized_ltbl)

        # Execute the DAG
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

        rtable_chunks = np.array_split(rtable, n_rtable_chunks)

        # Construct the DAG for probing
        probe_result = []
        start_row_id = 0
        for i in range(len(rtable_chunks)):
            result = delayed(self.probe)(rtable_chunks[i][r_overlap_attr],
                                         inverted_index, start_row_id, rem_stop_words,
                                         tokenizer, overlap_size)
            probe_result.append(result)
            start_row_id += len(rtable_chunks[i])
        probe_result = delayed(wrap)(probe_result)

        # Execute the DAG for probing
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
        fk_ltable = ltable.iloc[tmp.fk_ltable_rid][l_key].values
        fk_rtable = rtable.iloc[tmp.fk_rtable_rid][r_key].values
        id_vals = list(range(len(flat_list)))

        candset = pd.DataFrame.from_dict(
            {'_id': id_vals, l_output_prefix+l_key: fk_ltable, r_output_prefix+r_key: fk_rtable})


        # set the properties for the candidate set
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

        # Return the final candidate set to user.
        return ret_candset


    def block_candset(self, candset, l_overlap_attr, r_overlap_attr,
                      rem_stop_words=False, q_val=None, word_level=True,
                      overlap_size=1, allow_missing=False,
                      verbose=False, show_progress=True, n_chunks=-1):

        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.
        
        Blocks an input candidate set of tuple pairs based on the overlap
        of token sets of attribute values. Finds tuple pairs from an input 
        candidate set of tuple pairs such that
        the overlap between (a) the set of tokens obtained by tokenizing the
        value of attribute l_overlap_attr of the left tuple in a tuple pair,
        and (b) the set of tokens obtained by tokenizing the value of
        attribute r_overlap_attr of the right tuple in the tuple pair,
        is above a certain threshold.

        Args:
            candset (DataFrame): The input candidate set of tuple pairs.

            l_overlap_attr (string): The overlap attribute in left table.

            r_overlap_attr (string): The overlap attribute in right table.

            rem_stop_words (boolean): A flag to indicate whether stop words
                                      (e.g., a, an, the) should be removed
                                      from the token sets of the overlap
                                      attribute values (defaults to False).

            q_val (int): The value of q to use if the overlap attributes values
                         are to be tokenized as qgrams (defaults to None).

            word_level (boolean): A flag to indicate whether the overlap
                                  attributes should be tokenized as words
                                  (i.e, using whitespace as delimiter)
                                  (defaults to True).

            overlap_size (int): The minimum number of tokens that must overlap
                                (defaults to 1).

            allow_missing (boolean): A flag to indicate whether tuple pairs
                                     with missing value in at least one of the
                                     blocking attributes should be included in
                                     the output candidate set (defaults to
                                     False). If this flag is set to True, a
                                     tuple pair with missing value in either
                                     blocking attribute will be retained in the
                                     output candidate set.

            verbose (boolean): A flag to indicate whether the debug information 
                should be logged (defaults to False).

            show_progress (boolean): A flag to indicate whether progress should
                                     be displayed to the user (defaults to True).

            n_chunks (int): The number of partitions to split the candidate set. If it 
                            is set to -1, the number of partitions will be set to the 
                            number of cores in the machine.  

        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).

        Raises:
            AssertionError: If `candset` is not of type pandas
                DataFrame.
            AssertionError: If `l_overlap_attr` is not of type string.
            AssertionError: If `r_overlap_attr` is not of type string.
            AssertionError: If `q_val` is not of type int.
            AssertionError: If `word_level` is not of type boolean.
            AssertionError: If `overlap_size` is not of type int.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `allow_missing` is not of type boolean.
            AssertionError: If `show_progress` is not of type
                boolean.
            AssertionError: If `n_chunks` is not of type
                int.
            AssertionError: If `l_overlap_attr` is not in the ltable
                columns.
            AssertionError: If `r_block_attr` is not in the rtable columns.
            SyntaxError: If `q_val` is set to a valid value and
                `word_level` is set to True.
            SyntaxError: If `q_val` is set to None and
                `word_level` is set to False.
        Examples:
            >>> import py_entitymatching as em
            >>> from py_entitymatching.dask.dask_overlap_blocker import DaskOverlapBlocker
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ob = DaskOverlapBlocker()
            >>> C = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'])

            >>> D1 = ob.block_candset(C, 'name', 'name', allow_missing=True)
            # Include all possible tuple pairs with missing values
            >>> D2 = ob.block_candset(C, 'name', 'name', allow_missing=True)
            # Execute blocking using multiple cores
            >>> D3 = ob.block_candset(C, 'name', 'name', n_chunks=-1)
            # Use q-gram tokenizer
            >>> D2 = ob.block_candset(C, 'name', 'name', word_level=False, q_val=2)


        """
        logger.warning(
            "WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
            "RISK.")

        # Validate input parameters
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

        # validate number of chunks
        validate_object_type(n_chunks, int, 'Parameter n_chunks')
        validate_chunks(n_chunks)


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


        n_chunks = get_num_partitions(n_chunks, len(candset))
        c_splits = np.array_split(candset, n_chunks)
        valid_splits = []

        # Create DAG
        for i in range(n_chunks):
            result = delayed(self._block_candset_split)(c_splits[i], l_df, r_df, l_key,
                                                       r_key, l_overlap_attr,
                                                       r_overlap_attr, fk_ltable,
                                                       fk_rtable, allow_missing,
                                                       rem_stop_words, tokenizer, overlap_size)
            valid_splits.append(result)
        valid_splits = delayed(wrap)(valid_splits)

        # Execute the DAG
        if show_progress:
            with ProgressBar():
                valid_splits = valid_splits.compute(scheduler="processes",
                                                    num_workers=get_num_cores())
        else:
            valid_splits = valid_splits.compute(scheduler="processes",
                                                num_workers=get_num_cores())

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

    # ------------ helper functions ------- #

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
                l_dict[row_lkey] = l_df.loc[row_lkey, l_block_attr]
            l_val = l_dict[row_lkey]

            row_rkey = row[rkey_idx]
            if row_rkey not in r_dict:
                r_dict[row_rkey] = r_df.loc[row_rkey, r_block_attr]
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
                if overlap >= overlap_threshold:
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

    def block_tuples(self, ltuple, rtuple, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1, allow_missing=False):
        """Blocks a tuple pair based on the overlap of token sets of attribute values.

        Args:
            ltuple (Series): The input left tuple.

            rtuple (Series): The input right tuple.

            l_overlap_attr (string): The overlap attribute in left tuple.

            r_overlap_attr (string): The overlap attribute in right tuple.

            rem_stop_words (boolean): A flag to indicate whether stop words
                                      (e.g., a, an, the) should be removed
                                      from the token sets of the overlap
                                      attribute values (defaults to False).

            q_val (int): A value of q to use if the overlap attributes values
                         are to be tokenized as qgrams (defaults to None).

            word_level (boolean): A flag to indicate whether the overlap
                                  attributes should be tokenized as words
                                  (i.e, using whitespace as delimiter)
                                  (defaults to True).

            overlap_size (int): The minimum number of tokens that must overlap
                                (defaults to 1).

            allow_missing (boolean): A flag to indicate whether a tuple pair
                                     with missing value in at least one of the
                                     blocking attributes should be blocked
                                     (defaults to False). If this flag is set
                                     to True, the pair will be kept if either
                                     ltuple has missing value in l_block_attr
                                     or rtuple has missing value in r_block_attr
                                     or both.

        Returns:
            A status indicating if the tuple pair is blocked (boolean).

        Examples:
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ob = em.OverlapBlocker()
            >>> status = ob.block_tuples(A.loc[0], B.loc[0], 'address', 'address')

        """

        # validate data types of input parameters specific to overlap blocker
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val,
                                         word_level, overlap_size)

        # validate word_level and q_val
        self.validate_word_level_qval(word_level, q_val)

        # determine which tokenizer to use
        if word_level == True:
            # # create a whitespace tokenizer
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            # # create a qgram tokenizer
            tokenizer = QgramTokenizer(qval=q_val, return_set=True)

        # # cleanup the tuples from non-ascii characters, punctuations, and stop words
        l_val = self.cleanup_tuple_val(ltuple[l_overlap_attr], rem_stop_words)
        r_val = self.cleanup_tuple_val(rtuple[r_overlap_attr], rem_stop_words)

        # create a filter for overlap similarity
        overlap_filter = OverlapFilter(tokenizer, overlap_size,
                                       allow_missing=allow_missing)

        return overlap_filter.filter_pair(l_val, r_val)

    def cleanup_tuple_val(self, val, rem_stop_words):
        if pd.isnull(val):
            return val

        return self.process_string(val, rem_stop_words)


    def process_string(self, input_string, rem_stop_words):
        if not input_string:
            return input_string

        if isinstance(input_string, bytes):
            input_string = input_string.decode('utf-8', 'ignore')
        input_string = input_string.lower()

        input_string = self.rem_punctuations(input_string)

        # remove stopwords
        # chop the attribute values and convert into a set
        val_chopped = list(set(input_string.strip().split()))

        # remove stop words
        if rem_stop_words:
            val_chopped_no_stopwords = self.rem_stopwords(val_chopped)
            val_joined = ' '.join(val_chopped_no_stopwords)
        else:
            val_joined = ' '.join(val_chopped)

        return val_joined

    def rem_punctuations(self, s):
        return self.regex_punctuation.sub('', s)



    def rem_stopwords(self, lst):
        return [t for t in lst if t not in self.stop_words]
