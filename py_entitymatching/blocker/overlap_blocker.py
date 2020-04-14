# coding=utf-8
import logging
import re
import string

import pandas as pd
import py_stringsimjoin as ssj
import six
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
from py_stringsimjoin.filter.overlap_filter import OverlapFilter
from py_stringsimjoin.join.overlap_join import overlap_join

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.blocker.blocker import Blocker
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column
from py_entitymatching.utils.generic_helper import remove_non_ascii
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


class OverlapBlocker(Blocker):
    """
    Blocks  based on the overlap of token sets of attribute values.
    """

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
                     allow_missing=False, verbose=False, show_progress=True,
                     n_jobs=1):
        """
        Blocks two tables based on the overlap of token sets of attribute
         values.

        Finds tuple pairs from left and right tables such that the overlap
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

            n_jobs (int): The number of parallel jobs to be used for computation
                (defaults to 1). If -1 all CPUs are used. If 0 or 1,
                no parallel computation is used at all, which is useful for
                debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
                used (where n_cpus is the total number of CPUs in the
                machine). Thus, for n_jobs = -2, all CPUs but one are used.
                If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
                computation is used (i.e., equivalent to the default).


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

            AssertionError: If `n_jobs` is not of type
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
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ob = em.OverlapBlocker()
            # Use word-level tokenizer
            >>> C1 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], word_level=True, overlap_size=1)
            # Use q-gram tokenizer
            >>> C2 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], word_level=False, q_val=2)
            # Include all possible missing values
            >>> C3 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], allow_missing=True)
            # Use all the cores in the machine
            >>> C3 = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'], n_jobs=-1)


        """

        # validate data types of standard input parameters
        self.validate_types_params_tables(ltable, rtable,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix,
                                          r_output_prefix, verbose, n_jobs)

        # validate data types of input parameters specific to overlap blocker
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val,
                                         word_level, overlap_size)

        # validate data type of allow_missing
        self.validate_allow_missing(allow_missing)

        # validate data type of show_progress
        self.validate_show_progress(show_progress)

        # validate overlap attributes
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr,
                                    r_overlap_attr)

        # validate output attributes
        self.validate_output_attrs(ltable, rtable, l_output_attrs,
                                   r_output_attrs)

        # get and validate required metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger,
                                                     verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger,
                                        verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger,
                                        verbose)

        # validate word_level and q_val
        self.validate_word_level_qval(word_level, q_val)

        # do blocking

        # # do projection before merge
        l_proj_attrs = self.get_attrs_to_project(l_key, l_overlap_attr,
                                                 l_output_attrs)
        l_df = ltable[l_proj_attrs]
        r_proj_attrs = self.get_attrs_to_project(r_key, r_overlap_attr,
                                                 r_output_attrs)
        r_df = rtable[r_proj_attrs]

        # # case the column to string if required.
        l_df.is_copy, r_df.is_copy = False, False  # to avoid setwithcopy warning
        ssj.dataframe_column_to_str(l_df, l_overlap_attr, inplace=True)
        ssj.dataframe_column_to_str(r_df, r_overlap_attr, inplace=True)



        # # cleanup the tables from non-ascii characters, punctuations, and stop words
        l_dummy_overlap_attr = '@#__xx__overlap_ltable__#@'
        r_dummy_overlap_attr = '@#__xx__overlap_rtable__#@'
        l_df[l_dummy_overlap_attr] = l_df[l_overlap_attr]
        r_df[r_dummy_overlap_attr] = r_df[r_overlap_attr]

        if not l_df.empty:
            self.cleanup_table(l_df, l_dummy_overlap_attr, rem_stop_words)
        if not r_df.empty:
            self.cleanup_table(r_df, r_dummy_overlap_attr, rem_stop_words)

        # # determine which tokenizer to use
        if word_level == True:
            # # # create a whitespace tokenizer
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            # # # create a qgram tokenizer 
            tokenizer = QgramTokenizer(qval=q_val, return_set=True)

        # # perform overlap similarity join
        candset = overlap_join(l_df, r_df, l_key, r_key, l_dummy_overlap_attr,
                               r_dummy_overlap_attr, tokenizer, overlap_size,
                               '>=',
                               allow_missing, l_output_attrs, r_output_attrs,
                               l_output_prefix, r_output_prefix, False, n_jobs,
                               show_progress)

        # # retain only the required attributes in the output candidate set 
        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs,
                                               r_output_attrs,
                                               l_output_prefix, r_output_prefix)
        candset = candset[retain_cols]

        # update metadata in the catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return the candidate set
        return candset

    def block_candset(self, candset, l_overlap_attr, r_overlap_attr,
                      rem_stop_words=False, q_val=None, word_level=True,
                      overlap_size=1, allow_missing=False,
                      verbose=False, show_progress=True, n_jobs=1):
        """Blocks an input candidate set of tuple pairs based on the overlap
           of token sets of attribute values.

        Finds tuple pairs from an input candidate set of tuple pairs such that
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

            n_jobs (int): The number of parallel jobs to be used for computation
                (defaults to 1). If -1 all CPUs are used. If 0 or 1,
                no parallel computation is used at all, which is useful for
                debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
                used (where n_cpus are the total number of CPUs in the
                machine).Thus, for n_jobs = -2, all CPUs but one are used.
                If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
                computation is used (i.e., equivalent to the default).

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
            AssertionError: If `n_jobs` is not of type
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
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ob = em.OverlapBlocker()
            >>> C = ob.block_tables(A, B, 'address', 'address', l_output_attrs=['name'], r_output_attrs=['name'])

            >>> D1 = ob.block_candset(C, 'name', 'name', allow_missing=True)
            # Include all possible tuple pairs with missing values
            >>> D2 = ob.block_candset(C, 'name', 'name', allow_missing=True)
            # Execute blocking using multiple cores
            >>> D3 = ob.block_candset(C, 'name', 'name', n_jobs=-1)
            # Use q-gram tokenizer
            >>> D2 = ob.block_candset(C, 'name', 'name', word_level=False, q_val=2)


        """

        # validate data types of standard input parameters
        self.validate_types_params_candset(candset, verbose, show_progress,
                                           n_jobs)

        # validate data types of input parameters specific to overlap blocker
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val,
                                         word_level, overlap_size)

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

        # do blocking

        # # do projection before merge
        l_df = ltable[[l_key, l_overlap_attr]]
        r_df = rtable[[r_key, r_overlap_attr]]

        # # case the overlap attribute to string if required.
        l_df.is_copy, r_df.is_copy = False, False  # to avoid setwithcopy warning
        ssj.dataframe_column_to_str(l_df, l_overlap_attr, inplace=True)
        ssj.dataframe_column_to_str(r_df, r_overlap_attr, inplace=True)

        # # cleanup the tables from non-ascii characters, punctuations, and stop words
        self.cleanup_table(l_df, l_overlap_attr, rem_stop_words)
        self.cleanup_table(r_df, r_overlap_attr, rem_stop_words)

        # # determine which tokenizer to use
        if word_level == True:
            # # # create a whitespace tokenizer
            tokenizer = WhitespaceTokenizer(return_set=True)
        else:
            # # # create a qgram tokenizer
            tokenizer = QgramTokenizer(qval=q_val, return_set=True)

        # # create a filter for overlap similarity join
        overlap_filter = OverlapFilter(tokenizer, overlap_size,
                                       allow_missing=allow_missing)

        # # perform overlap similarity filtering of the candset
        out_table = overlap_filter.filter_candset(candset, fk_ltable, fk_rtable,
                                                  l_df, r_df, l_key, r_key,
                                                  l_overlap_attr,
                                                  r_overlap_attr,
                                                  n_jobs,
                                                  show_progress=show_progress)
        # update catalog
        cm.set_candset_properties(out_table, key, fk_ltable, fk_rtable, ltable,
                                  rtable)

        # return candidate set
        return out_table

    def block_tuples(self, ltuple, rtuple, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1, allow_missing=False):
        """Blocks a tuple pair based on the overlap of token sets of attribute
           values.
        
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

    # helper functions

    # validate the data types of input parameters specific to overlap blocker
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





    # cleanup a table from non-ascii characters, punctuations and stop words
    def cleanup_table(self, table, overlap_attr, rem_stop_words):

        # get overlap_attr column
        attr_col_values = table[overlap_attr]

        values = []
        for val in attr_col_values:
            if pd.isnull(val):
                values.append(val)
            else:
                processed_val = self.process_string(val, rem_stop_words)
                values.append(processed_val)

        table.is_copy = False
        table[overlap_attr] = values

    # cleanup a tuple from non-ascii characters, punctuations and stop words
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

