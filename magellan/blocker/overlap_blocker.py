# coding=utf-8
from collections import OrderedDict, Counter
import logging
import re
import string

import pandas as pd
import pyprind
import six

from magellan.blocker.blocker import Blocker
import magellan.catalog.catalog_manager as cm
from magellan.externals.py_stringmatching.tokenizers import qgram
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column

from magellan.externals.py_stringsimjoin.filter.overlap_filter import OverlapFilter
from magellan.externals.py_stringsimjoin.join.join import overlap_join
from magellan.externals.py_stringsimjoin.utils.tokenizers import create_qgram_tokenizer, create_delimiter_tokenizer

from magellan.utils.generic_helper import remove_non_ascii, rem_nan

from functools import partial

logger = logging.getLogger(__name__)


class OverlapBlocker(Blocker):
    def __init__(self):
        self.stop_words = ['a', 'an', 'and', 'are', 'as', 'at',
                           'be', 'by', 'for', 'from',
                           'has', 'he', 'in', 'is', 'it',
                           'its', 'on', 'that', 'the', 'to',
                           'was', 'were', 'will', 'with']
        self.regex_punctuation = re.compile('[%s]' % re.escape(string.punctuation))
        super(OverlapBlocker, self).__init__()

    def block_tables(self, ltable, rtable, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True, overlap_size=1,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, show_progress=True, n_jobs=1):

        # validate data types of standard input parameters
        self.validate_types_params_tables(ltable, rtable,
			    l_output_attrs, r_output_attrs, l_output_prefix,
			    r_output_prefix, verbose, n_jobs)

        # validate data types of input parameters specific to overlap blocker
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val,
                                         word_level, overlap_size)
 
        # validations
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr, r_overlap_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # required metadata; keys from ltable and rtable
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm.validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        if word_level == True and q_val != None:
            raise SyntaxError('Parameters word_level and q_val cannot be set together; Note that word_level is '
                              'set to True by default, so explicity set word_level=false to use qgram with the '
                              'specified q_val')

        if word_level == False and q_val == None:
            raise SyntaxError('Parameters word_level and q_val cannot be unset together; Note that q_val is '
                              'set to None by default, so if you want to use qgram then '
                              'explictiy specify set word_level=False and specify the q_val')

        # do blocking

        # # rem nans
        l_df = rem_nan(ltable, l_overlap_attr)
        r_df = rem_nan(rtable, r_overlap_attr)

        # # do projection before merge
        l_proj_attrs = self.get_attrs_to_project(l_key, l_overlap_attr, l_output_attrs)
        l_df = l_df[l_proj_attrs]
        r_proj_attrs = self.get_attrs_to_project(r_key, r_overlap_attr, r_output_attrs)
        r_df = r_df[r_proj_attrs]

        # # case the column to string if required.
        if l_df.dtypes[l_overlap_attr] != object:
            logger.warning('Left overlap attribute is not of type string; coverting to string temporarily')
            l_df[l_overlap_attr] = l_df[l_overlap_attr].astype(str)

        if r_df.dtypes[r_overlap_attr] != object:
            logger.warning('Right overlap attribute is not of type string; coverting to string temporarily')
            r_df[r_overlap_attr] = r_df[r_overlap_attr].astype(str)

        # # cleanup the tables from non-ascii characters, punctuations, and stop words
        self.cleanup_table(l_df, l_overlap_attr, rem_stop_words)
        self.cleanup_table(r_df, r_overlap_attr, rem_stop_words)

        tokenizer = None
        if word_level == True:
            tokenizer = create_delimiter_tokenizer()
        else:
            tokenizer = create_qgram_tokenizer(q_val)

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(r_df))
        if n_procs < 1:
            n_procs = 1 

        candset = overlap_join(l_df, r_df, l_key, r_key, l_overlap_attr, r_overlap_attr,
                     tokenizer, overlap_size, l_output_attrs, r_output_attrs,
                     l_output_prefix, r_output_prefix, out_sim_score=False,
                     n_jobs=n_procs)
        
        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs, r_output_attrs,
                                               l_output_prefix, r_output_prefix)
        candset = candset[retain_cols]

        # Update metadata in the catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key, r_output_prefix + r_key, ltable, rtable)

        # return the candidate set
        return candset

    def block_candset(self, candset, l_overlap_attr, r_overlap_attr,
                      rem_stop_words=False, q_val=None, word_level=True, overlap_size=1,
                      verbose=True, show_progress=True, n_jobs=1):

        # validate data types of standard input parameters
        self.validate_types_params_candset(candset, verbose, show_progress, n_jobs)

        # validate data types of input parameters specific to overlap blocker
        self.validate_types_other_params(l_overlap_attr, r_overlap_attr,
                                         rem_stop_words, q_val,
                                         word_level, overlap_size)

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                         'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)

        # validate overlap attrs
        self.validate_overlap_attrs(ltable, rtable, l_overlap_attr, r_overlap_attr)

        # do blocking

        # # rem nans
        l_df = rem_nan(ltable, l_overlap_attr)
        r_df = rem_nan(rtable, r_overlap_attr)

        # # do projection before merge
        l_df = l_df[[l_key, l_overlap_attr]]
        r_df = r_df[[r_key, r_overlap_attr]]

        # # case the column to string if required.
        if l_df.dtypes[l_overlap_attr] != object:
            logger.warning('Left overlap attribute is not of type string; coverting to string temporarily')
            l_df[l_overlap_attr] = l_df[l_overlap_attr].astype(str)

        if r_df.dtypes[r_overlap_attr] != object:
            logger.warning('Right overlap attribute is not of type string; coverting to string temporarily')
            r_df[r_overlap_attr] = r_df[r_overlap_attr].astype(str)

        # # cleanup the tables from non-ascii characters, punctuations, and stop words
        self.cleanup_table(l_df, l_overlap_attr, rem_stop_words)
        self.cleanup_table(r_df, r_overlap_attr, rem_stop_words)

        tokenizer = None
        if word_level == True:
            tokenizer = create_delimiter_tokenizer()
        else:
            tokenizer = create_qgram_tokenizer(q_val)       
        overlap_filter = OverlapFilter(tokenizer, overlap_size)

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(candset)) 
        if n_procs < 1:
            n_procs = 1
        out_table = overlap_filter.filter_candset(candset, fk_ltable, fk_rtable,
                                                  l_df, r_df, l_key, r_key,
                                                  l_overlap_attr, r_overlap_attr,
                                                  n_jobs=n_procs)
        # update catalog
        cm.set_candset_properties(out_table, key, fk_ltable, fk_rtable, ltable, rtable)

        # return candidate set
        return out_table

    def block_tuples(self, ltuple, rtuple, l_overlap_attr, r_overlap_attr,
                     rem_stop_words=False, q_val=None, word_level=True,
                     overlap_size=1):

        num_overlap = self.get_token_overlap_bt_two_tuples(ltuple, rtuple, l_overlap_attr, r_overlap_attr,
                                                           q_val, rem_stop_words)
        if num_overlap < overlap_size:
            return True
        else:
            return False

    # helper functions

    # validate the data types of input parameters specific to overlap blocker
    def validate_types_other_params(self, l_overlap_attr, r_overlap_attr,
                                    rem_stop_words, q_val,
                                    word_level, overlap_size):
        if not isinstance(l_overlap_attr, six.string_types):
            logger.error('Overlap attribute name of left table is not of type string')
            raise AssertionError('Overlap attribute name of left table is not of type string')
        if not isinstance(r_overlap_attr, six.string_types):
            logger.error('Overlap attribute name of right table is not of type string')
            raise AssertionError('Overlap attribute name of right table is not of type string')
        if not isinstance(rem_stop_words, bool):
            logger.error('Parameter rem_stop_words is not of type bool')
            raise AssertionError('Parameter rem_stop_words is not of type bool')
        if q_val != None and not isinstance(q_val, int):
            logger.error('Parameter q_val is not of type int')
            raise AssertionError('Parameter q_val is not of type int')
        if not isinstance(word_level, bool):
            logger.error('Parameter word_level is not of type bool')
            raise AssertionError('Parameter word_level is not of type bool')
        if not isinstance(overlap_size, int):
            logger.error('Parameter overlap_size is not of type int')
            raise AssertionError('Parameter overlap_size is not of type int')

    # validate the overlap attrs
    def validate_overlap_attrs(self, ltable, rtable, l_overlap_attr, r_overlap_attr):
        if not isinstance(l_overlap_attr, list):
            l_overlap_attr = [l_overlap_attr]
        assert set(l_overlap_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_overlap_attr, list):
            r_overlap_attr = [r_overlap_attr]
        assert set(r_overlap_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'

    def get_token_overlap_bt_two_tuples(self, l_tuple, r_tuple, l_overlap_attr, r_overlap_attr,
                                        q_val, rem_stop_words):
        l_val = l_tuple[l_overlap_attr]
        r_val = r_tuple[r_overlap_attr]

        if l_val == None and r_val == None:
            return 0

        if not isinstance(l_val, basestring):
            l_val = str(l_val)

        if not isinstance(r_val, basestring):
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
            chopped_vals = qgram(values, q_val)
        return list(set(chopped_vals))

    def cleanup_table(self, table, overlap_attr, rem_stop_words):

        # get overlap_attr column
        attr_col_values = table[overlap_attr]

        # remove non-ascii chars
        attr_col_values = [remove_non_ascii(val) for val in attr_col_values]

        # remove special characters
        attr_col_values = [self.rem_punctuations(val).lower() for val in attr_col_values]

        # chop the attribute values
        col_values_chopped = [val.split() for val in attr_col_values]

        # convert the chopped values into a set
        col_values_chopped = [list(set(val)) for val in col_values_chopped]

        # remove stop words
        if rem_stop_words == True:
            col_values_chopped = [self.rem_stopwords(val) for val in col_values_chopped]

        values = [' '.join(val) for val in col_values_chopped]

        table[overlap_attr] = values

    def rem_punctuations(self, s):
        return self.regex_punctuation.sub('', s)

    def rem_stopwords(self, lst):
        return [t for t in lst if t not in self.stop_words]
