import logging

import sys
import pandas as pd
import numpy as np
import pyprind
import six
import warnings
from joblib import Parallel, delayed
from collections import deque, OrderedDict
import heapq
from itertools import chain

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.blocker.blocker import Blocker
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from py_entitymatching.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)


class SortedNeighborhoodBlocker(Blocker):
    """
    WARNING: THIS IS AN EXPERIMENTAL CLASS. THIS CLASS IS NOT TESTED. 
    USE AT YOUR OWN RISK.

    Blocks based on the sorted neighborhood blocking method
    """

    def __init__(self):
        # display warning message upon object initialization
        print("WARNING: THIS IS AN EXPERIMENTAL COMMAND. THIS COMMAND IS NOT TESTED. USE AT YOUR OWN RISK.")

    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr, window_size=2,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     allow_missing=False, verbose=False, n_jobs=1):
        """
        WARNING: THIS IS AN EXPERIMENTAL COMMAND. THIS COMMAND IS NOT TESTED. 
        USE AT YOUR OWN RISK.

        Blocks two tables based on sorted neighborhood.

        Finds tuple pairs from left and right tables such that when each table
        is sorted based upon a blocking attribute, tuple pairs are within a
        distance w of each other. The blocking attribute is created prior to calling
        this function.

        Args:
            ltable (DataFrame): The left input table.

            rtable (DataFrame): The right input table.

            l_block_attr (string): The blocking attribute for left table.

            r_block_attr (string): The blocking attribute for right table.

            window_size (int): size of sliding window. Defaults to 2

            l_output_attrs (list): A list of attribute names from the left
                                   table to be included in the
                                   output candidate set (defaults to None).

            r_output_attrs (list): A list of attribute names from the right
                                   table to be included in the
                                   output candidate set (defaults to None).

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

            verbose (boolean): A flag to indicate whether the debug information
                should be logged (defaults to False).


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
            AssertionError: If `l_block_attr` is not of type string.
            AssertionError: If `r_block_attr` is not of type string.
            AssertionError: If `window_size` is not of type of
                int or if window_size < 2.
            AssertionError: If the values in `l_output_attrs` is not of type
                string.
            AssertionError: If the values in `r_output_attrs` is not of type
                string.
            AssertionError: If `l_output_prefix` is not of type
                string.
            AssertionError: If `r_output_prefix` is not of type
                string.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `allow_missing` is not of type boolean.
            AssertionError: If `n_jobs` is not of type
                int.
            AssertionError: If `l_block_attr` is not in the ltable columns.
            AssertionError: If `r_block_attr` is not in the rtable columns.
            AssertionError: If `l_out_attrs` are not in the ltable.
            AssertionError: If `r_out_attrs` are not in the rtable.

        """

        # Warning that this code is still in alpha stage
        # display warning message
        print("WARNING: THIS IS AN EXPERIMENTAL COMMAND. THIS COMMAND IS NOT TESTED. USE AT YOUR OWN RISK.")

        # validate data types of input parameters
        self.validate_types_params_tables(ltable, rtable,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix,
                                          r_output_prefix, verbose, n_jobs)

        # validate data types of input blocking attributes
        self.validate_types_block_attrs(l_block_attr, r_block_attr)

        # validate data type of allow_missing
        self.validate_allow_missing(allow_missing)

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs,
                                   r_output_attrs)

        # get and validate required metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # check if ltable or rtable are empty.
        if ltable.empty:
            raise AssertionError('Left table is empty')
        if rtable.empty:
            raise AssertionError('Right table is empty')

        # check if window_size < 2
        if window_size < 2:
            raise AssertionError(
                'window_size is < 2')


        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger,
                                                     verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger,
                                        verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger,
                                        verbose)

        # do blocking
        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, min(len(ltable), len(rtable)))

        # handle potential missing values
        c_missing = pd.DataFrame()

        if n_procs <= 1:
            # single process
            c_splits, c_missing = _sn_block_tables_split(ltable, rtable, l_key, r_key,
                                                         l_block_attr, r_block_attr,
                                                         l_output_attrs, r_output_attrs,
                                                         allow_missing)
        else:
            # multiprocessing
            # Split l and r into n_procs chunks.
            # each core will get an l and an r, merge them, sort them.

            l_splits = np.array_split(ltable, n_procs)
            r_splits = np.array_split(rtable, n_procs)

            p_answer = Parallel(n_jobs=n_procs)(
                delayed(_sn_block_tables_split)(l_splits[i], r_splits[i], l_key, r_key,
                                                l_block_attr, r_block_attr,
                                                l_output_attrs, r_output_attrs,
                                                allow_missing)
                for i in range(n_procs))

            c_splits, c_missing = zip(*p_answer)
            c_splits = list(c_splits)
            c_missing = pd.concat(c_missing)

        # make a deque for the sliding window
        sliding_window = deque()
        result = []

        c_missing = c_missing.to_dict(orient='records')

        # Use generator function to merge sorted runs.
        # If single core, generator is trivial (see fn below)

        for row in _gen_iter_merge(c_splits):
            row = row._asdict()

            # if the sliding window is full, remove the largest.  The new tuple will be
            #   compared against the (window_size-1) previously seen tuples.
            # (if at the beginning just compare with whatever we have)
            if len(sliding_window) >= window_size:
                sliding_window.popleft()

            # Now, iterate over the sliding window (plus any tuples missing BKV's,
            #   if that was called for):
            for window_element in chain(sliding_window, c_missing):
                ltable = window_element
                rtable = row

                # SN blocking is often implemented on a single table.
                # In this implementation, we are only considering tuples that have
                #   one tuple from the left table and one tuple from the right table.
                # Thus, only keep candidates that span both tables.
                # However, the restriction is that matches need to be (left, right) so
                #   if we end up with (right, left) flip it.

                if ltable["source"] != rtable["source"]: # Span both tables
                    if ltable["source"] == 'r': # Left is right, so flip it to make it sane again
                        ltable, rtable = rtable, ltable

                    merged = OrderedDict()
                    merged[l_output_prefix+"ID"] = ltable[l_key]
                    merged[r_output_prefix+"ID"] = rtable[r_key]
                    merged[l_output_prefix+l_key] = ltable[l_key]
                    merged[r_output_prefix+r_key] = rtable[r_key]

                    # # add l/r output attributes to the ordered dictionary
                    if l_output_attrs is not None:
                        for attr in l_output_attrs:
                            merged[l_output_prefix + attr] = ltable[attr]
                    if r_output_attrs is not None:
                        for attr in r_output_attrs:
                            merged[r_output_prefix + attr] = rtable[attr]

                    # # add the ordered dict to the list
                    result.append(merged)

            sliding_window.append(row)
        candset = pd.DataFrame(result, columns=result[0].keys())


        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)

        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        return candset

    @staticmethod
    def block_candset(*args, **kwargs):
        """block_candset does not apply to sn_blocker, return unimplemented"""

        #  It isn't clear what SN on a candidate set would mean, throw an Assersion error
        raise AssertionError('unimplemented')

    @staticmethod
    def block_tuples(*args, **kwargs):
        """block_tuples does not apply to sn_blocker, return unimplemented"""

        #  It also isn't clear what SN on a tuple pair would mean, throw an Assersion error
        raise AssertionError('unimplemented')


    # ------------------------------------------------------------
    # utility functions specific to sorted neighborhood blocking

    # validate the data types of the blocking attributes
    @staticmethod
    def validate_types_block_attrs(l_block_attr, r_block_attr):
        """validate the data types of the blocking attributes"""

        if not isinstance(l_block_attr, six.string_types):
            logger.error(
                'Blocking attribute name of left table is not of type string')
            raise AssertionError(
                'Blocking attribute name of left table is not of type string')

        if not isinstance(r_block_attr, six.string_types):
            logger.error(
                'Blocking attribute name of right table is not of type string')
            raise AssertionError(
                'Blocking attribute name of right table is not of type string')

    # validate the blocking attributes
    @staticmethod
    def validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr):
        """validate the blocking attributes"""
        if l_block_attr not in ltable.columns:
            raise AssertionError(
                'Left block attribute is not in the left table')

        if r_block_attr not in rtable.columns:
            raise AssertionError(
                'Right block attribute is not in the right table')

def _get_attrs_to_project(key, output_attrs):
    proj_attrs = []
    if output_attrs is not None:
        proj_attrs.extend(output_attrs)
    if key not in proj_attrs:
        proj_attrs.append(key)
    return proj_attrs

def _sn_block_tables_split(ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                           l_output_attrs, r_output_attrs, allow_missing):

    #project the key + output attributes
    #copy bkv column from each table into a common column, named BKV__
    #combine tables
    #sort on BKV__

    lconv = pd.DataFrame()
    rconv = pd.DataFrame()

    if l_output_attrs:
        lconv = ltable[[l_key] + l_output_attrs]
    else:
        lconv = ltable[[l_key]]
    lconv = lconv.copy() # Make a full copy
    lconv.loc[:, 'source'] = 'l'
    lconv.loc[:, 'BKV__'] = ltable[l_block_attr]
    if r_output_attrs:
        rconv = rtable[[r_key] + r_output_attrs]
    else:
        rconv = rtable[[r_key]]
    rconv = rconv.copy() # Make a full copy
    rconv.loc[:, 'source'] = 'r'
    rconv.loc[:, 'BKV__'] = rtable[r_block_attr]

    # Now, if allow_missing=True, yank out "missing" values
    lmissing = pd.DataFrame()
    rmissing = pd.DataFrame()

    if allow_missing:
        lmissing = lconv[pd.isnull(lconv['BKV__'])]
        rmissing = rconv[pd.isnull(rconv['BKV__'])]
        lconv = lconv[pd.notnull(lconv['BKV__'])]
        rconv = rconv[pd.notnull(rconv['BKV__'])]

    return pd.concat([lconv, rconv]).sort_values(by='BKV__'), pd.concat([lmissing, rmissing])


def _prefix_columns(col_names, output_prefix):
    cols = []
    for col_name in col_names:
        cols.append(output_prefix + str(col_name))
    return cols



def _output_columns(l_key, r_key, col_names, l_output_attrs, r_output_attrs,
                    l_output_prefix, r_output_prefix):
    # retain id columns from merge
    ret_cols = [_retain_names(l_key, col_names, '_ltable')]
    ret_cols.append(_retain_names(r_key, col_names, '_rtable'))

    # final columns in the output
    fin_cols = [_final_names(l_key, l_output_prefix)]
    fin_cols.append(_final_names(r_key, r_output_prefix))

    # retain output attrs from merge
    if l_output_attrs:
        for attr in l_output_attrs:
            if attr != l_key:
                ret_cols.append(_retain_names(attr, col_names, '_ltable'))
                fin_cols.append(_final_names(attr, l_output_prefix))

    if r_output_attrs:
        for attr in r_output_attrs:
            if attr != r_key:
                ret_cols.append(_retain_names(attr, col_names, '_rtable'))
                fin_cols.append(_final_names(attr, r_output_prefix))

    return ret_cols, fin_cols


def _retain_names(the_name, col_names, suffix):
    if the_name in col_names:
        return the_name
    else:
        return str(the_name) + suffix


def _final_names(col, prefix):
    return prefix + str(col)

def _gen_iter_merge(*things):
    """
    Input: either:
           a. a list of iters, where each iter is a sorted list of items.
           b. a list of sorted items
    Output: iter-wise output

    This is used to merge the output from either (a) the multicore or (b) the single core case.
    """

    # If this is one thing, just return the contents (single core)
    if not isinstance(things[0], list):
        for value in things[0].itertuples(index=False):
            yield value
    else:
        # We will have a list of iters, iter_list
        # make heapq for heads of each entry of iter_list
        # pull from min from heapq for smallest of all iter_list
        # refill deque if the particular thing it came from, if not empty

        iter_list = []
        # Priority queue for the heapq
        priority_queue = []
        value_offset = 0

        # load the heads of the queues into the priority_queue
        for value in things[0]:
            this_iter = value.itertuples(index=True)
            try:
                next_value = next(this_iter)
                next_bkv = next_value.BKV__
                thing_for_heap = (next_bkv, next_value, value_offset)
                heapq.heappush(priority_queue, (thing_for_heap))
            except:
                raise
            # Append this iterator on the iter_list
            iter_list.append(this_iter)
            value_offset += 1

        while True:
            # If the priority_queue has gone to zero, we're done!
            if len(priority_queue) == 0:
                raise StopIteration

            # .... otherwise, take the next smallest thing off the priority_queue.
            try:
                this_tuple = heapq.heappop(priority_queue)
            except:
                raise

            #this_bkv = this_tuple[0]
            this_object = this_tuple[1]
            load_next = this_tuple[2]

            try:
                load_object = next(iter_list[load_next])
                heapq.heappush(priority_queue, (load_object.BKV__, load_object, load_next))
            except StopIteration:
                pass # Ignore the StopIteration
            yield this_object
