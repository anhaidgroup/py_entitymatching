from collections import OrderedDict
import logging
import time
import sys

import pandas as pd
import numpy as np
import pyprind

import dask
from dask import delayed
from dask.diagnostics import ProgressBar

import cloudpickle as cp
import pickle

from py_entitymatching.blocker.blocker import Blocker
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column

from py_entitymatching.utils.validation_helper import validate_object_type
from py_entitymatching.dask.utils import validate_chunks, get_num_partitions, \
    get_num_cores, wrap

logger = logging.getLogger(__name__)


class DaskBlackBoxBlocker(Blocker):
    """
    WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE A0T YOUR OWN RISK.

    Blocks based on a black box function specified by the user.
    """

    def __init__(self, *args, **kwargs):
        logger.warning(
            "WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
            "RISK.")
        super(Blocker, self).__init__(*args, **kwargs)
        self.black_box_function = None

    def set_black_box_function(self, function):
        """Sets black box function to be used for blocking.
        
        Args:
            function (function): the black box function to be used for blocking .
        
        """
        self.black_box_function = function

    def block_tables(self, ltable, rtable,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=False, show_progress=True, n_ltable_chunks=1,
                     n_rtable_chunks=1):
        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.        

        Blocks two tables based on a black box blocking function specified
        by the user.
        Finds tuple pairs from left and right tables that survive the black
        box function. A tuple pair survives the black box blocking function if
        the function returns False for that pair, otherwise the tuple pair is
        dropped.
        
        Args:
            ltable (DataFrame): The left input table.
            rtable (DataFrame): The right input table.
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
            AssertionError: If `l_output_attrs` is not of type of
                list.
            AssertionError: If `r_output_attrs` is not of type of
                list.
            AssertionError: If values in `l_output_attrs` is not of type
                string.
            AssertionError: If values in `r_output_attrs` is not of type
                string.
            AssertionError: If `l_output_prefix` is not of type
                string.
            AssertionError: If `r_output_prefix` is not of type
                string.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `show_progress` is not of type boolean.
            AssertionError: If `n_ltable_chunks` is not of type
                int.
            AssertionError: If `n_rtable_chunks` is not of type
                int.
            AssertionError: If `l_out_attrs` are not in the ltable.
            AssertionError: If `r_out_attrs` are not in the rtable.
        Examples:
            >>> def match_last_name(ltuple, rtuple):
                # assume that there is a 'name' attribute in the input tables
                # and each value in it has two words
                l_last_name = ltuple['name'].split()[1]
                r_last_name = rtuple['name'].split()[1]
                if l_last_name != r_last_name:
                    return True
                else:
                    return False
            >>> import py_entitymatching as em
            >>> from py_entitymatching.dask.dask_black_box_blocker DaskBlackBoxBlocker
            >>> bb = DaskBlackBoxBlocker()
            >>> bb.set_black_box_function(match_last_name)
            >>> C = bb.block_tables(A, B, l_output_attrs=['name'], r_output_attrs=['name'] )
        """

        logger.warning(
            "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.")


        # validate data types of standard input parameters
        self.validate_types_params_tables(ltable, rtable,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix, r_output_prefix,
                                          verbose, 1)

        # validate data type of show_progress
        self.validate_show_progress(show_progress)

        # validate black box function
        assert self.black_box_function != None, 'Black box function is not set'

        # validate output attributes
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # validate number of ltable and rtable chunks
        validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
        validate_object_type(n_rtable_chunks, int, 'Parameter n_rtable_chunks')

        validate_chunks(n_ltable_chunks)
        validate_chunks(n_rtable_chunks)

        # # determine the number of chunks
        n_ltable_chunks = get_num_partitions(n_ltable_chunks, len(ltable))
        n_rtable_chunks = get_num_partitions(n_rtable_chunks, len(rtable))

        # do blocking

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # remove l_key from l_output_attrs and r_key from r_output_attrs
        l_output_attrs_1 = []
        if l_output_attrs:
            l_output_attrs_1 = [x for x in l_output_attrs if x != l_key]
        r_output_attrs_1 = []
        if r_output_attrs:
            r_output_attrs_1 = [x for x in r_output_attrs if x != r_key]



        # # pickle the black-box function before passing it as an arg to
        # # _block_tables_split to be executed by each child process
        black_box_function_pkl = cp.dumps(self.black_box_function)

        if n_ltable_chunks == 1 and n_rtable_chunks == 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
                                          l_output_attrs_1, r_output_attrs_1,
                                          l_output_prefix, r_output_prefix,
                                          black_box_function_pkl, show_progress)
        else:
            # multiprocessing
            l_splits = np.array_split(l_df, n_ltable_chunks)
            r_splits = np.array_split(r_df, n_rtable_chunks)

            c_splits = []
            for i in range(len(l_splits)):
                for j in range(len(r_splits)):
                    partial_result = delayed(_block_tables_split)(l_splits[i], r_splits[j],
                                             l_key, r_key,
                                             l_output_attrs_1, r_output_attrs_1,
                                             l_output_prefix, r_output_prefix,
                                             black_box_function_pkl, False)
                    c_splits.append(partial_result)
            c_splits = delayed(wrap)(c_splits)
            if show_progress:
                with ProgressBar():
                    c_splits = c_splits.compute(scheduler="processes", num_workers=get_num_cores())
            else:
                c_splits = c_splits.compute(scheduler="processes", num_workers=get_num_cores())

            candset = pd.concat(c_splits, ignore_index=True)

        # # determine the attributes to retain in the output candidate set
        retain_cols = self.get_attrs_to_retain(l_key, r_key,
                                               l_output_attrs, r_output_attrs,
                                               l_output_prefix, r_output_prefix)
        if len(candset) > 0:
            candset = candset[retain_cols]
        else:
            candset = pd.DataFrame(columns=retain_cols)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, verbose=True, show_progress=True, n_chunks=1):

        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.


        Blocks an input candidate set of tuple pairs based on a black box
        blocking function specified by the user.

        Finds tuple pairs from an input candidate set of tuple pairs that
        survive the black box function. A tuple pair survives the black box
        blocking function if the function returns False for that pair,
        otherwise the tuple pair is dropped.

        Args:
            candset (DataFrame): The input candidate set of tuple pairs.

            verbose (boolean): A flag to indicate whether logging should be done
                               (defaults to False).

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
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `n_chunks` is not of type
                int.
            AssertionError: If `show_progress` is not of type boolean.
            AssertionError: If `l_block_attr` is not in the ltable columns.
            AssertionError: If `r_block_attr` is not in the rtable columns.

        Examples:
            >>> def match_last_name(ltuple, rtuple):
                # assume that there is a 'name' attribute in the input tables
                # and each value in it has two words
                l_last_name = ltuple['name'].split()[1]
                r_last_name = rtuple['name'].split()[1]
                if l_last_name != r_last_name:
                    return True
                else:
                    return False
            >>> import py_entitymatching as em
            >>> from py_entitymatching.dask.dask_black_box_blocker import DaskBlackBoxBlocker
            >>> bb = DaskBlackBoxBlocker()
            >>> bb.set_black_box_function(match_last_name)
            >>> D = bb.block_candset(C) # C is an output from block_tables
        """

        logger.warning(
            "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.")


        # validate data types of standard input parameters
        self.validate_types_params_candset(candset, verbose, show_progress, n_chunks)

        # validate black box functionn
        assert self.black_box_function != None, 'Black box function is not set'

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                         'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(
            candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable,
                                          rtable, l_key, r_key,
                                          logger, verbose)

        validate_object_type(n_chunks, int, 'Parameter n_chunks')
        validate_chunks(n_chunks)
        # do blocking

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # project candset to keep only the ID attributes
        c_df = candset[[key, fk_ltable, fk_rtable]]

        # # determine the number of processes to launch parallely
        n_chunks = get_num_partitions(n_chunks, len(candset))

        # # pickle the black-box function before passing it as an arg to
        # # _block_candset_split to be executed by each child process
        black_box_function_pkl = cp.dumps(self.black_box_function)

        valid = []
        if n_chunks == 1:
            # single process
            valid = _block_candset_split(c_df, l_df, r_df, l_key, r_key,
                                         fk_ltable, fk_rtable,
                                         black_box_function_pkl, show_progress)
        else:
            # multiprocessing
            c_splits = np.array_split(c_df, n_chunks)

            valid_splits = []
            for i in range(len(c_splits)):
                partial_result =  delayed(_block_candset_split)(c_splits[i],
                                              l_df, r_df,
                                              l_key, r_key,
                                              fk_ltable, fk_rtable,
                                              black_box_function_pkl, False)
                valid_splits.append(partial_result)

            valid_splits = delayed(wrap)(valid_splits)
            if show_progress:
                with ProgressBar():
                    valid_splits = valid_splits.compute(scheduler="processes",
                                                        num_workers=get_num_cores())
            else:
                valid_splits = valid_splits.compute(scheduler="processes",
                                                    num_workers=get_num_cores())

            valid = sum(valid_splits, [])

        # construct output table
        if len(c_df) > 0:
            c_df = candset[valid]
        else:
            c_df = pd.DataFrame(columns=candset.columns)

        # update catalog
        cm.set_candset_properties(c_df, key, fk_ltable, fk_rtable, ltable, rtable)

        # return candidate set
        return c_df

    def block_tuples(self, ltuple, rtuple):
        """
        Blocks a tuple pair based on a black box blocking function specified
        by the user.

        Takes a tuple pair as input, applies the black box blocking function to
        it, and returns True (if the intention is to drop the pair) or False
        (if the intention is to keep the tuple pair).

        Args:
            ltuple (Series): input left tuple.

            rtuple (Series): input right tuple.

        Returns:
            A status indicating if the tuple pair should be dropped or kept,
            based on the black box blocking function (boolean).

        Examples:
            >>> def match_last_name(ltuple, rtuple):
                # assume that there is a 'name' attribute in the input tables
                # and each value in it has two words
                l_last_name = ltuple['name'].split()[1]
                r_last_name = rtuple['name'].split()[1]
                if l_last_name != r_last_name:
                    return True
                else:
                    return False
            >>> from py_entitymatching.dask.dask_black_box_blocker import DaskBlackBoxBlocker
            >>> bb = DaskBlackBoxBlocker()
            >>> bb.set_black_box_function(match_last_name)
            >>> status = bb.block_tuples(A.loc[0], B.loc[0]) # A, B are input tables.

        """

        # validate black box function
        assert self.black_box_function is not None, 'Black box function is not set'
        return self.black_box_function(ltuple, rtuple)


def _block_tables_split(l_df, r_df, l_key, r_key,
                        l_output_attrs, r_output_attrs,
                        l_output_prefix, r_output_prefix,
                        black_box_function_pkl, show_progress):
    # initialize progress bar
    if show_progress:
        bar = pyprind.ProgBar(len(l_df) * len(r_df))

    # create look up dictionaries for faster processing
    l_dict = {}
    for k, r in l_df.iterrows():
        l_dict[k] = r
    r_dict = {}
    for k, r in r_df.iterrows():
        r_dict[k] = r

    # get the position of the ID attribute in the tables
    l_id_pos = list(l_df.columns).index(l_key)
    r_id_pos = list(r_df.columns).index(r_key)

    # create candset column names for the ID attributes of the tables
    ltable_id = l_output_prefix + l_key
    rtable_id = r_output_prefix + r_key

    # list to keep the tuple pairs that survive blocking
    valid = []

    # unpickle the black box function
    black_box_function = pickle.loads(black_box_function_pkl)

    # iterate through the two tables
    for l_t in l_df.itertuples(index=False):
        # # get ltuple from the look up dictionary
        ltuple = l_dict[l_t[l_id_pos]]
        for r_t in r_df.itertuples(index=False):
            # # update the progress bar
            if show_progress:
                bar.update()

            # # get rtuple from the look up dictionary
            rtuple = r_dict[r_t[r_id_pos]]

            # # apply the black box function to the tuple pair
            res = black_box_function(ltuple, rtuple)

            if res != True:
                # # this tuple pair survives blocking

                # # an ordered dictionary to keep a surviving tuple pair
                d = OrderedDict()

                # # add ltable and rtable ids to an ordered dictionary
                d[ltable_id] = ltuple[l_key]
                d[rtable_id] = rtuple[r_key]

                # # add l/r output attributes to the ordered dictionary
                l_out = ltuple[l_output_attrs]
                l_out.index = l_output_prefix + l_out.index
                d.update(l_out)

                r_out = rtuple[r_output_attrs]
                r_out.index = r_output_prefix + r_out.index
                d.update(r_out)

                # # add the ordered dict to the list
                valid.append(d)

    # construct candidate set
    candset = pd.DataFrame(valid)

    return candset


def _block_candset_split(c_df, l_df, r_df, l_key, r_key, fk_ltable, fk_rtable,
                         black_box_function_pkl, show_progress):
    # initialize the progress bar
    if show_progress:
        bar = pyprind.ProgBar(len(c_df))

    # create lookup dictionaries for faster processing
    l_dict = {}
    r_dict = {}

    # list to keep track of valid ids
    valid = []

    # find positions of the ID attributes of the two tables in the candset
    l_id_pos = list(c_df.columns).index(fk_ltable)
    r_id_pos = list(c_df.columns).index(fk_rtable)

    # unpickle the black box function
    black_box_function = pickle.loads(black_box_function_pkl)

    # iterate candidate set
    for row in c_df.itertuples(index=False):
        # # update progress bar
        if show_progress:
            bar.update()

        # # get ltuple, try dictionary first, then dataframe
        row_lkey = row[l_id_pos]
        if row_lkey not in l_dict:
            l_dict[row_lkey] = l_df.loc[row_lkey]
        ltuple = l_dict[row_lkey]

        # # get rtuple, try dictionary first, then dataframe
        row_rkey = row[r_id_pos]
        if row_rkey not in r_dict:
            r_dict[row_rkey] = r_df.loc[row_rkey]
        rtuple = r_dict[row_rkey]

        # # apply the black box function to the tuple pair
        res = black_box_function(ltuple, rtuple)
        if res != True:
            valid.append(True)
        else:
            valid.append(False)

    return valid
