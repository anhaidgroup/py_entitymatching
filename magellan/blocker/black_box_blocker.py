from collections import OrderedDict
import logging
import time
import sys

import pandas as pd
import pyprind
from joblib import Parallel, delayed

from magellan.blocker.blocker import Blocker
import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column

logger = logging.getLogger(__name__)

class BlackBoxBlocker(Blocker):
    """Blocks two tables, a candset, or a pair of tuples based on a black box
       function specified by the user.
    """

    def __init__(self, *args, **kwargs):
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
                     verbose=False, show_progress=True, n_jobs=1):
        
        """Blocks two tables based on a black box blocking function specified
           by the user.

        Finds tuple pairs from left and right tables that survive the black
        box function. A tuple pair survives the black box blocking function if
        the function returns False for that pair, otherwise the tuple pair is
        dropped.

        Args:
            ltable (DataFrame): left input table.

            rtable (DataFrame): right input table.

            l_output_attrs (list): list of attribute names from the left
                                   table to be included in the
                                   output candidate set (defaults to None).

            r_output_attrs (list): list of attribute names from the right
                                   table to be included in the
                                   output candidate set (defaults to None).

            l_output_prefix (str): prefix to be used for the attribute names
                                   coming from the left table in the output
                                   candidate set (defaults to 'ltable\_').

            r_output_prefix (str): prefix to be used for the attribute names
                                   coming from the right table in the output
                                   candidate set (defaults to 'rtable\_').

            verbose (boolean): flag to indicate whether logging should be done
                               (defaults to False).

            show_progress (boolean): flag to indicate whether progress should
                                     be displayed to the user (defaults to True).

            n_jobs (int): number of parallel jobs to be used for computation
                          (defaults to 1).
                          If -1 all CPUs are used. If 0 or 1, no parallel computation
                          is used at all, which is useful for debugging.
                          For n_jobs below -1, (n_cpus + 1 + n_jobs) are used.
                          Thus, for n_jobs = -2, all CPUS but one are used.
                          If (n_cpus + 1 + n_jobs) is less than 1, then n_jobs is
                          set to 1, which means no parallel computation at all.

        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).
        """

        # validate data types of standard input parameters
        self.validate_types_params_tables(ltable, rtable,
			                  l_output_attrs, r_output_attrs,
                                          l_output_prefix, r_output_prefix,
                                          verbose, show_progress, n_jobs)

        # validate black box function
        assert self.black_box_function != None, 'Black box function is not set'

        # validate output attributes
        self.validate_output_attrs(ltable, rtable, l_output_attrs,r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

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

        # # determine the number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(l_df) * len(r_df))

        if n_procs <= 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
                                          l_output_attrs_1, r_output_attrs_1,
                                          l_output_prefix, r_output_prefix,
                                          self, show_progress)
        else:
            # multiprocessing
            m, n = self.get_split_params(n_procs, len(l_df), len(r_df))
            l_splits = pd.np.array_split(l_df, m)
            r_splits = pd.np.array_split(r_df, n)
            c_splits = Parallel(n_jobs=m*n)(delayed(_block_tables_split)(l_splits[i], r_splits[j],
                                                l_key, r_key, 
                                                l_output_attrs_1, r_output_attrs_1,
                                                l_output_prefix, r_output_prefix,
                                                self, show_progress and i == len(l_splits) - 1 and j == len(r_splits) - 1)
                                                for i in range(len(l_splits)) for j in range(len(r_splits)))
            candset = pd.concat(c_splits, ignore_index=True)

        # # determine the attributes to retain in the output candidate set
        retain_cols = self.get_attrs_to_retain(l_key, r_key,
                                               l_output_attrs, r_output_attrs,
                                               l_output_prefix, r_output_prefix)
        #print "Candset columns: ", candset.columns
        #print "Attributes to retain: ", retain_cols
        if len(candset) > 0:
            candset = candset[retain_cols]
        else:
            candset =pd.DataFrame(columns=retain_cols)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix+l_key,
                                  r_output_prefix+r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, verbose=True, show_progress=True, n_jobs=1):

        """Blocks an input candidate set of tuple pairs based on a black box
           blocking function specified by the user.

        Finds tuple pairs from an input candidate set of tuple pairs that
        survive the black box function. A tuple pair survives the black box
        blocking function if the function returns False for that pair,
        otherwise the tuple pair is dropped.

        Args:
            candset (DataFrame): input candidate set of tuple pairs.

            verbose (boolean): flag to indicate whether logging should be done
                               (defaults to False).

            show_progress (boolean): flag to indicate whether progress should
                                     be displayed to the user (defaults to True).

            n_jobs (int): number of parallel jobs to be used for computation
                          (defaults to 1).
                          If -1 all CPUs are used. If 0 or 1, no parallel computation
                          is used at all, which is useful for debugging.
                          For n_jobs below -1, (n_cpus + 1 + n_jobs) are used.
                          Thus, for n_jobs = -2, all CPUS but one are used.
                          If (n_cpus + 1 + n_jobs) is less than 1, then n_jobs is
                          set to 1, which means no parallel computation at all.

        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).
        """

        # validate data types of standard input parameters
        self.validate_types_params_candset(candset, verbose, show_progress, n_jobs)

        # validate black box functionn
        assert self.black_box_function != None, 'Black box function is not set'

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                         'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # do blocking

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # project candset to keep only the ID attributes
        c_df = candset[[key, fk_ltable, fk_rtable]]

        # # determine the number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(c_df))

        valid = []
        if n_procs <= 1:
            # single process
            valid = _block_candset_split(c_df, l_df, r_df, l_key, r_key,
                                         fk_ltable, fk_rtable, self, show_progress)
        else:
            # multiprocessing
            c_splits = pd.np.array_split(c_df, n_procs)
            valid_splits = Parallel(n_jobs=n_procs)(delayed(_block_candset_split)(c_splits[i],
                                                            l_df, r_df,
                                                            l_key, r_key,
                                                            fk_ltable, fk_rtable,
                                                            self, show_progress and i == len(c_splits) - 1)
                                                            for i in range(len(c_splits)))
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
        """Blocks a tuple pair based on a black box blocking function specified
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
        """

        # validate black box function
        assert self.black_box_function is not None, 'Black box function is not set'
        return self.black_box_function(ltuple, rtuple)


def _block_tables_split(l_df, r_df, l_key, r_key,
                        l_output_attrs, r_output_attrs,
                        l_output_prefix, r_output_prefix,
                        black_box_blocker, show_progress):

    # initialize progress bar
    if show_progress:
        bar = pyprind.ProgBar(len(l_df)*len(r_df))

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
            res = black_box_blocker.black_box_function(ltuple, rtuple)

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
                         black_box_blocker, show_progress):

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

    # iterate candidate set
    for row in c_df.itertuples(index=False):
        # # update progress bar
        if show_progress:
            bar.update()
        
        # # get ltuple, try dictionary first, then dataframe
        row_lkey = row[l_id_pos]
        if row_lkey not in l_dict:
            l_dict[row_lkey] = l_df.ix[row_lkey]
        ltuple = l_dict[row_lkey]

        # # get rtuple, try dictionary first, then dataframe
        row_rkey = row[r_id_pos]
        if row_rkey not in r_dict:
            r_dict[row_rkey] = r_df.ix[row_rkey]
        rtuple = r_dict[row_rkey]

        # # apply the black box function to the tuple pair
        res = black_box_blocker.black_box_function(ltuple, rtuple)
        if res != True:
            valid.append(True)
        else:
            valid.append(False)

    return valid
