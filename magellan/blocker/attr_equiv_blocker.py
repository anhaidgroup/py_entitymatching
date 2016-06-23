import logging

import pandas as pd
import six
import pyprind
from joblib import Parallel, delayed

from magellan.blocker.blocker import Blocker
import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from magellan.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)


class AttrEquivalenceBlocker(Blocker):
    """Blocks two tables, a candset, or a pair of tuples based on attribute equivalence.
    """

    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=False, show_progress=True, n_jobs=1):
        """Blocks two tables based on attribute equivalence.

        Finds tuple pairs from left and right tables such that the value of
        attribute l_block_attr of a tuple from the left table exactly matches
        the value of attribute r_block_attr of a tuple from the right table.
        This is similar to equi-join of two tables.

        Args:
            ltable (pandas dataframe): left input table.

            rtable (pandas dataframe): right input table.

            l_block_attr (string): blocking attribute in left table.

            r_block_attr (string): blocking attribute in right table. 

            l_output_attrs (list of strings): list of attributes from the left
                                              table to be included in the
                                              output candidate set (defaults to None).

            r_output_attrs (list of strings): list of attributes from the right
                                              table to be included in the
                                              output candidate set (defaults to None).

            l_output_prefix (string): prefix to be used for the attribute names
                                      coming from the left table in the output
                                      candidate set (defaults to 'ltable\_').

            r_output_prefix (string): prefix to be used for the attribute names
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
            A candidate set of tuple pairs that survived blocking (pandas dataframe).
        """

        # validate data types of input parameters
        self.validate_types_params_tables(ltable, rtable,
			    l_output_attrs, r_output_attrs, l_output_prefix,
			    r_output_prefix, verbose, show_progress, n_jobs)

        # validate data types of input blocking attributes
        self.validate_types_block_attrs(l_block_attr, r_block_attr)
 
        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs,
                                   r_output_attrs)

        # get and validate required metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger,
                                                     verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # do blocking

        # # remove nans: should be modified based on missing data policy
        l_df, r_df = rem_nan(ltable, l_block_attr), rem_nan(rtable, r_block_attr)

        # # do projection before merge
        l_proj_attrs = self.get_attrs_to_project(l_key, l_block_attr, l_output_attrs)
        l_df = l_df[l_proj_attrs]
        r_proj_attrs = self.get_attrs_to_project(r_key, r_block_attr, r_output_attrs)
        r_df = r_df[r_proj_attrs]
       
        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs) 
        candset = None
        if n_procs <= 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
					  l_block_attr, r_block_attr,
					  l_output_attrs, r_output_attrs,
					  l_output_prefix, r_output_prefix)
        else:
            # multiprocessing
            m, n = self.get_split_params(n_procs)
            # safeguard against very small tables
            m, n = min(m, len(l_df)), min(n, len(r_df))
            l_splits = pd.np.array_split(l_df, m)
            r_splits = pd.np.array_split(r_df, n)
            c_splits = Parallel(n_jobs=m*n)(delayed(_block_tables_split)(l, r, l_key, r_key,
						l_block_attr, r_block_attr,
						l_output_attrs, r_output_attrs,
						l_output_prefix, r_output_prefix)
						for l in l_splits for r in r_splits)
            candset = pd.concat(c_splits, ignore_index=True)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr, verbose=True,
                      show_progress=True, n_jobs=1):
        """Blocks an input candidate set of tuple pairs based on attribute equivalence.

        Finds tuple pairs from an input candidate set of tuple pairs
        such that the value of attribute l_block_attr of the left tuple in a
        tuple pair exactly matches the value of attribute r_block_attr of the 
        right tuple in the tuple pair.

        Args:
            candset (pandas dataframe): input candidate set of tuple pairs.

            l_block_attr (string): blocking attribute in left table.

            r_block_attr (string): blocking attribute in right table. 

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
            A candidate set of tuple pairs that survived blocking (pandas dataframe).
        """

        # validate data types of input parameters
        self.validate_types_params_candset(candset, verbose, show_progress, n_jobs)

        # validate data types of input blocking attributes
        self.validate_types_block_attrs(l_block_attr, r_block_attr)

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, ' +
                 'fk rtable, ltable, rtable, ltable key, rtable key',
                 verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable,
                                          ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)

        # do blocking

        # # remove nans: should be modified based on missing data policy
        l_df, r_df = rem_nan(ltable, l_block_attr), rem_nan(rtable, r_block_attr)

        # # do projection before merge
        l_df = l_df[[l_key, l_block_attr]]
        r_df = r_df[[r_key, r_block_attr]]
       
        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs) 

        valid = []
        if n_procs <= 1:
            # single process
            valid = _block_candset_split(candset, l_df, r_df, l_key, r_key,
					 l_block_attr, r_block_attr,
					 fk_ltable, fk_rtable, show_progress)
        else:
            # safeguard against very small candset
            n_procs = min(n_procs, len(candset))
            c_splits = pd.np.array_split(candset, n_procs)
            valid_splits = Parallel(n_jobs=n_procs)(delayed(_block_candset_split)(c,
	    						    l_df, r_df,
                                                            l_key, r_key,
	    						    l_block_attr, r_block_attr,
	    						    fk_ltable, fk_rtable, show_progress)
	    						    for c in c_splits)
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

    def block_tuples(self, ltuple, rtuple, l_block_attr, r_block_attr):
        """Blocks a tuple pair based on attribute equivalence.

        Args:
            ltuple (pandas series): input left tuple.

            rtuple (pandas series): input right tuple.
            
            l_block_attr (string): blocking attribute in left tuple.

            r_block_attr (string): blocking attribute in right tuple.

        Returns:
            A status indicating if the tuple pair is blocked, i.e., the values
            of l_block_attr in ltuple and r_block_attr in rtuple are different
            (boolean).
        """
        return ltuple[l_block_attr] != rtuple[r_block_attr]

    # ------------------------------------------------------------
    # utility functions specific to attribute equivalence blocking

    # validate the data types of the blocking attributes 
    def validate_types_block_attrs(self, l_block_attr, r_block_attr):
        if not isinstance(l_block_attr, six.string_types):
            logger.error('Blocking attribute name of left table is not of type string')
            raise AssertionError('Blocking attribute name of left table is not of type string')

        if not isinstance(r_block_attr, six.string_types):
            logger.error('Blocking attribute name of right table is not of type string')
            raise AssertionError('Blocking attribute name of right table is not of type string')

    # validate the blocking attributes
    def validate_block_attrs(self, ltable, rtable, l_block_attr, r_block_attr):
        if not isinstance(l_block_attr, list):
            l_block_attr = [l_block_attr]
        assert set(l_block_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_block_attr, list):
            r_block_attr = [r_block_attr]
        assert set(r_block_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'


def _block_tables_split(l_df, r_df, l_key, r_key, l_block_attr, r_block_attr,
			l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix):
    candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr,
		       suffixes=('_ltable', '_rtable'))
    retain_cols, final_cols = _output_columns(l_key, r_key, list(candset.columns),
				              l_output_attrs, r_output_attrs,
					      l_output_prefix, r_output_prefix)
    candset = candset[retain_cols]
    candset.columns = final_cols
    return candset

def _block_candset_split(c_df, l_df, r_df, l_key, r_key, l_block_attr, r_block_attr,
			 fk_ltable, fk_rtable, show_progress):

    # initialize progress bar
    if show_progress:
        prog_bar = pyprind.ProgBar(len(c_df))

    # initialize list to keep track of valid ids
    valid = []

    # get the indexes for the key attributes in the candset
    col_names = list(c_df.columns)
    lkey_idx = col_names.index(fk_ltable)
    rkey_idx = col_names.index(fk_rtable)

    # create a look up table for the blocking attribute values
    l_dict = {}
    r_dict = {}

    # iterate the rows in candset
    for row in c_df.itertuples(index=False):

        # # update the progress bar
        if show_progress:
            prog_bar.update()

        # # get the value of block attributes
        row_lkey = row[lkey_idx]
        if row_lkey not in l_dict:
            l_dict[row_lkey] = l_df.ix[row_lkey, l_block_attr]
        l_val = l_dict[row_lkey]

        row_rkey = row[rkey_idx]
        if row_rkey not in r_dict:
            r_dict[row_rkey] = r_df.ix[row_rkey, r_block_attr]
        r_val = r_dict[row_rkey]

        if l_val == r_val:
            valid.append(True)
        else:
            valid.append(False)

    return valid

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
        for at in l_output_attrs:
            if at != l_key:
                ret_cols.append(_retain_names(at, col_names, '_ltable'))
                fin_cols.append(_final_names(at, l_output_prefix))

    if r_output_attrs:
        for at in r_output_attrs:
            if at != r_key:
                ret_cols.append(_retain_names(at, col_names, '_rtable'))
                fin_cols.append(_final_names(at, r_output_prefix))

    return ret_cols, fin_cols

def _retain_names(x, col_names, suffix):
    if x in col_names:
        return x
    else:
        return str(x) + suffix

def _final_names(col, prefix):
    return prefix + str(col)
