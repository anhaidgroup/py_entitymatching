import logging

import pandas as pd
import pyprind
import multiprocessing
from joblib import Parallel, delayed

from magellan.blocker.blocker import Blocker
import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from magellan.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)

def block_tables_per_process(l_df, r_df, l_key, r_key, l_block_attr, r_block_attr,
			     l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix):

    #t0 = time.time() 
    # use pandas merge to do equi join
    candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr,
		       suffixes=('_ltable', '_rtable'))
    #t1 = time.time()
    #print "Per process merge time: ", (t1 - t0)
    retain_cols, final_cols = output_columns_fn(l_key, r_key, list(candset.columns),
				  l_output_attrs, r_output_attrs, l_output_prefix,
				  r_output_prefix)

    print "candset attrs: ", list(candset.columns)   
    print "retain attrs: ", retain_cols  
    print "final_attrs: ", final_cols  
    candset = candset[retain_cols]
    candset.columns = final_cols
    return candset
 
def output_columns_fn(l_key, r_key, col_names, l_output_attrs,
                      r_output_attrs, l_output_prefix, r_output_prefix):

    # retain id columns from merge                                          
    ret_cols = [retain_names_fn(l_key, col_names, '_ltable')]
    ret_cols.append(retain_names_fn(r_key, col_names, '_rtable'))

    # final columns in the output                                           
    fin_cols = [final_names_fn(l_key, l_output_prefix)]
    fin_cols.append(final_names_fn(r_key, r_output_prefix))

    # retain output attrs from merge                                        
    if l_output_attrs:
        for at in l_output_attrs:
            if at != l_key:
                ret_cols.append(retain_names_fn(at, col_names, '_ltable'))
                fin_cols.append(final_names_fn(at, l_output_prefix))

    if r_output_attrs:
        for at in r_output_attrs:
            if at != r_key:
                ret_cols.append(retain_names_fn(at, col_names, '_rtable'))
                fin_cols.append(final_names_fn(at, r_output_prefix))

    return ret_cols, fin_cols

def get_attrs_to_project(key, block_attr, output_attrs):
    if not output_attrs:
        output_attrs = [];              
    if key not in output_attrs:                                             
        output_attrs.append(key)                                            
    if block_attr not in output_attrs:                                      
        output_attrs.append(block_attr)                                     
    return output_attrs

def retain_names_fn(x, col_names, suffix):
    if x in col_names:
        return x
    else:
        return str(x) + suffix

def final_names_fn(col, prefix):
    return prefix + str(col)

def retain_cols_fn(l_key, r_key, col_names, l_output_attrs, r_output_attrs):
    
    # retain id columns from merge
    ret_cols = [retain_names_fn(l_key, col_names, '_ltable')]
    ret_cols.append(retain_names_fn(r_key, col_names, '_rtable'))
    
    # retain output attrs from merge
    if l_output_attrs:
    	ret_cols.extend([retain_names_fn(x, col_names, '_ltable') for x in l_output_attrs if x != l_key])
    if r_output_attrs:
        ret_cols.extend([retain_names_fn(x, col_names, '_rtable') for x in r_output_attrs if x != r_key])
    return ret_cols


class AttrEquivalenceBlocker(Blocker):
    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, n_jobs=1):

        # validate data types of input parameters
        self.validate_types_tables(ltable, rtable, l_block_attr, r_block_attr,
			    l_output_attrs, r_output_attrs, l_output_prefix,
			    r_output_prefix, verbose)

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
        cm.validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm.validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # do blocking

        # # remove nans: should be modified based on missing data policy
        l_df, r_df = rem_nan(ltable, l_block_attr), rem_nan(rtable, r_block_attr)

        # # do projection before merge
        l_proj_attrs = self.get_proj_attrs(l_key, l_block_attr, l_output_attrs)
        l_df = l_df[l_proj_attrs]
        r_proj_attrs = self.get_proj_attrs(r_key, r_block_attr, r_output_attrs)
        r_df = r_df[r_proj_attrs]
        
        # # determine number of processes to launch parallely
        n_cpus = multiprocessing.cpu_count()
        if n_jobs >= 0:
            n_procs = n_jobs
        else:
            n_procs = n_cpus + 1 + n_jobs

        candset = None
        if n_procs <= 1:
            # single process
            candset = block_tables_per_process(l_df, r_df, l_key, r_key, l_block_attr,
					       r_block_attr,
					       l_output_attrs, r_output_attrs,
					       l_output_prefix, r_output_prefix)
        else:
            # multiprocessing
            #t0000 = time.time()
            m, n = self.get_split_params(n_procs)
            #print "n_procs: ", n_procs, "m: ", m, "n: ", n
            #t000 = time.time() 
            l_splits = pd.np.array_split(l_df, m)
            r_splits = pd.np.array_split(r_df, n)
            #t00 = time.time()
            c_splits = Parallel(n_jobs=n_procs)(delayed(block_tables_per_process)(l, r, l_key, r_key,
						l_block_attr, r_block_attr,
						l_output_attrs, r_output_attrs,
						l_output_prefix, r_output_prefix)
						for l in l_splits for r in r_splits)
            #t0 = time.time()
            candset = pd.concat(c_splits, ignore_index=True)
            #t1 = time.time()
            #print "Computing split params: ", (t000 - t0000)
            #print "Split time: ", (t00 - t000)
            #print "Merge time: ", (t0 - t00)
            #print "Concatenation time: ", (t1 - t0)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr, verbose=True,
                      show_progress=True):

        self.validate_types_candset(candset, l_block_attr, r_block_attr,
				    verbose, show_progress)
        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, ' +
                 'fk rtable, ltable, rtable, ltable key, rtable key',
                 verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable,
                                         ltable, rtable, l_key, r_key,
                                         logger, verbose)

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)

        # do blocking

        # # initialize progress bar
        if show_progress:
            prog_bar = pyprind.ProgBar(len(candset))

        # # initialize list to keep track of valid ids
        valid = []

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # get the indexes for the key attributes in the candset
        col_names = list(candset.columns)
        lkey_idx = col_names.index(fk_ltable)
        rkey_idx = col_names.index(fk_rtable)

        # # create a look up table for the blocking attribute values
        l_dict = {}
        r_dict = {}

        # # iterate the rows in candset
        for row in candset.itertuples(index=False):

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
        return ltuple[l_block_attr] != rtuple[r_block_attr]

    # -----------------------------------------------------

    # utility functions - seem to be specific to attribute equivalence blocking

    # validate the blocking attrs
    def validate_block_attrs(self, ltable, rtable, l_block_attr, r_block_attr):
        if not isinstance(l_block_attr, list):
            l_block_attr = [l_block_attr]
        assert set(l_block_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_block_attr, list):
            r_block_attr = [r_block_attr]
        assert set(r_block_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'

    def output_columns(self, l_key, r_key, col_names, l_output_attrs,
                       r_output_attrs, l_output_prefix, r_output_prefix):

        # retain id columns from merge                                          
        ret_cols = [self.retain_names(l_key, col_names, '_ltable')]
        ret_cols.append(self.retain_names(r_key, col_names, '_rtable'))

        # final columns in the output                                           
        fin_cols = [self.final_names(l_key, l_output_prefix)]
        fin_cols.append(self.final_names(r_key, r_output_prefix))

        # retain output attrs from merge                                        
        if l_output_attrs:
            for at in l_output_attrs:
                if at != l_key:
                    ret_cols.append(self.retain_names(at, col_names, '_ltable'))
                    fin_cols.append(self.final_names(at, l_output_prefix))

        if r_output_attrs:
            for at in r_output_attrs:
                if at != r_key:
                    ret_cols.append(self.retain_names(at, col_names, '_rtable'))
                    fin_cols.append(self.final_names(at, r_output_prefix))

        return ret_cols, fin_cols

    def retain_names(self, col, col_names, suffix):
        if col in col_names:
            return col
        else:
            return str(col) + suffix

    def final_names(self, col, prefix):
        return prefix + str(col)
