import logging


import pandas as pd
import numpy as np
import pyprind

from magellan.blocker.blocker import Blocker
import magellan.core.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from magellan.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)


class AttrEquivalenceBlocker(Blocker):
    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True):

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # get and validate required metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

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

        # # use pandas merge to do equi join
        candset = pd.merge(l_df, r_df, left_on=l_block_attr, right_on=r_block_attr, suffixes=('_ltable', '_rtable'))

        # construct output table
        retain_cols, final_cols = self.output_columns(l_key, r_key, list(candset.columns),
                                                      l_output_attrs, r_output_attrs,
                                                      l_output_prefix, r_output_prefix)
        candset = candset[retain_cols]
        candset.columns = final_cols

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix+l_key, r_output_prefix+r_key, ltable, rtable)

        # return candidate set
        return candset



    def block_candset(self, candset, l_block_attr, r_block_attr, verbose=True, show_progress=True):

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                                'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)

        # do blocking

        # # initialize progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(candset))

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
                bar.update()

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
        cm.set_candset_properties(out_table, key, fk_ltable, fk_rtable, ltable, rtable)

        # return the output table
        return out_table


    def block_tuples(self, ltuple, rtuple, l_block_attr, r_block_attr):
        return ltuple[l_block_attr] != rtuple[r_block_attr]
















    # -----------------------------------------------------
    # utility functions -- this function seems to be specific to attribute equivalence blocking

    # validate the blocking attrs
    def validate_block_attrs(self, ltable, rtable, l_block_attr, r_block_attr):
        if not isinstance(l_block_attr, list):
            l_block_attr = [l_block_attr]
        assert set(l_block_attr).issubset(ltable.columns) is True, 'Left block attribute is not in the left table'

        if not isinstance(r_block_attr, list):
            r_block_attr = [r_block_attr]
        assert set(r_block_attr).issubset(rtable.columns) is True, 'Right block attribute is not in the right table'

    def output_columns(self, l_key, r_key, col_names, l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix):
                                                                                
        # retain id columns from merge                                          
        ret_cols = [self.retain_names(l_key, col_names, '_ltable')]             
        ret_cols.append(self.retain_names(r_key, col_names, '_rtable'))         
                                                                                
        # final columns in the output                                           
        fin_cols = [self.final_names(l_key, l_output_prefix)]                   
        fin_cols.append(self.final_names(r_key, r_output_prefix))               
                                                                                
        # retain output attrs from merge                                        
        if l_output_attrs:                                                      
            for x in l_output_attrs:                                            
                if x != l_key:                                                  
                    ret_cols.append(self.retain_names(x, col_names, '_ltable')) 
                    fin_cols.append(self.final_names(x, l_output_prefix))       
                                                                                
        if r_output_attrs:                                                      
            for x in r_output_attrs:                                            
                if x != r_key:                                                  
                    ret_cols.append(self.retain_names(x, col_names, '_rtable')) 
                    fin_cols.append(self.final_names(x, r_output_prefix))       
                                                                                
        return ret_cols, fin_cols

    def retain_names(self, x, col_names, suffix):
        if x in col_names:
            return x
        else:
            return str(x) + suffix

    def final_names(self, x, prefix):
        return prefix + str(x)
