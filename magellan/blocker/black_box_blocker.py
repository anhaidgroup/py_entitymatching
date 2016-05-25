from collections import OrderedDict
import logging


import pandas as pd
import pyprind

from magellan.blocker.blocker import Blocker
import magellan.core.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column

logger = logging.getLogger(__name__)

class BlackBoxBlocker(Blocker):
    def __init__(self, *args, **kwargs):
        super(Blocker, self).__init__(*args, **kwargs)
        self.black_box_function = None

    def set_black_box_function(self, function):
        self.black_box_function = function

    def block_tables(self, ltable, rtable, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, show_progress=True):

        # validate black box functionn
        assert self.black_box_function != None, 'Black box function is not set'

        # validate input parameters
        self.validate_output_attrs(ltable, rtable, l_output_attrs,r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm.validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # do blocking

        # # initialize progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(ltable)*len(rtable))

        # # list to keep track the tuple pairs that survive blocking
        valid = []

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # create look up table for faster processing
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # # get the position of the id attribute in the tables
        l_id_pos = list(ltable.columns).index(l_key)
        r_id_pos = list(rtable.columns).index(r_key)

        # # iterate through the tuples and apply the black box function
        for l_t in ltable.itertuples(index=False):
            ltuple = l_dict[l_t[l_id_pos]]
            for r_t in rtable.itertuples(index=False):
                # # update the progress bar
                if show_progress:
                    bar.update()

                rtuple = r_dict[r_t[r_id_pos]]

                res = self.black_box_function(ltuple, rtuple)

                if res != True:
                    d = OrderedDict()

                    # # add ltable and rtable ids
                    ltable_id = l_output_prefix + l_key
                    rtable_id = r_output_prefix + r_key

                    d[ltable_id] = ltuple[l_key]
                    d[rtable_id] = rtuple[r_key]

                    # # add l/r output attributes
                    if l_output_attrs:
                        l_out = ltuple[l_output_attrs]
                        l_out.index = l_output_prefix + l_out.index
                        d.update(l_out)

                    if r_output_attrs:
                        r_out = rtuple[r_output_attrs]
                        r_out.index = r_output_prefix + r_out.index
                        d.update(r_out)

                    # # add the ordered dict to the list
                    valid.append(d)

        # construct output table
        candset = pd.DataFrame(valid)
        l_output_attrs = self.process_output_attrs(ltable, l_key, l_output_attrs, l_output_prefix)
        r_output_attrs = self.process_output_attrs(rtable, r_key, r_output_attrs, r_output_prefix)

        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs, r_output_attrs,
                                               l_output_prefix, r_output_prefix)

        if len(candset) > 0:
            candset = candset[retain_cols]
        else:
            candset = pd.DataFrame(columns=retain_cols)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix+l_key, r_output_prefix+r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, verbose=True, show_progress=True):

        # validate black box functionn
        assert self.black_box_function != None, 'Black box function is not set'

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                                'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm.validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                         logger, verbose)

        # do blocking

        # # initialize the progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(candset))

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # create lookup table for faster processing
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # # list to keep track of valid ids
        valid = []
        l_id_pos = list(candset.columns).index(fk_ltable)
        r_id_pos = list(candset.columns).index(fk_rtable)

        # # iterate candidate set
        for row in candset.itertuples(index=False):
            # # update progress bar
            if show_progress:
                bar.update()

            ltuple = l_dict[row[l_id_pos]]
            rtuple = r_dict[row[r_id_pos]]

            res = self.black_box_function(ltuple, rtuple)
            if res != True:
                valid.append(True)
            else:
                valid.append(False)

        # construct output table
        if len(candset) > 0:
            candset = candset[valid]
        else:
            candset = pd.DataFrame(columns=candset.columns)

        # update catalog
        cm.set_candset_properties(candset, key, fk_ltable, fk_rtable, ltable, rtable)

        # return candidate set
        return candset

    def block_tuples(self, ltuple, rtuple):
        # validate black box function
        assert self.black_box_function is not None, 'Black box function is not set'
        return self.black_box_function(ltuple, rtuple)
