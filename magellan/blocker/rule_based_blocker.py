from collections import OrderedDict
import logging

import pandas as pd
import pyprind

from magellan.blocker.blocker import Blocker
import magellan.core.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column

logger = logging.getLogger(__name__)

class RuleBasedBlocker(Blocker):
    def __init__(self, *args, **kwargs):
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rules = OrderedDict()

        # meta data : should be removed if they are not useful.
        self.rule_source = OrderedDict()
        self.rule_cnt = 0

        super(Blocker, self).__init__(*args, **kwargs)

    def create_rule(self, conjunct_list, feature_table=None):
        if feature_table is None and self.feature_table is None:
            logger.error('Either feature table should be given as parameter or use set_feature_table to '
                         'set the feature table')
            return False

        # set the rule name
        name = '_rule_' + str(self.rule_cnt)
        self.rule_cnt += 1

        # create function string
        fn_str = 'def ' + name + '(ltuple, rtuple):\n'

        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)

        if feature_table is not None:
            feat_dict = dict(zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'], feature_table['function']))


        exec(fn_str in feat_dict)

        return feat_dict[name], name, fn_str


    def add_rule(self, conjunct_list, feature_table):

        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        fn, name, fn_str = self.create_rule(conjunct_list, feature_table)

        self.rules[name] = fn
        self.rule_source[name] = fn_str


    def delete_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        return True

    def view_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        return self.rules.keys()

    def get_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        return self.rules[rule_name]

    def set_feature_table(self, feature_table):
        if self.feature_table is not None:
            logger.warning('Feature table is already set, changing it now will not recompile '
                           'existing rules')
        self.feature_table = feature_table


    def block_tables(self, ltable, rtable, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=True, show_progress=True
                     ):
        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # validate input parameters
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

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

        # # list to keep track of the tuple pairs that survive blocking
        valid = []

        #  # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # create look up table for faster processing
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # # get the position of the id attributes in the tables
        l_id_pos = list(ltable.columns).index(l_key)
        r_id_pos = list(rtable.columns).index(r_key)

        # # iterate through the tuples and apply the rules
        for l_t in ltable.itertuples(index=False):
            ltuple = l_dict[l_t[l_id_pos]]
            for r_t in rtable.itertuples(index=False):
                # # update the progress bar
                if show_progress:
                    bar.update()

                rtuple = r_dict[r_t[r_id_pos]]
                res = self.apply_rules(ltuple, rtuple)

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

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

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

            res = self.apply_rules(ltuple, rtuple)
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
        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'
        return self.apply_rules(ltuple, rtuple)










    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            # here if the function returns true, then the tuple pair must be dropped.
            res =  fn(ltuple, rtuple)
            return res
