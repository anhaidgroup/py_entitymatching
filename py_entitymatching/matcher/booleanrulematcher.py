"""
This file contains functions for boolean rule based matcher.
# NOTE: This will not be included in the first version of py_entitymatching release
"""
from collections import OrderedDict
import logging

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.matcher.rulematcher import RuleMatcher
from py_entitymatching.matcher.matcherutils import get_ts
import six

logger = logging.getLogger(__name__)

class BooleanRuleMatcher(RuleMatcher):
    def __init__(self, *args, **kwargs):
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'BooleanRuleMatcher' + '_' + get_ts()
        else:
            self.name = name
        self.rules = OrderedDict()
        self.rule_source = OrderedDict()
        self.rule_conjunct_list = OrderedDict()
        self.rule_cnt = 0

    def fit(self):
        pass


    def _predict_candset(self, candset, verbose=False):
        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # # keep track of predictions
        predictions = []

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # get the index of fk_ltable and fk_rtable from the cand. set
        col_names = list(candset.columns)
        lid_idx = col_names.index(fk_ltable)
        rid_idx = col_names.index(fk_rtable)

        # # iterate through the cand. set
        for row in candset.itertuples(index=False):
            l_row = l_df.ix[row[lid_idx]]
            r_row = r_df.ix[row[rid_idx]]
            res = self.apply_rules(l_row, r_row)
            if res is True:
                predictions.append(1)
            else:
                predictions.append(0)

        return predictions

    def predict(self, table=None, target_attr=None, append=False, inplace=True):
        if table  is not None:
            y = self._predict_candset(table)
            if target_attr is not None and append is True:
                if inplace == True:
                    table[target_attr] = y
                    return table
                else:
                    tbl = table.copy()
                    tbl[target_attr] = y
                    return tbl
            else:
                return y
        else:
            raise SyntaxError('The arguments supplied does not match the signatures supported !!!')


    def create_rule(self, conjunct_list, feature_table, name=None):
        if feature_table is None:
            logger.error('Feature table is not given')
            return False
        # set the name
        if name is None:
            name = '_rule_' + str(self.rule_cnt)
            self.rule_cnt += 1

        fn_str = self.get_function_str(name, conjunct_list)

        if feature_table is not None:
            feat_dict = dict(zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'], self.feature_table['function']))

        # exec fn_str in feat_dict
        six.exec_(fn_str, feat_dict)
        return feat_dict[name], name, fn_str

    def add_rule(self, conjunct_list, feature_table):
        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        fn, name, fn_str = self.create_rule(conjunct_list, feature_table)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_conjunct_list[name] = conjunct_list

        return True

    def del_rule(self, rule_name):
        if rule_name not in self.rules.keys():
            logger.error('Rule name not present in current set of rules')
            return False

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_conjunct_list[rule_name]

        return True

    def view_rule(self, rule_name):
        if rule_name not in self.rules.keys():
            logger.error('Rule name not present in current set of rules')
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        return self.rules.keys()

    def get_rule(self, rule_name):
        if rule_name not in self.rules.keys():
            logger.error('Rule name not present in current set of rules')
        return self.rules[rule_name]

    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            res = fn(ltuple, rtuple)
            if res is True:
                return True
        return False

    def get_function_str(self, name, conjunct_list):
        # create function str
        fn_str = "def " + name + "(ltuple, rtuple):\n"
        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)
        return fn_str