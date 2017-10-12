from collections import OrderedDict
import logging

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.validation_helper import validate_object_type
from py_entitymatching.utils.generic_helper import parse_conjunct
import pandas as pd
import six

logger = logging.getLogger(__name__)


class MatchTrigger(object):
    def __init__(self):
        self.cond_status = False
        self.rules = OrderedDict()
        self.rule_source = OrderedDict()
        self.rule_conjunct_list = OrderedDict()
        self.rule_cnt = 0
        self.value_to_set = 0
        self.rule_ft = OrderedDict()
        self.feature_table = None

    def add_cond_rule(self, conjunct_list, feature_table, rule_name=None):
        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        if rule_name is not None and rule_name in self.rules.keys():
            logger.error('A rule with the specified rule_name already exists.')
            raise AssertionError('A rule with the specified rule_name already exists.')

        if feature_table is None and self.feature_table is None:
            logger.error('Either feature table should be given as parameter ' +
                         'or use set_feature_table to set the feature table.')
            raise AssertionError('Either feature table should be given as ' +
                                 'parameter or use set_feature_table to set ' +
                                 'the feature table.')

        if feature_table is None:
            feature_table = self.feature_table

        fn, name, fn_str = self._create_rule(conjunct_list, feature_table, rule_name)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_conjunct_list[name] = conjunct_list
        if feature_table is not None:
            self.rule_ft[name] = feature_table
        else:
            self.rule_ft[name] = self.feature_table

        return True

    def add_cond_status(self, status):
        if not isinstance(status, bool):
            raise AssertionError('status is expected to be a boolean i.e True/False')
        self.cond_status = status
        return True

    def add_action(self, value):
        if value != 0 and value != 1:
            raise AssertionError('Currently py_entitymatching supports only values 0/1 as label value')
        self.value_to_set = value
        return True

    def execute(self, input_table, label_column, inplace=True, verbose=False):

        # Validate input parameters
        # # We expect the table to be of type pandas DataFrame
        validate_object_type(input_table, pd.DataFrame, 'Input table')

        # # We expect the target_attr to be of type string if not None
        if label_column is not None and not isinstance(label_column, str):
            logger.error('Input target_attr must be a string.')
            raise AssertionError('Input target_attr must be a string.')

        # # We expect the inplace to be of type boolean
        validate_object_type(inplace, bool, 'Input inplace')

        # # We expect the verbose to be of type boolean
        validate_object_type(verbose, bool, 'Input append')

        # Validate that there are some rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(input_table, logger,
                                                                                              verbose)
        # # validate metadata
        cm._validate_metadata_for_candset(input_table, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                          logger, verbose)

        assert ltable is not None, 'Left table is not set'
        assert rtable is not None, 'Right table is not set'
        assert label_column in input_table.columns, 'Label column not in the input table'

        if inplace == False:
            table = input_table.copy()
        else:
            table = input_table

        # set the index and store it in l_tbl/r_tbl
        l_tbl = ltable.set_index(l_key, drop=False)
        r_tbl = rtable.set_index(r_key, drop=False)

        # keep track of valid ids
        y = []

        column_names = list(input_table.columns)
        lid_idx = column_names.index(fk_ltable)
        rid_idx = column_names.index(fk_rtable)

        label_idx = column_names.index(label_column)
        idx = 0
        for row in input_table.itertuples(index=False):
            if row[label_idx] != self.value_to_set:
                l_row = l_tbl.ix[row[lid_idx]]
                r_row = r_tbl.ix[row[rid_idx]]
                res = self.apply_rules(l_row, r_row)
                if res == self.cond_status:
                    table.iat[idx, label_idx] = self.value_to_set
            idx += 1
        return table

    def _create_rule(self, conjunct_list, feature_table, rule_name=None):
        if feature_table is None:
            logging.getLogger(__name__).error('Feature table is not given')
            return False

        # set the name
        if rule_name is None:
            name = '_rule_' + str(self.rule_cnt)
            self.rule_cnt += 1
        else:
            # use the rule name supplied by the user
            name = rule_name

        fn_str = self.get_function_str(name, conjunct_list)

        if feature_table is not None:
            feat_dict = dict(zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'], self.feature_table['function']))

        # exec fn_str in feat_dict
        six.exec_(fn_str, feat_dict)
        return feat_dict[name], name, fn_str

    def delete_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_conjunct_list[rule_name]

        return True

    def view_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        return self.rules.keys()

    def get_rule(self, rule_name):
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        return self.rules[rule_name]

    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            res = fn(ltuple, rtuple)
            if res == True:
                return True
        return False

    def get_function_str(self, name, conjunct_list):
        # create function str
        fn_str = "def " + name + "(ltuple, rtuple):\n"
        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)
        return fn_str

    def set_feature_table(self, feature_table):
        if self.feature_table is not None:
            logger.warning(
                'Feature table is already set, changing it now will not recompile '
                'existing rules')
        self.feature_table = feature_table
