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
        """Adds a rule to the match trigger.

            Args:
                conjunct_list (list): A list of conjuncts specifying the rule.

                feature_table (DataFrame): A DataFrame containing all the
                                           features that are being referenced by
                                           the rule (defaults to None). If the
                                           feature_table is not supplied here,
                                           then it must have been specified
                                           during the creation of the rule-based
                                           blocker or using set_feature_table
                                           function. Otherwise an AssertionError
                                           will be raised and the rule will not
                                           be added to the rule-based blocker.

                rule_name (string): A string specifying the name of the rule to
                                    be added (defaults to None). If the
                                    rule_name is not specified then a name will
                                    be automatically chosen. If there is already
                                    a rule with the specified rule_name, then
                                    an AssertionError will be raised and the
                                    rule will not be added to the rule-based
                                    blocker.

            Returns:
                The name of the rule added (string).

            Raises:
                AssertionError: If `rule_name` already exists.

                AssertionError: If `feature_table` is not a valid value parameter.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)

            """

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

        return name

    def add_cond_status(self, status):
        """Adds a condition status to the match trigger. If the result of a rule
           is the same value as the condition status, then the action will be
           carried out. The action can be added with the function add_action.

            Args:
               status (boolean): The condition status.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.add_cond_status(True)
                >>> mt.add_action(1)

        """

        if not isinstance(status, bool):
            raise AssertionError('status is expected to be a boolean i.e True/False')
        self.cond_status = status
        return True

    def add_action(self, value):
        """Adds an action to the match trigger. If the result of a rule is the
           same value as the condition status, then the action will be carried
           out. The condition status can be added with the function add_cond_status.

            Args:
               value (integer): The action. Currently only the values 0 and 1 are supported.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.add_cond_status(True)
                >>> mt.add_action(1)

        """

        if value != 0 and value != 1:
            raise AssertionError('Currently py_entitymatching supports only values 0/1 as label value')
        self.value_to_set = value
        return True

    def execute(self, input_table, label_column, inplace=True, verbose=False):
        """ Executes the rules of the match trigger for a table of matcher
            results.

            Args:
                input_table (DataFrame): The input table of type pandas DataFrame
                    containing tuple pairs and labels from matching (defaults to None).
                label_column (string): The attribute name where the predictions
                    are stored in the input table (defaults to None).
                inplace (boolean): A flag to indicate whether the append needs to be
                    done inplace (defaults to True).
                verbose (boolean): A flag to indicate whether the debug information
                    should be logged (defaults to False).

            Returns:
                A DataFrame with predictions updated.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.add_cond_status(True)
                >>> mt.add_action(1)
                >>> # The table H is a table with prediction labels generated from matching
                >>> mt.execute(input_table=H, label_column='predicted_labels', inplace=False)

        """

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

        # Parse conjuncts to validate that the features are in the feature table
        for rule in self.rule_conjunct_list:
            for conjunct in self.rule_conjunct_list[rule]:
                parse_conjunct(conjunct, self.rule_ft[rule])

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
                l_row = l_tbl.loc[row[lid_idx]]
                r_row = r_tbl.loc[row[rid_idx]]
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
        """Deletes a rule from the match trigger.

            Args:
               rule_name (string): Name of the rule to be deleted.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.delete_rule('rule_1')

        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_conjunct_list[rule_name]

        return True

    def view_rule(self, rule_name):
        """Prints the source code of the function corresponding to a rule.

            Args:
               rule_name (string): Name of the rule to be viewed.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.view_rule('rule_1')

        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        """Returns the names of all the rules in the match trigger.

           Returns:
               A list of names of all the rules in the match trigger (list).

           Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.get_rule_names()

        """

        return self.rules.keys()

    def get_rule(self, rule_name):
        """Returns the function corresponding to a rule.

           Args:
               rule_name (string): Name of the rule.

           Returns:
               A function object corresponding to the specified rule.

           Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['title_title_lev_sim(ltuple, rtuple) > 0.7']
                >>> mt.add_cond_rule(rule, match_f)
                >>> mt.get_rule()

        """

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
        """Sets feature table for the match trigger.

            Args:
               feature_table (DataFrame): A DataFrame containing features.

            Examples:
                >>> import py_entitymatching as em
                >>> mt = em.MatchTrigger()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> mt.set_feature_table(match_f)
        """

        if self.feature_table is not None:
            logger.warning(
                'Feature table is already set, changing it now will not recompile '
                'existing rules')
        self.feature_table = feature_table
