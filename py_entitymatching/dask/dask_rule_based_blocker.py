import logging
from collections import OrderedDict

import pandas as pd
import numpy as np
import pyprind
import six

import dask
from dask import delayed
from dask.diagnostics import ProgressBar

from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer
from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
import cloudpickle as cp
import pickle

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.blocker.blocker import Blocker
import py_stringsimjoin as ssj
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, \
    add_key_column
from py_entitymatching.utils.generic_helper import parse_conjunct
from py_entitymatching.utils.validation_helper import validate_object_type


from py_entitymatching.dask.utils import validate_chunks, get_num_partitions, \
    get_num_cores, wrap

logger = logging.getLogger(__name__)


class DaskRuleBasedBlocker(Blocker):
    """
    WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.

    Blocks  based on a sequence of blocking rules supplied by the user.
    """
    def __init__(self, *args, **kwargs):
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rules = OrderedDict()

        self.rule_str = OrderedDict()
        self.rule_ft = OrderedDict()
        self.filterable_sim_fns = {'jaccard', 'cosine', 'dice', 'overlap_coeff'}
        self.allowed_ops = {'<', '<='}

        self.rule_source = OrderedDict()
        self.rule_cnt = 0


        logger.warning("WARNING THIS BLOCKER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN "
              "RISK.")
        super(Blocker, self).__init__(*args, **kwargs)


    def _create_rule(self, conjunct_list, feature_table, rule_name):
        if rule_name is None:
            # set the rule name automatically
            name = '_rule_' + str(self.rule_cnt)
            self.rule_cnt += 1
        else:
            # use the rule name supplied by the user
            name = rule_name

        # create function string
        fn_str = 'def ' + name + '(ltuple, rtuple):\n'

        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)

        if feature_table is not None:
            feat_dict = dict(
                zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'],
                                 self.feature_table['function']))

        six.exec_(fn_str, feat_dict)

        return feat_dict[name], name, fn_str

    def add_rule(self, conjunct_list, feature_table=None, rule_name=None):
        """Adds a rule to the rule-based blocker.
        
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
                AssertionError: If `feature_table` is not a valid value
                 parameter.
        
            Examples:
                >>> import py_entitymatching 
                >>> from py_entitymatching.dask.dask_rule_based_blocker import DaskRuleBasedBlocker
                >>> rb = DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, rule_name='rule1')
        """

        if rule_name is not None and rule_name in self.rules.keys():
            logger.error('A rule with the specified rule_name already exists.')
            raise AssertionError('A rule with the specified rule_name already exists.')

        if feature_table is None and self.feature_table is None:
            logger.error('Either feature table should be given as parameter ' +
                         'or use set_feature_table to set the feature table.')
            raise AssertionError('Either feature table should be given as ' +
                                 'parameter or use set_feature_table to set ' +
                                 'the feature table.')

        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        fn, name, fn_str = self._create_rule(conjunct_list, feature_table, rule_name)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_str[name] = conjunct_list
        if feature_table is not None:
            self.rule_ft[name] = feature_table
        else:
            self.rule_ft[name] = self.feature_table

        return name

    def delete_rule(self, rule_name):
        """Deletes a rule from the rule-based blocker.
        
            Args:
               rule_name (string): Name of the rule to be deleted.
        
            Examples:
                >>> import py_entitymatching as em
                >>> from py_entitymatching.dask.dask_rule_based_blocker import DaskRuleBasedBlocker
                >>> rb = DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, block_f, rule_name='rule_1')
                >>> rb.delete_rule('rule_1')
        """
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_str[rule_name]
        del self.rule_ft[rule_name]

        return True

    def view_rule(self, rule_name):
        """Prints the source code of the function corresponding to a rule.
        
            Args:
               rule_name (string): Name of the rule to be viewed.
        
            Examples:
                >>> import py_entitymatching as em
                >>> rb = em.DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, block_f, rule_name='rule_1')
                >>> rb.view_rule('rule_1')
        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        """Returns the names of all the rules in the rule-based blocker.
        
           Returns:
               A list of names of all the rules in the rule-based blocker (list).
        
           Examples:
                >>> import py_entitymatching as em
                >>> rb = em.DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, block_f, rule_name='rule_1')
                >>> rb.get_rule_names()
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
                >>> rb = em.DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, feature_table=block_f, rule_name='rule_1')
                >>> rb.get_rule()
        """
        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        return self.rules[rule_name]

    def set_feature_table(self, feature_table):
        """Sets feature table for the rule-based blocker.
        
            Args:
               feature_table (DataFrame): A DataFrame containing features.
        
            Examples:
                >>> import py_entitymatching as em
                >>> rb = em.DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rb.set_feature_table(block_f)
        """

        if self.feature_table is not None:
            logger.warning(
                'Feature table is already set, changing it now will not recompile '
                'existing rules')
        self.feature_table = feature_table

    def block_tables(self, ltable, rtable, l_output_attrs=None,
                     r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=False, show_progress=True, n_ltable_chunks=1,
                     n_rtable_chunks=1):
        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK

        Blocks two tables based on the sequence of rules supplied by the user.
        Finds tuple pairs from left and right tables that survive the sequence
        of blocking rules. A tuple pair survives the sequence of blocking rules
        if none of the rules in the sequence returns True for that pair. If any
        of the rules returns True, then the pair is blocked.
        
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
                information  should be logged (defaults to False).
                
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
            
            A candidate set of tuple pairs that survived the sequence of
            blocking rules (DataFrame).

        Raises:
            
            AssertionError: If `ltable` is not of type pandas
                DataFrame.
            
            AssertionError: If `rtable` is not of type pandas
                DataFrame.
            AssertionError: If `l_output_attrs` is not of type of
                list.
            AssertionError: If `r_output_attrs` is not of type of
                list.
            AssertionError: If the values in `l_output_attrs` is not of type
                string.
            AssertionError: If the values in `r_output_attrs` is not of type
                string.
            AssertionError: If the input `l_output_prefix` is not of type
                string.
            AssertionError: If the input `r_output_prefix` is not of type
                string.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `show_progress` is not of type
                boolean.
            AssertionError: If `n_ltable_chunks` is not of type
                int.
            AssertionError: If `n_rtable_chunks` is not of type
                int.
            AssertionError: If `l_out_attrs` are not in the ltable.
            AssertionError: If `r_out_attrs` are not in the rtable.
            AssertionError: If there are no rules to apply.
        Examples:
                >>> import py_entitymatching as em
                >>> from py_entitymatching.dask.dask_rule_based_blocker import DaskRuleBasedBlocker
                >>> rb = DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, feature_table=block_f)
                >>> C = rb.block_tables(A, B)
        """

        logger.warning(
            "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.")

        # validate data types of input parameters
        self.validate_types_params_tables(ltable, rtable,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix,
                                          r_output_prefix, verbose, 1)

        # validate data type of show_progress
        self.validate_show_progress(show_progress)

        # validate input parameters
        self.validate_output_attrs(ltable, rtable, l_output_attrs,
                                   r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger,
                                                     verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger,
                                        verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger,
                                        verbose)

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # validate number of ltable and rtable chunks
        validate_object_type(n_ltable_chunks, int, 'Parameter n_ltable_chunks')
        validate_object_type(n_rtable_chunks, int, 'Parameter n_rtable_chunks')

        validate_chunks(n_ltable_chunks)
        validate_chunks(n_rtable_chunks)

        # # determine the number of chunks
        n_ltable_chunks = get_num_partitions(n_ltable_chunks, len(ltable))
        n_rtable_chunks = get_num_partitions(n_rtable_chunks, len(rtable))

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

        # # get attributes to project
        l_proj_attrs, r_proj_attrs = self.get_attrs_to_project(l_key, r_key,
                                                               l_output_attrs_1,
                                                               r_output_attrs_1)
        l_df, r_df = l_df[l_proj_attrs], r_df[r_proj_attrs]

        candset, rule_applied = self.block_tables_with_filters(l_df, r_df,
                                                               l_key, r_key,
                                                               l_output_attrs_1,
                                                               r_output_attrs_1,
                                                               l_output_prefix,
                                                               r_output_prefix,
                                                               verbose,
                                                               show_progress,
                                                               get_num_cores())
                                                               # pass number of splits as
        #  the number of cores in the machine

        if candset is None:
            # no filterable rule was applied
            candset = self.block_tables_without_filters(l_df, r_df, l_key,
                                                        r_key, l_output_attrs_1,
                                                        r_output_attrs_1,
                                                        l_output_prefix,
                                                        r_output_prefix,
                                                        verbose, show_progress,
                                                        n_ltable_chunks, n_rtable_chunks)
        elif len(self.rules) > 1:
            # one filterable rule was applied but other rules are left
            # block candset by applying other rules and excluding the applied rule
            candset = self.block_candset_excluding_rule(candset, l_df, r_df,
                                                        l_key, r_key,
                                                        l_output_prefix + l_key,
                                                        r_output_prefix + r_key,
                                                        rule_applied,
                                                        show_progress, get_num_cores())

        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs_1,
                                               r_output_attrs_1,
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

    def block_candset_excluding_rule(self, c_df, l_df, r_df, l_key, r_key,
                                     fk_ltable, fk_rtable, rule_to_exclude,
                                     show_progress, n_chunks):


        # # list to keep track of valid ids
        valid = []

        apply_rules_excluding_rule_pkl = cp.dumps(self.apply_rules_excluding_rule)

        if n_chunks == 1:
            # single process
            valid = _block_candset_excluding_rule_split(c_df, l_df, r_df,
                                                        l_key, r_key,
                                                        fk_ltable, fk_rtable,
                                                        rule_to_exclude,
                                                        apply_rules_excluding_rule_pkl,
                                                        show_progress)
        else:
            # multiprocessing
            c_splits = np.array_split(c_df, n_chunks)

            valid_splits = []

            for i in range(len(c_splits)):
                partial_result = delayed(_block_candset_excluding_rule_split)(c_splits[i],
                                                             l_df, r_df,
                                                             l_key, r_key,
                                                             fk_ltable,
                                                             fk_rtable,
                                                             rule_to_exclude,
                                                             apply_rules_excluding_rule_pkl, False)
                                                            # use Progressbar from
                                                            # Dask.diagnostics so set the
                                                            #show_progress to False

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

        # construct output candset
        if len(c_df) > 0:
            candset = c_df[valid]
        else:
            candset = pd.DataFrame(columns=c_df.columns)

        # return candidate set
        return candset

    def block_tables_without_filters(self, l_df, r_df, l_key, r_key,
                                     l_output_attrs, r_output_attrs,
                                     l_output_prefix, r_output_prefix,
                                     verbose, show_progress, n_ltable_chunks,
                                     n_rtable_chunks):

        # do blocking

        # # determine the number of processes to launch parallely


        candset = None

        apply_rules_pkl = cp.dumps(self.apply_rules)

        if n_ltable_chunks == 1 and n_rtable_chunks == 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix, r_output_prefix,
                                          apply_rules_pkl, show_progress)
        else:
            # multiprocessing
            # m, n = self.get_split_params(n_procs, len(l_df), len(r_df))
            l_splits = np.array_split(l_df, n_ltable_chunks)
            r_splits = np.array_split(r_df, n_rtable_chunks)

            c_splits = []
            for i in range(len(l_splits)):
                for j in range(len(r_splits)):
                    partial_result = delayed(_block_tables_split)(l_splits[i], r_splits[j],
                                             l_key, r_key,
                                             l_output_attrs, r_output_attrs,
                                             l_output_prefix, r_output_prefix,
                                             apply_rules_pkl, False) # we will use
                    # Dask.diagnostics to display the progress bar so set
                    # show_progress to False
                    c_splits.append(partial_result)

            c_splits = delayed(wrap)(c_splits)
            if show_progress:
                with ProgressBar():
                    c_splits = c_splits.compute(scheduler="processes", num_workers = get_num_cores())
            else:
                c_splits = c_splits.compute(scheduler="processes", num_workers=get_num_cores())
            candset = pd.concat(c_splits, ignore_index=True)

        # return candidate set
        return candset

    def block_candset(self, candset, verbose=False, show_progress=True,
                      n_chunks=1):
        """
        WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK

        Blocks an input candidate set of tuple pairs based on a sequence of
        blocking rules supplied by the user.
        Finds tuple pairs from an input candidate set of tuple pairs that
        survive the sequence of blocking rules. A tuple pair survives the
        sequence of blocking rules if none of the rules in the sequence returns
        True for that pair. If any of the rules returns True, then the pair is
        blocked (dropped).

        Args:
            candset (DataFrame): The input candidate set of tuple pairs.
            verbose (boolean): A flag to indicate whether the debug
                information  should be logged (defaults to False).
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
            AssertionError: If there are no rules to apply.
            
        Examples:
                >>> import py_entitymatching as em
                >>> from py_entitymatching.dask.dask_rule_based_blocker import DaskRuleBasedBlocker
                >>> rb = DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, feature_table=block_f)
                >>> D = rb.block_tables(C) # C is the candidate set.
        """
        logger.warning(
            "WARNING THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.")

        # validate data types of input parameters
        self.validate_types_params_candset(candset, verbose, show_progress,
                                           n_chunks)

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, ' +
                 'fk rtable, ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(
            candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable,
                                          ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # validate n_chunks parameter
        validate_object_type(n_chunks, int, 'Parameter n_chunks')
        validate_chunks(n_chunks)

        n_chunks = get_num_partitions(n_chunks, len(candset))
        # do blocking

        # # initialize the progress bar
        # if show_progress:
        #     bar = pyprind.ProgBar(len(candset))

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # get attributes to project
        l_proj_attrs, r_proj_attrs = self.get_attrs_to_project(l_key, r_key,
                                                               [], [])
        l_df, r_df = l_df[l_proj_attrs], r_df[r_proj_attrs]

        c_df = self.block_candset_excluding_rule(candset, l_df, r_df, l_key,
                                                 r_key,
                                                 fk_ltable, fk_rtable, None,
                                                 show_progress, n_chunks)

        # update catalog
        cm.set_candset_properties(c_df, key, fk_ltable, fk_rtable, ltable,
                                  rtable)

        # return candidate set
        return c_df


    def block_tuples(self, ltuple, rtuple):
        """
        Blocks a tuple pair based on a sequence of blocking rules supplied
        by the user.
        
        Args:
            ltuple (Series): The input left tuple.
            rtuple (Series): The input right tuple.
        
        Returns:
            A status indicating if the tuple pair is blocked by applying the
            sequence of blocking rules (boolean).
        
        Examples:
                >>> import py_entitymatching as em
                >>> from py_entitymatching.dask.dask_rule_based_blocker import DaskRuleBasedBlocker
                >>> rb = DaskRuleBasedBlocker()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> block_f = em.get_features_for_blocking(A, B)
                >>> rule = ['name_name_lev(ltuple, rtuple) > 3']
                >>> rb.add_rule(rule, feature_table=block_f)
                >>> D = rb.block_tuples(A.loc[0], B.loc[1)
        """


        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'
        return self.apply_rules(ltuple, rtuple)

    def apply_rules(self, ltuple, rtuple):
        for fn in self.rules.values():
            # here if the function returns true, then the tuple pair must be dropped.
            res = fn(ltuple, rtuple)
            if res == True:
                return res
        return False

    def apply_rules_excluding_rule(self, ltuple, rtuple, rule_to_exclude):
        for rule_name in self.rules.keys():
            if rule_name != rule_to_exclude:
                fn = self.rules[rule_name]
                # here if fn returns true, then the tuple pair must be dropped.
                res = fn(ltuple, rtuple)
                if res == True:
                    return res
        return False

    def get_attrs_to_project(self, l_key, r_key, l_output_attrs,
                             r_output_attrs):
        l_proj_attrs = [l_key]
        r_proj_attrs = [r_key]
        if l_output_attrs:
            l_proj_attrs.extend(
                [c for c in l_output_attrs if c not in l_proj_attrs])
        if r_output_attrs:
            r_proj_attrs.extend(
                [c for c in r_output_attrs if c not in r_proj_attrs])
        for rule_name, conjunct_list in six.iteritems(self.rule_str):
            for conjunct in conjunct_list:
                is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = parse_conjunct(
                    conjunct, self.rule_ft[rule_name])
                if l_attr not in l_proj_attrs:
                    l_proj_attrs.append(l_attr)
                if r_attr not in r_proj_attrs:
                    r_proj_attrs.append(r_attr)
        return l_proj_attrs, r_proj_attrs

    def block_tables_with_filters(self, l_df, r_df, l_key, r_key,
                                  l_output_attrs, r_output_attrs,
                                  l_output_prefix, r_output_prefix, verbose,
                                  show_progress, n_chunks):
        for rule_name in self.rules.keys():
            # first check if a rule is filterable
            if self.is_rule_filterable(rule_name):
                candset = self.apply_filterable_rule(rule_name, l_df, r_df,
                                                     l_key, r_key,
                                                     l_output_attrs,
                                                     r_output_attrs,
                                                     l_output_prefix,
                                                     r_output_prefix, verbose,
                                                     show_progress, n_chunks)
                if candset is not None:
                    # rule was filterable
                    return candset, rule_name
        return None, None

    def is_rule_filterable(self, rule_name):
        # a rule is filterable if all the conjuncts in the conjunct list
        # are filterable
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            res = self.is_conjunct_filterable(conjunct, rule_name)
            if res != True:
                return res
        return True

    def is_conjunct_filterable(self, conjunct, rule_name):
        # a conjunct is filterable if it uses
        # a filterable sim function (jaccard, cosine, dice, ...),
        # an allowed operator (<, <=),
        is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = parse_conjunct(
            conjunct, self.rule_ft[rule_name])
        if is_auto_gen != True:
            # conjunct not filterable as the feature is not auto generated
            return False
        if sim_fn == 'lev_dist':
            if op == '>' or op == '>=':
                return True
            else:
                # conjunct not filterable due to unsupported operator
                return False
        if l_tok != r_tok:
            # conjunct not filterable because left and right tokenizers mismatch
            return False
        if sim_fn not in self.filterable_sim_fns:
            # conjunct not filterable due to unsupported sim_fn
            return False
        if op not in self.allowed_ops:
            # conjunct not filterable due to unsupported operator
            return False
        # conjunct is filterable
        return True

    def apply_filterable_rule(self, rule_name, l_df, r_df, l_key, r_key,
                              l_output_attrs, r_output_attrs,
                              l_output_prefix, r_output_prefix,
                              verbose, show_progress, n_chunks):
        candset = None
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            is_auto_gen, sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = parse_conjunct(
                conjunct, self.rule_ft[rule_name])

            if l_tok == 'dlm_dc0':
                tokenizer = WhitespaceTokenizer(return_set=True)
            elif l_tok == 'qgm_3':
                tokenizer = QgramTokenizer(qval=3, return_set=True)

            if sim_fn == 'jaccard':
                join_fn = ssj.jaccard_join
            elif sim_fn == 'cosine':
                join_fn = ssj.cosine_join
            elif sim_fn == 'dice':
                join_fn = ssj.dice_join
            elif sim_fn == 'overlap_coeff':
                join_fn = ssj.overlap_coefficient_join
            elif sim_fn == 'lev_dist':
                join_fn = ssj.edit_distance_join

            if join_fn == ssj.edit_distance_join:
                comp_op = '<='
                if op == '>=':
                    comp_op = '<'
            else:
                comp_op = '>='
                if op == '<=':
                    comp_op = '>'

            ssj.dataframe_column_to_str(l_df, l_attr, inplace=True)
            ssj.dataframe_column_to_str(r_df, r_attr, inplace=True)

            if join_fn == ssj.edit_distance_join:
                c_df = join_fn(l_df, r_df, l_key, r_key, l_attr, r_attr,
                               float(th), comp_op, True, l_output_attrs,
                               r_output_attrs, l_output_prefix,
                               r_output_prefix, False, n_chunks, show_progress)
            else:
                c_df = join_fn(l_df, r_df, l_key, r_key, l_attr, r_attr,
                               tokenizer, float(th), comp_op, True, True,
                               l_output_attrs, r_output_attrs,
                               l_output_prefix,
                               r_output_prefix, False, n_chunks, show_progress)
            if candset is not None:
                # union the candset of this conjunct with the existing candset
                candset = pd.concat([candset, c_df]).drop_duplicates(
                    [l_output_prefix + l_key,
                     r_output_prefix + r_key]).reset_index(drop=True)
            else:
                # candset from the first conjunct of the rule
                candset = c_df
        return candset

def _block_tables_split(l_df, r_df, l_key, r_key,
                        l_output_attrs, r_output_attrs,
                        l_output_prefix, r_output_prefix, apply_rules_pkl,
                        show_progress):
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

    # unpickle the apply_rules function
    apply_rules = pickle.loads(apply_rules_pkl)

    # iterate through the two tables
    for l_t in l_df.itertuples(index=False):
        # # get ltuple from the look up table
        ltuple = l_dict[l_t[l_id_pos]]
        for r_t in r_df.itertuples(index=False):
            # # update the progress bar
            if show_progress:
                bar.update()

            # # get rtuple from the look up dictionary
            rtuple = r_dict[r_t[r_id_pos]]

            # # apply the rules to the tuple pair
            res = apply_rules(ltuple, rtuple)

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


def _block_candset_excluding_rule_split(c_df, l_df, r_df, l_key, r_key,
                                        fk_ltable,
                                        fk_rtable, rule_to_exclude,
                                        apply_rules_excluding_rule_pkl,
                                        show_progress):
    # do blocking

    # # initialize the progress bar
    if show_progress:
        bar = pyprind.ProgBar(len(c_df))

    # # initialize lookup tables for faster processing
    l_dict = {}
    r_dict = {}

    # # list to keep track of valid ids
    valid = []

    l_id_pos = list(c_df.columns).index(fk_ltable)
    r_id_pos = list(c_df.columns).index(fk_rtable)

    # # unpickle the apply_rules_excluding_rule function
    apply_rules_excluding_rule = pickle.loads(apply_rules_excluding_rule_pkl)

    # # iterate candidate set
    for row in c_df.itertuples(index=False):
        # # update progress bar
        if show_progress:
            bar.update()

        # # get ltuple, try dictionary first, then dataframe
        row_lid = row[l_id_pos]
        if row_lid not in l_dict:
            l_dict[row_lid] = l_df.loc[row_lid]
        ltuple = l_dict[row_lid]

        # # get rtuple, try dictionary first, then dataframe
        row_rid = row[r_id_pos]
        if row_rid not in r_dict:
            r_dict[row_rid] = r_df.loc[row_rid]
        rtuple = r_dict[row_rid]

        res = apply_rules_excluding_rule(ltuple, rtuple, rule_to_exclude)

        if res != True:
            valid.append(True)
        else:
            valid.append(False)

    return valid
