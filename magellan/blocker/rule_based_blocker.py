from collections import OrderedDict
import logging
import sys

import pandas as pd
import pyprind
import six
from joblib import Parallel, delayed

from py_stringmatching.tokenizer.whitespace_tokenizer import WhitespaceTokenizer
from py_stringmatching.tokenizer.qgram_tokenizer import QgramTokenizer

from magellan.externals.py_stringsimjoin.join.cosine_join import cosine_join
from magellan.externals.py_stringsimjoin.join.dice_join import dice_join
from magellan.externals.py_stringsimjoin.join.jaccard_join import jaccard_join
from magellan.externals.py_stringsimjoin.join.overlap_coefficient_join import overlap_coefficient_join
from magellan.externals.py_stringsimjoin.join.overlap_join import overlap_join

from magellan.blocker.blocker import Blocker
import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column

logger = logging.getLogger(__name__)

global_rb = None

class RuleBasedBlocker(Blocker):
    def __init__(self, *args, **kwargs):
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rules = OrderedDict()

        self.rule_str = OrderedDict()
        self.rule_ft = OrderedDict()
        self.filterable_sim_fns = {'jaccard', 'cosine', 'dice', 'overlap',
                                   'overlap_coefficient'}
        self.allowed_ops = {'<', '<='}
 
        # meta data : should be removed if they are not useful.
        self.rule_source = OrderedDict()
        self.rule_cnt = 0

        super(Blocker, self).__init__(*args, **kwargs)

    def create_rule(self, conjunct_list, feature_table=None):
        if feature_table is None and self.feature_table is None:
            logger.error('Either feature table should be given as parameter ' +
                         'or use set_feature_table to set the feature table')
            raise AssertionError('Either feature table should be given as ' +
                                 'parameter or use set_feature_table to set ' +
                                 'the feature table')

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
            feat_dict = dict(zip(self.feature_table['feature_name'], self.feature_table['function']))


        six.exec_(fn_str, feat_dict)

        return feat_dict[name], name, fn_str


    def add_rule(self, conjunct_list, feature_table):

        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        fn, name, fn_str = self.create_rule(conjunct_list, feature_table)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_str[name] = conjunct_list
        if feature_table is not None:
            self.rule_ft[name] = feature_table
        else:
            self.rule_ft[name] = self.feature_table

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
                     verbose=False, show_progress=True, n_jobs=1):

        # validate data types of input parameters
        self.validate_types_params_tables(ltable, rtable,
                       l_output_attrs, r_output_attrs, l_output_prefix,
                       r_output_prefix, verbose, show_progress, n_jobs)

        # validate input parameters
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # do blocking

        # # initialize progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(ltable)*len(rtable))

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
        l_proj_attrs, r_proj_attrs = self.get_attrs_to_project(l_key, r_key, l_output_attrs_1, r_output_attrs_1)
        l_df, r_df = l_df[l_proj_attrs], r_df[r_proj_attrs]
      
        candset, rule_applied = self.block_tables_with_filters(l_df, r_df, l_key, r_key, l_output_attrs_1,
                                      r_output_attrs_1, l_output_prefix, r_output_prefix, verbose, show_progress, n_jobs)

        if candset is None:
            # no filterable rule was applied
            print('No filterable rule was applied')
            candset = self.block_tables_without_filters(l_df, r_df, l_key, r_key, l_output_attrs_1,
                                         r_output_attrs_1, l_output_prefix,
                                         r_output_prefix, verbose, show_progress, n_jobs)
        elif len(self.rules) > 1:
            # one filterable rule was applied but other rules are left
            # block the candset by applying other rules
            rules = self.rules.copy()
            rules.pop(rule_applied, None)
            candset = self.block_candset_with_rules(candset, l_df, r_df, l_key, r_key,
                                                 l_output_prefix + l_key,
                                                 r_output_prefix + r_key, rules,
                                                 show_progress)
        
        #print('candset cols: ', candset.columns)                                         
        retain_cols = self.get_attrs_to_retain(l_key, r_key, l_output_attrs_1, r_output_attrs_1,
                                               l_output_prefix, r_output_prefix)
        #print('retain_cols: ', retain_cols)
        candset = candset[retain_cols]
 
        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix+l_key, r_output_prefix+r_key, ltable, rtable)

        # return candidate set
        #print('Candset:', candset)
        return candset

    def block_candset_with_rules(self, c_df, l_df, r_df, l_key, r_key,
                                 fk_ltable, fk_rtable, rules, show_progress):

        # do blocking

        # # initialize the progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(c_df))

        # # create lookup table for faster processing
        l_dict = {}
        for k, r in l_df.iterrows():
            l_dict[k] = r

        r_dict = {}
        for k, r in r_df.iterrows():
            r_dict[k] = r

        # # list to keep track of valid ids
        valid = []
        l_id_pos = list(c_df.columns).index(fk_ltable)
        r_id_pos = list(c_df.columns).index(fk_rtable)

        # # iterate candidate set
        for row in c_df.itertuples(index=False):
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
        if len(c_df) > 0:
            candset = c_df[valid]
        else:
            candset = pd.DataFrame(columns=c_df.columns)

        # return candidate set
        return candset
         
    def block_tables_skd(self, ltable, rtable, l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     verbose=False, show_progress=True, n_jobs=1):
        #print('feature_table: ', self.feature_table)
        #print('rules: ', self.rules)
        #print('rule_source: ', self.rule_source)
        self.validate_types_params_tables(ltable, rtable,
		       l_output_attrs, r_output_attrs, l_output_prefix,
		       r_output_prefix, verbose, show_progress, n_jobs)

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # validate input parameters
        self.validate_output_attrs(ltable, rtable, l_output_attrs, r_output_attrs)

        # get and validate metadata
        log_info(logger, 'Required metadata: ltable key, rtable key', verbose)

        # # get metadata
        l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)


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

    def block_tables_without_filters(self, l_df, r_df, l_key, r_key,
                                     l_output_attrs, r_output_attrs,
                                     l_output_prefix, r_output_prefix,
                                     verbose, show_progress, n_jobs):

        # do blocking

        # # determine the number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs)

        candset = None
        global global_rb
        global_rb = self
        if n_procs <= 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix, r_output_prefix, #self,
                                          show_progress)
        else:
            # multiprocessing
            m, n = self.get_split_params(n_procs)
            # safeguard against very small tables
            m, n = min(m, len(l_df)), min(n, len(r_df))
            l_splits = pd.np.array_split(l_df, m)
            r_splits = pd.np.array_split(r_df, n)
            c_splits = Parallel(n_jobs=m*n)(delayed(_block_tables_split)(l, r,
                                                l_key, r_key, 
                                                l_output_attrs, r_output_attrs,
                                                l_output_prefix, r_output_prefix, #self,
                                                show_progress)
                                                for l in l_splits for r in r_splits)
            candset = pd.concat(c_splits, ignore_index=True)

        global_rb = None

        # return candidate set
        return candset



    def block_candset(self, candset, verbose=True, show_progress=True, n_jobs=1):

        # validate data types of input parameters
        self.validate_types_params_candset(candset, verbose, show_progress, n_jobs)

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, ' +
                         'fk rtable, ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # do blocking

        # # initialize the progress bar
        if show_progress:
            bar = pyprind.ProgBar(len(candset))

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # get attributes to project
        l_proj_attrs, r_proj_attrs = self.get_attrs_to_project(l_key, r_key,
                                                               [], [])
        l_df, r_df = l_df[l_proj_attrs], r_df[r_proj_attrs]

        c_df = self.block_candset_with_rules(candset, l_df, r_df, l_key, r_key,
                                             fk_ltable, fk_rtable, self.rules,
                                             show_progress) 

        # update catalog
        cm.set_candset_properties(c_df, key, fk_ltable, fk_rtable, ltable, rtable)

        # return candidate set
        return c_df

    def block_candset_skd(self, candset, verbose=True, show_progress=True):

        # validate rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                                'ltable, rtable, ltable key, rtable key', verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
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
            if res == True:
                return res
        return False

    def get_attrs_to_project(self, l_key, r_key, l_output_attrs, r_output_attrs):
        l_proj_attrs = [l_key]
        r_proj_attrs = [r_key]
        if l_output_attrs:
            l_proj_attrs.extend([c for c in l_output_attrs if c not in l_proj_attrs])
        if r_output_attrs:
            r_proj_attrs.extend([c for c in r_output_attrs if c not in r_proj_attrs])
        for rule_name, conjunct_list in six.iteritems(self.rule_str):
            #print('conjunct_list: ', conjunct_list)
            for conjunct in conjunct_list:
                sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self.parse_conjunct(conjunct, rule_name)
                #print('left_attr: ', l_attr)
                if l_attr not in l_proj_attrs:
                    l_proj_attrs.append(l_attr)
                #print('right_attr: ', r_attr)
                if r_attr not in r_proj_attrs:
                    r_proj_attrs.append(r_attr)
        #print('l_proj_attrs: ', l_proj_attrs)    
        #print('r_proj_attrs: ', r_proj_attrs)
        return l_proj_attrs, r_proj_attrs

    def parse_conjunct(self, conjunct, rule_name):
        #print >> sys.stderr, 'conjunct:', conjunct
        #print >> sys.stderr, 'conjunct split:', conjunct.split()
        # @TODO: Make parsing more robust using pyparsing
        feature_table = self.rule_ft[rule_name]
        vals = conjunct.split('(')
        feature_name = vals[0].strip()
        if feature_name not in feature_table.feature_name.values:
            logger.error('Feature ' + feature_name + ' is not present in ' +
                         'supplied feature table. Cannot apply rules.')
            raise AssertionError('Feature ' + feature_name + ' is not present ' +
                                 'in supplied feature table. Cannot apply rules.')
        vals1 = vals[1].split(')')
        vals2 = vals1[1].strip()
        vals3 = vals2.split()
        operator = vals3[0].strip()
        threshold = vals3[1].strip()
        ft_df = feature_table.set_index('feature_name')
        #print('ft_df: ', ft_df.ix[feature_name])
        return (ft_df.ix[feature_name]['simfunction'],
               ft_df.ix[feature_name]['left_attribute'],
               ft_df.ix[feature_name]['right_attribute'],
               ft_df.ix[feature_name]['left_attr_tokenizer'],
               ft_df.ix[feature_name]['right_attr_tokenizer'],
               operator, threshold)

    def block_tables_with_filters(self, l_df, r_df, l_key, r_key,
                                  l_output_attrs, r_output_attrs,
                                  l_output_prefix, r_output_prefix, verbose,
                                  show_progress, n_jobs):
        for rule_name in self.rules.keys():
            # first check if a rule is filterable
            print('Trying first rule: ', rule_name)
            if self.is_rule_filterable(rule_name):
                print('Rule is filterable: ', rule_name)
                candset = self.apply_filterable_rule(rule_name, l_df, r_df,
                                                    l_key, r_key, l_output_attrs,
                                                    r_output_attrs, l_output_prefix,
                                                    r_output_prefix, verbose,
                                                    show_progress, n_jobs)
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
        sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self.parse_conjunct(conjunct, rule_name)
        if l_tok != r_tok:
            print('Conjunct not filterable due to tokenizer mismatch', l_tok, r_tok)
            return False
        if sim_fn not in self.filterable_sim_fns:
            print('Conjunct not filterable due to unsupported sim_fn', sim_fn)
            return False
        if op not in self.allowed_ops:
            print('Conjunct not filterable unsupported op', op)
            return False
        # conjunct is filterable
        return True

    def apply_filterable_rule(self, rule_name, l_df, r_df, l_key, r_key,
                              l_output_attrs, r_output_attrs,
                              l_output_prefix, r_output_prefix,
                              verbose, show_progress, n_jobs):
        candset = None
        conjunct_list = self.rule_str[rule_name]
        for conjunct in conjunct_list:
            sim_fn, l_attr, r_attr, l_tok, r_tok, op, th = self.parse_conjunct(conjunct, rule_name)
            tokenizer = None
            if l_tok == 'dlm_dc0':
                print('Choosing whitespace tokenizer')
                tokenizer = WhitespaceTokenizer(return_set=True)
            elif l_tok == 'qgm_3':
                print('Choosing 3gram tokenizer')
                tokenizer = QgramTokenizer(qval=3, return_set=True)
            else:
                # not supported
                print('Tokenizer not supported')
                return None
 
            join_fn = None
            if sim_fn == 'jaccard':
                join_fn = jaccard_join
            elif sim_fn == 'cosine':
                join_fn = cosine_join
            elif sim_fn == 'dice':
                join_fn == dice_join
            elif sim_fn == 'overlap':
                join_fn = overlap_join
            elif sim_fn == 'overlap_coefficient':
                join_fn = overlap_coefficient_join
            else:
                logger.info(sim_fn + ' is not filterable, so not applying fitlers to this rule')
                return None
            c_df = None
            #try:
            if True:
                c_df = join_fn(l_df, r_df, l_key, r_key, l_attr,
                               r_attr, tokenizer, float(th), l_output_attrs,
                               r_output_attrs, l_output_prefix,
                               r_output_prefix, False, n_jobs)
            #except:    
            #    logger.warning('Cannot apply filters to rule because ...')
            #    return None
            if candset is not None:
                # union the candset of this conjunct with the existing candset
                #candset = pd.merge(candset, c_df, how='outer', suffixes=('', ''),
                #                   on=[l_output_prefix + l_key, r_output_prefix + r_key])
                print('candset:', candset)
                print('c_df:', c_df)
                candset=pd.concat([candset, c_df]).drop_duplicates().reset_index(drop=True) 
            else:
                # candset from the first conjunct of the rule
                candset = c_df
        return candset

def _block_tables_split(l_df, r_df, l_key, r_key,
                        l_output_attrs, r_output_attrs,
                        l_output_prefix, r_output_prefix, #rule_based_blocker,
                        show_progress):

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
        # # get ltuple from the look up table
        ltuple = l_dict[l_t[l_id_pos]]
        for r_t in r_df.itertuples(index=False):
            # # update the progress bar
            if show_progress:
                bar.update()

            # # get rtuple from the look up dictionary
            rtuple = r_dict[r_t[r_id_pos]]

            # # apply the rules to the tuple pair
            res = global_rb.apply_rules(ltuple, rtuple)
            #res = rule_based_blocker.apply_rules(ltuple, rtuple)

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
