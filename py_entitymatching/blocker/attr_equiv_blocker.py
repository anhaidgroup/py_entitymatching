import logging

import pandas as pd
import numpy as np
import pyprind
import six
from joblib import Parallel, delayed

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.blocker.blocker import Blocker
from py_entitymatching.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from py_entitymatching.utils.generic_helper import rem_nan
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


class AttrEquivalenceBlocker(Blocker):
    """
    Blocks based on the equivalence of attribute values.
    """

    def block_tables(self, ltable, rtable, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None,
                     l_output_prefix='ltable_', r_output_prefix='rtable_',
                     allow_missing=False, verbose=False, n_jobs=1):
        """Blocks two tables based on attribute equivalence.

        Conceptually, this will check `l_block_attr=r_block_attr` for each tuple
        pair from the Cartesian product of tables `ltable` and `rtable`. It outputs a
        Pandas dataframe object with tuple pairs that satisfy the equality condition.
        The dataframe will include attributes '_id', key attribute from
        ltable, key attributes from rtable, followed by lists `l_output_attrs` and
        `r_output_attrs` if they are specified. Each of these output and key attributes will be
        prefixed with given `l_output_prefix` and `r_output_prefix`. If `allow_missing` is set
        to `True` then all tuple pairs with missing value in at least one of the tuples will be
        included in the output dataframe.
        Further, this will update the following metadata in the catalog for the output table:
        (1) key, (2) ltable, (3) rtable, (4) fk_ltable, and (5) fk_rtable.

        Args:
            ltable (DataFrame): The left input table.

            rtable (DataFrame): The right input table.

            l_block_attr (string): The blocking attribute in left table.

            r_block_attr (string): The blocking attribute in right table.

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

            allow_missing (boolean): A flag to indicate whether tuple pairs
                                     with missing value in at least one of the
                                     blocking attributes should be included in
                                     the output candidate set (defaults to
                                     False). If this flag is set to True, a
                                     tuple in ltable with missing value in the
                                     blocking attribute will be matched with
                                     every tuple in rtable and vice versa.

            verbose (boolean): A flag to indicate whether the debug information
                should be logged (defaults to False).


            n_jobs (int): The number of parallel jobs to be used for computation
                (defaults to 1). If -1 all CPUs are used. If 0 or 1,
                no parallel computation is used at all, which is useful for
                debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
                used (where n_cpus is the total number of CPUs in the
                machine). Thus, for n_jobs = -2, all CPUs but one are used.
                If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
                computation is used (i.e., equivalent to the default).

        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).

        Raises:
            AssertionError: If `ltable` is not of type pandas
                DataFrame.
            AssertionError: If `rtable` is not of type pandas
                DataFrame.
            AssertionError: If `l_block_attr` is not of type string.
            AssertionError: If `r_block_attr` is not of type string.
            AssertionError: If `l_output_attrs` is not of type of
                list.
            AssertionError: If `r_output_attrs` is not of type of
                list.
            AssertionError: If the values in `l_output_attrs` is not of type
                string.
            AssertionError: If the values in `r_output_attrs` is not of type
                string.
            AssertionError: If `l_output_prefix` is not of type
                string.
            AssertionError: If `r_output_prefix` is not of type
                string.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `allow_missing` is not of type boolean.
            AssertionError: If `n_jobs` is not of type
                int.
            AssertionError: If `l_block_attr` is not in the ltable columns.
            AssertionError: If `r_block_attr` is not in the rtable columns.
            AssertionError: If `l_out_attrs` are not in the ltable.
            AssertionError: If `r_out_attrs` are not in the rtable.

        Examples:
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ab = em.AttrEquivalenceBlocker()
            >>> C1 = ab.block_tables(A, B, 'zipcode', 'zipcode', l_output_attrs=['name'], r_output_attrs=['name'])
            # Include all possible tuple pairs with missing values
            >>> C2 = ab.block_tables(A, B, 'zipcode', 'zipcode', l_output_attrs=['name'], r_output_attrs=['name'], allow_missing=True)


        """

        # validate data types of input parameters
        self.validate_types_params_tables(ltable, rtable,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix,
                                          r_output_prefix, verbose, n_jobs)

        # validate data types of input blocking attributes
        self.validate_types_block_attrs(l_block_attr, r_block_attr)

        # validate data type of allow_missing
        self.validate_allow_missing(allow_missing)

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
        cm._validate_metadata_for_table(ltable, l_key, 'ltable', logger,
                                        verbose)
        cm._validate_metadata_for_table(rtable, r_key, 'rtable', logger,
                                        verbose)

        # do blocking

        # # do projection of required attributes from the tables
        l_proj_attrs = self.get_attrs_to_project(l_key, l_block_attr,
                                                 l_output_attrs)
        ltable_proj = ltable[l_proj_attrs]
        r_proj_attrs = self.get_attrs_to_project(r_key, r_block_attr,
                                                 r_output_attrs)
        rtable_proj = rtable[r_proj_attrs]

        # # remove records with nans in the blocking attribute
        l_df = rem_nan(ltable_proj, l_block_attr)
        r_df = rem_nan(rtable_proj, r_block_attr)

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(l_df) * len(r_df))

        if n_procs <= 1:
            # single process
            candset = _block_tables_split(l_df, r_df, l_key, r_key,
                                          l_block_attr, r_block_attr,
                                          l_output_attrs, r_output_attrs,
                                          l_output_prefix, r_output_prefix,
                                          allow_missing)
        else:
            # multiprocessing
            m, n = self.get_split_params(n_procs, len(l_df), len(r_df))
            l_splits = np.array_split(l_df, m)
            r_splits = np.array_split(r_df, n)
            c_splits = Parallel(n_jobs=m * n)(
                delayed(_block_tables_split)(l, r, l_key, r_key,
                                             l_block_attr, r_block_attr,
                                             l_output_attrs, r_output_attrs,
                                             l_output_prefix, r_output_prefix,
                                             allow_missing)
                for l in l_splits for r in r_splits)
            candset = pd.concat(c_splits, ignore_index=True)

        # if allow_missing flag is True, then compute
        # all pairs with missing value in left table, and
        # all pairs with missing value in right table
        if allow_missing:
            missing_pairs = self.get_pairs_with_missing_value(ltable_proj,
                                                              rtable_proj,
                                                              l_key, r_key,
                                                              l_block_attr,
                                                              r_block_attr,
                                                              l_output_attrs,
                                                              r_output_attrs,
                                                              l_output_prefix,
                                                              r_output_prefix)
            candset = pd.concat([candset, missing_pairs], ignore_index=True)

        # update catalog
        key = get_name_for_key(candset.columns)
        candset = add_key_column(candset, key)
        cm.set_candset_properties(candset, key, l_output_prefix + l_key,
                                  r_output_prefix + r_key, ltable, rtable)

        # return candidate set
        return candset

    def block_candset(self, candset, l_block_attr, r_block_attr,
                      allow_missing=False, verbose=False, show_progress=True,
                      n_jobs=1):
        """Blocks an input candidate set of tuple pairs based on attribute equivalence.

        Finds tuple pairs from an input candidate set of tuple pairs
        such that the value of attribute l_block_attr of the left tuple in a
        tuple pair exactly matches the value of attribute r_block_attr of the 
        right tuple in the tuple pair.

        Args:
            candset (DataFrame): The input candidate set of tuple pairs.

            l_block_attr (string): The blocking attribute in left table.

            r_block_attr (string): The blocking attribute in right table.

            allow_missing (boolean): A flag to indicate whether tuple pairs
                                     with missing value in at least one of the
                                     blocking attributes should be included in
                                     the output candidate set (defaults to
                                     False). If this flag is set to True, a
                                     tuple pair with missing value in either
                                     blocking attribute will be retained in the
                                     output candidate set.

            verbose (boolean): A flag to indicate whether the debug
                information should be logged (defaults to False).

            show_progress (boolean): A flag to indicate whether progress should
                                     be displayed to the user (defaults to True).

            n_jobs (int): The number of parallel jobs to be used for computation
                (defaults to 1). If -1 all CPUs are used. If 0 or 1,
                no parallel computation is used at all, which is useful for
                debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
                used (where n_cpus is the total number of CPUs in the
                machine). Thus, for n_jobs = -2, all CPUs but one are used.
                If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
                computation is used (i.e., equivalent to the default).

        Returns:
            A candidate set of tuple pairs that survived blocking (DataFrame).

        Raises:
            AssertionError: If `candset` is not of type pandas
                DataFrame.
            AssertionError: If `l_block_attr` is not of type string.
            AssertionError: If `r_block_attr` is not of type string.
            AssertionError: If `verbose` is not of type
                boolean.
            AssertionError: If `n_jobs` is not of type
                int.
            AssertionError: If `l_block_attr` is not in the ltable columns.
            AssertionError: If `r_block_attr` is not in the rtable columns.

        Examples:
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ab = em.AttrEquivalenceBlocker()
            >>> C = ab.block_tables(A, B, 'zipcode', 'zipcode', l_output_attrs=['name'], r_output_attrs=['name'])

            >>> D1 = ab.block_candset(C, 'age', 'age', allow_missing=True)
            # Include all possible tuple pairs with missing values
            >>> D2 = ab.block_candset(C, 'age', 'age', allow_missing=True)
            # Execute blocking using multiple cores
            >>> D3 = ab.block_candset(C, 'age', 'age', n_jobs=-1)

        """

        # validate data types of input parameters
        self.validate_types_params_candset(candset, verbose, show_progress,
                                           n_jobs)

        # validate data types of input blocking attributes
        self.validate_types_block_attrs(l_block_attr, r_block_attr)

        # get and validate metadata
        log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                         'fk rtable, ltable, rtable, ltable key, rtable key',
                 verbose)

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(
            candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable,
                                          ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # validate input parameters
        self.validate_block_attrs(ltable, rtable, l_block_attr, r_block_attr)

        # do blocking

        # # do projection before merge
        l_df = ltable[[l_key, l_block_attr]]
        r_df = rtable[[r_key, r_block_attr]]

        # # set index for convenience
        l_df = l_df.set_index(l_key, drop=False)
        r_df = r_df.set_index(r_key, drop=False)

        # # determine number of processes to launch parallely
        n_procs = self.get_num_procs(n_jobs, len(candset))

        valid = []
        if n_procs <= 1:
            # single process
            valid = _block_candset_split(candset, l_df, r_df, l_key, r_key,
                                         l_block_attr, r_block_attr, fk_ltable,
                                         fk_rtable, allow_missing, show_progress)
        else:
            c_splits = np.array_split(candset, n_procs)
            valid_splits = Parallel(n_jobs=n_procs)(
                delayed(_block_candset_split)(c_splits[i],
                                              l_df, r_df,
                                              l_key, r_key,
                                              l_block_attr, r_block_attr,
                                              fk_ltable, fk_rtable, allow_missing,
                                              show_progress and i == len(
                                                  c_splits) - 1)
                for i in range(len(c_splits)))
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

    def block_tuples(self, ltuple, rtuple, l_block_attr, r_block_attr,
                     allow_missing=False):
        """Blocks a tuple pair based on attribute equivalence.

        Args:
            ltuple (Series): The input left tuple.

            rtuple (Series): The input right tuple.
            
            l_block_attr (string): The blocking attribute in left tuple.

            r_block_attr (string): The blocking attribute in right tuple.

            allow_missing (boolean): A flag to indicate whether a tuple pair
                                     with missing value in at least one of the
                                     blocking attributes should be blocked
                                     (defaults to False). If this flag is set
                                     to True, the pair will be kept if either
                                     ltuple has missing value in l_block_attr
                                     or rtuple has missing value in r_block_attr
                                     or both.

        Returns:
            A status indicating if the tuple pair is blocked, i.e., the values
            of l_block_attr in ltuple and r_block_attr in rtuple are different
            (boolean).

        Examples:
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
            >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
            >>> ab = em.AttrEquivalenceBlocker()
            >>> status = ab.block_tuples(A.loc[0], B.loc[0], 'zipcode', 'zipcode')
        """
        l_val, r_val = ltuple[l_block_attr], rtuple[r_block_attr]
        if allow_missing:
            if pd.isnull(l_val) or pd.isnull(r_val) or l_val == r_val:
                return False
            else:
                return True
        else:
            if pd.notnull(l_val) and pd.notnull(r_val) and l_val == r_val:
                return False
            else:
                return True


    # ------------------------------------------------------------
    # utility functions specific to attribute equivalence blocking

    # validate the data types of the blocking attributes 
    def validate_types_block_attrs(self, l_block_attr, r_block_attr):
        validate_object_type(l_block_attr, six.string_types, error_prefix='Blocking attribute name of left table')
        validate_object_type(r_block_attr, six.string_types, error_prefix='Blocking attribute name of right table')

    # validate the blocking attributes
    def validate_block_attrs(self, ltable, rtable, l_block_attr, r_block_attr):
        if l_block_attr not in ltable.columns:
            raise AssertionError(
                'Left block attribute is not in the left table')

        if r_block_attr not in rtable.columns:
            raise AssertionError(
                'Right block attribute is not in the right table')

    def get_pairs_with_missing_value(self, l_df, r_df, l_key, r_key,
                                     l_block_attr, r_block_attr,
                                     l_output_attrs, r_output_attrs,
                                     l_output_prefix, r_output_prefix):

        l_df.is_copy, r_df.is_copy = False, False  # to avoid setwithcopy warning
        l_df['ones'] = np.ones(len(l_df))
        r_df['ones'] = np.ones(len(r_df))

        # find ltable records with missing value in l_block_attr
        l_df_missing = l_df[pd.isnull(l_df[l_block_attr])]

        # find ltable records with no missing value in l_block_attr
        l_df_no_missing = l_df[pd.notnull(l_df[l_block_attr])]

        # find rtable records with missing value in r_block_attr
        r_df_missing = r_df[pd.isnull(r_df[r_block_attr])]

        missing_pairs_1 = pd.merge(l_df_missing, r_df, left_on='ones',
                                   right_on='ones',
                                   suffixes=('_ltable', '_rtable'))

        missing_pairs_2 = pd.merge(l_df_no_missing, r_df_missing,
                                   left_on='ones',
                                   right_on='ones',
                                   suffixes=('_ltable', '_rtable'))

        missing_pairs = pd.concat([missing_pairs_1, missing_pairs_2],
                                  ignore_index=True)

        retain_cols, final_cols = _output_columns(l_key, r_key,
                                                  list(missing_pairs.columns),
                                                  l_output_attrs,
                                                  r_output_attrs,
                                                  l_output_prefix,
                                                  r_output_prefix)
        missing_pairs = missing_pairs[retain_cols]
        missing_pairs.columns = final_cols
        return missing_pairs


def _block_tables_split(l_df, r_df, l_key, r_key, l_block_attr, r_block_attr,
                        l_output_attrs, r_output_attrs, l_output_prefix,
                        r_output_prefix, allow_missing):
    # perform an inner join of the two data frames with no missing values
    candset = pd.merge(l_df, r_df, left_on=l_block_attr,
                       right_on=r_block_attr, suffixes=('_ltable', '_rtable'))

    retain_cols, final_cols = _output_columns(l_key, r_key,
                                              list(candset.columns),
                                              l_output_attrs, r_output_attrs,
                                              l_output_prefix, r_output_prefix)
    candset = candset[retain_cols]
    candset.columns = final_cols
    return candset


def _block_candset_split(c_df, l_df, r_df, l_key, r_key,
                         l_block_attr, r_block_attr, fk_ltable, fk_rtable,
                         allow_missing, show_progress):
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
            l_dict[row_lkey] = l_df.loc[row_lkey, l_block_attr]
        l_val = l_dict[row_lkey]

        row_rkey = row[rkey_idx]
        if row_rkey not in r_dict:
            r_dict[row_rkey] = r_df.loc[row_rkey, r_block_attr]
        r_val = r_dict[row_rkey]

        if allow_missing:
            if pd.isnull(l_val) or pd.isnull(r_val) or l_val == r_val:
                valid.append(True)
            else:
                valid.append(False)
        else:
            if pd.notnull(l_val) and pd.notnull(r_val) and l_val == r_val:
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
