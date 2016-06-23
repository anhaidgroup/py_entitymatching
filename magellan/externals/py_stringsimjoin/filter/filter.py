from joblib import delayed
from joblib import Parallel
import pandas as pd

from magellan.externals.py_stringsimjoin.utils.helper_functions import build_dict_from_table
from magellan.externals.py_stringsimjoin.utils.helper_functions import split_table


class Filter(object):
    """Filter base class.
    """

    def filter_candset(self, candset,
                       candset_l_key_attr, candset_r_key_attr,
                       ltable, rtable,
                       l_key_attr, r_key_attr,
                       l_filter_attr, r_filter_attr,
                       n_jobs=1):
        """Finds candidate matching pairs of strings from the input candset.

        Args:
            candset (dataframe): input candidate set.

            candset_l_key_attr (string): attribute in candidate set that is a key in left table.

            candset_r_key_attr (string): attribute in candidate set that is a key in right table.

            ltable (dataframe): left input table.

            rtable (dataframe): right input table.

            l_key_attr (string): key attribute in left table.

            r_key_attr (string): key attribute in right table.

            l_filter_attr (string): attribute to be used by the filter, in left table.

            r_filter_attr (string): attribute to be used by the filter,  in right table.

            n_jobs (int): The number of jobs to use for the computation (defaults to 1).                                                                                            
                If -1 all CPUs are used. If 1 is given, no parallel computing code is used at all, 
                which is useful for debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used. 
                Thus for n_jobs = -2, all CPUs but one are used. If (n_cpus + 1 + n_jobs) becomes less than 1,
                then n_jobs is set to 1.

        Returns:
            output table (dataframe)
        """

        # check for empty candset
        if candset.empty:
            return candset
        if n_jobs == 1:
            return _filter_candset_split(candset,
                                         candset_l_key_attr, candset_r_key_attr,
                                         ltable, rtable,
                                         l_key_attr, r_key_attr,
                                         l_filter_attr, r_filter_attr,
                                         self)
        else:
            candset_splits = split_table(candset, n_jobs)
            results = Parallel(n_jobs=n_jobs)(delayed(_filter_candset_split)(
                                      candset_split,
                                      candset_l_key_attr, candset_r_key_attr,
                                      ltable, rtable,
                                      l_key_attr, r_key_attr,
                                      l_filter_attr, r_filter_attr,
                                      self)
                                  for candset_split in candset_splits)
            return pd.concat(results)


def _filter_candset_split(candset,
                          candset_l_key_attr, candset_r_key_attr,
                          ltable, rtable,
                          l_key_attr, r_key_attr,
                          l_filter_attr, r_filter_attr,
                          filter_object):
    # Find column indices of key attr and filter attr in ltable
    l_columns = list(ltable.columns.values)
    l_key_attr_index = l_columns.index(l_key_attr)
    l_filter_attr_index = l_columns.index(l_filter_attr)

    # Find column indices of key attr and filter attr in rtable
    r_columns = list(rtable.columns.values)
    r_key_attr_index = r_columns.index(r_key_attr)
    r_filter_attr_index = r_columns.index(r_filter_attr)
    
    # Build a dictionary on ltable
    ltable_dict = build_dict_from_table(ltable, l_key_attr_index,
                                        l_filter_attr_index,
                                        remove_null=False, remove_empty=False)

    # Build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_filter_attr_index,
                                        remove_null=False, remove_empty=False)

    # Find indices of l_key_attr and r_key_attr in candset
    candset_columns = list(candset.columns.values)
    candset_l_key_attr_index = candset_columns.index(candset_l_key_attr)
    candset_r_key_attr_index = candset_columns.index(candset_r_key_attr)

    valid_rows = []

    for candset_row in candset.itertuples(index = False):
        l_id = candset_row[candset_l_key_attr_index]
        r_id = candset_row[candset_r_key_attr_index]

        l_row = ltable_dict[l_id]
        r_row = rtable_dict[r_id]
        if (pd.isnull(l_row[l_filter_attr_index]) or
            pd.isnull(r_row[r_filter_attr_index]) or
            len(str(l_row[l_filter_attr_index])) == 0 or
            len(str(r_row[r_filter_attr_index])) == 0):
            valid_rows.append(False)
        else:
            valid_rows.append(not filter_object.filter_pair(
                                                    l_row[l_filter_attr_index],
                                                    r_row[r_filter_attr_index]))

    return candset[valid_rows]
