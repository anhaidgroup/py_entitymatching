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
        """Filter candidate set.

        Args:
        candset : Pandas data frame
        candset_l_key_attr, candset_r_key_attr : String, key attributes in candset (that refer to ltable and rtable)
        ltable, rtable : Pandas data frame, base tables from which candset was obtained
        l_key_attr, r_key_attr : String, key attribute from ltable and rtable
        l_filter_attr, r_filter_attr : String, filter attribute from ltable and rtable

        Returns:
        result : Pandas data frame
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
                                        l_filter_attr_index)

    # Build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_filter_attr_index)

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
            pd.isnull(r_row[r_filter_attr_index])):
            valid_rows.append(False)
        else:
            valid_rows.append(not filter_object.filter_pair(
                                                    l_row[l_filter_attr_index],
                                                    r_row[r_filter_attr_index]))

    return candset[valid_rows]
