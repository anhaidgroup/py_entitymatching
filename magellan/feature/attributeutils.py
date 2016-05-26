import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)


def get_attr_types(table):
    if not isinstance(table, pd.DataFrame):
        logger.error('Input table is not of type pandas dataframe')
        raise AssertionError('Input table is not of type pandas dataframe')

    type_list = [get_type(table[col]) for col in table.columns]
    d = dict(zip(table.columns, type_list))
    d['_table'] = table
    return d


def get_attr_corres(table_a, table_b):
    if not isinstance(table_a, pd.DataFrame):
        logger.error('Input table_a is not of type pandas dataframe')
        raise AssertionError('Input table_a is not of type pandas dataframe')

    if not isinstance(table_b, pd.DataFrame):
        logger.error('Input table_b is not of type pandas dataframe')
        raise AssertionError('Input table_b is not of type pandas dataframe')

    ret_list = []
    for c in table_a.columns:
        if c in table_b.columns:
            ret_list.append((c, c))
    d = dict()
    d['corres'] = ret_list
    d['ltable'] = table_a
    d['rtable'] = table_b
    return d


# Given a pandas series (i.e column in pandas dataframe) obtain its type

def get_type(col):
    if not isinstance(col, pd.Series):
        raise ValueError('Input is not of type pandas series')
    # drop NAs
    col = col.dropna()
    # get type for each element and convert it into a set
    type_list = list(set(col.map(type).tolist()))

    if len(type_list) == 0:
        logger.warning('Column %s does not seem to qualify as any atomic type. '
                       'It may contain all NaNs. Currently, setting its type to '
                       'be numeric.We recommend the users to manually update '
                       'the returned types or features later. \n' % col.name)
        return 'numeric'

    if len(type_list) > 1:
        logger.error('Column %s qualifies to be more than one type (%s). \n'
                        'Please explicitly set the column type like this:\n'
                        'A["address"] = A["address"].astype(str) \n'
                        'Similarly use int, float, boolean types.' % (col.name, ', '.join(type_list)))

        raise TypeError('Column %s qualifies to be more than one type (%s). \n'
                        'Please explicitly set the column type like this:\n'
                        'A["address"] = A["address"].astype(str) \n'
                        'Similarly use int, float, boolean types.' % (col.name, ', '.join(type_list)))
    else:
        t = type_list[0]
        if t == bool:
            return 'boolean'
        # consider string and unicode as same
        elif t == str or t == unicode:
            # get average token length
            avg_tok_len = pd.Series.mean(col.str.split(' ').apply(len_handle_nan))
            if avg_tok_len == 1:
                return "str_eq_1w"
            elif avg_tok_len <= 5:
                return "str_bt_1w_5w"
            elif avg_tok_len <= 10:
                return "str_bt_5w_10w"
            else:
                return "str_gt_10w"
        else:
            return "numeric"


# Get the length of list, handling NaN
def len_handle_nan(v):
    if isinstance(v, list):
        return len(v)
    else:
        return np.NaN
