# coding=utf-8
import logging
import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)


def check_attrs_present(table, attrs):
    if isinstance(attrs, list) is False:
        attrs = [attrs]
    status = are_all_attrs_in_df(table, attrs, verbose=True)
    return status

def are_all_attrs_in_df(df, col_names, verbose=False):
    df_columns_names = list(df.columns)
    for c in col_names:
        if c not in df_columns_names:
            if verbose:
                logger.warning('Column name (' +c+ ') is not present in dataframe')
            return False
    return True

def log_info(lgr, s, verbose):
    if verbose:
        lgr.info(s)

def is_attr_unique(df, attr):
    """
    Check if the attribute is unique in a dataframe

    Args:
        df (pandas dataframe): Input dataframe
        attr (str): Attribute in the pandas dataframe

    Returns:
        result (bool). Returns True, if the attribute contains unique values, else returns False

    Notes:
        This is an internal helper function

    """
    uniq_flag = len(np.unique(df[attr])) == len(df)
    if not uniq_flag:
        return False
    else:
        return True


def does_contain_missing_vals(df, attr):
    """
    Check if the attribute contains missing values in the dataframe

    Args:
        df (pandas dataframe): Input dataframe
        attr (str): Attribute in the pandas dataframe

    Returns:
        result (bool). Returns True, if the attribute contains missing values, else returns False

    Notes:
        This is an internal helper function

    """
    nan_flag = sum(df[attr].isnull()) == 0
    if not nan_flag:
        return False
    else:
        return True

def is_key_attribute(df, attr, verbose=False):
    """
    Check if an attribute is a key attribute
    Args:
        df (pandas dataframe): Input dataframe
        attr (str): Attribute in the pandas dataframe
        verbose (bool):  Flag to indicate whether warnings should be printed out

    Returns:
        result (bool). Returns True, if the attribute is a key attribute (unique and without missing values), else
         returns False

    """
    # check if the length is > 0
    if len(df) > 0:
        # check for uniqueness
        uniq_flag = len(np.unique(df[attr])) == len(df)
        if not uniq_flag:
            if verbose:
                logger.warning('Attribute ' + attr + ' does not contain unique values')
            return False

        # check if there are missing or null values
        nan_flag = sum(df[attr].isnull()) == 0
        if not nan_flag:
            if verbose:
                logger.warning('Attribute ' + attr + ' contains missing values')
            return False
        return uniq_flag and nan_flag
    else:
        return True


def check_fk_constraint(df_foreign, attr_foreign, df_base, attr_base):
    """
    Check if the foreign key is a primary key
    Args:
        df_foreign (pandas dataframe): Foreign dataframe
        attr_foreign (str): Attribute in the foreign dataframe
        df_base (pandas dataframe): Base dataframe
        attr_base (str): Attribute in the base dataframe
    Returns:
        result (bool). Returns True if the foreign key contraint is satisfied, else returns False
    Notes:
        This is an internal helper function
    """
    if isinstance(attr_base, basestring) is False:
        return False
    if check_attrs_present(df_base, attr_base) is False:
        return False
    t = df_base[df_base[attr_base].isin(pd.unique(df_foreign[attr_foreign]))]
    return is_key_attribute(t, attr_base)


def does_contain_rows(df):
    """
    Check if the dataframe is non-empty

    Args:
        df (pandas dataframe): Input dataframe

    Returns:
        result (bool). Returns True, if the length of the dataframe is greater than 0, else returns False

    Notes:
        This is an internal helper function

    """
    return len(df) > 0


def get_name_for_key(columns):
    k = '_id'
    i = 0
    # try attribute name of the form "_id", "_id0", "_id1", ... and
    # return the first available name
    while True:
        if k not in columns:
            break
        else:
            k = '_id' + str(i)
        i += 1
    return k


def add_key_column(table, key):
    table.insert(0, key, range(0, len(table)))
    return table