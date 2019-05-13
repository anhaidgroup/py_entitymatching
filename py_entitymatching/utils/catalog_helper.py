# coding=utf-8
import logging
# import numpy as np
import pandas as pd
import six

from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def check_attrs_present(table, attrs):

    validate_object_type(table, pd.DataFrame)

    if attrs is None:
        logger.warning('Input attr. list is null')
        return False

    if isinstance(attrs, list) is False:
        attrs = [attrs]
    status = are_all_attrs_in_df(table, attrs, verbose=True)
    return status

def are_all_attrs_in_df(df, col_names, verbose=False):

    validate_object_type(df, pd.DataFrame)

    if col_names is None:
        logger.warning('Input col_names is null')
        return False

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
    validate_object_type(df, pd.DataFrame)

    validate_object_type(attr, six.string_types, error_prefix='Input attr.')

    uniq_flag = (len(pd.unique(df[attr])) == len(df))
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
    validate_object_type(df, pd.DataFrame)

    validate_object_type(attr, six.string_types, error_prefix='Input attr.')

    # nan_flag = (sum(df[attr].isnull()) != 0)
    nan_flag = any(pd.isnull(df[attr]))
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
    validate_object_type(df, pd.DataFrame)

    validate_object_type(attr, six.string_types, error_prefix='Input attr.')

    # check if the length is > 0
    if len(df) > 0:
        # check for uniqueness
        uniq_flag = len(pd.unique(df[attr])) == len(df)
        if not uniq_flag:
            if verbose:
                logger.warning('Attribute ' + attr + ' does not contain unique values')
            return False

        # check if there are missing or null values
        # nan_flag = sum(df[attr].isnull()) == 0
        nan_flag = not any(pd.isnull(df[attr]))
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
    validate_object_type(df_foreign, pd.DataFrame, error_prefix='Input object (df_foreign)')

    validate_object_type(attr_foreign, six.string_types, error_prefix='Input attr (attr_foreign)')

    validate_object_type(df_base, pd.DataFrame, error_prefix='Input object (df_base)')

    validate_object_type(attr_base, six.string_types, error_prefix='Input attr (attr_base)')

    if not check_attrs_present(df_base, attr_base):
        logger.warning('The attribute %s is not in df_base' %attr_base)
        return False

    if not check_attrs_present(df_foreign, attr_foreign):
        logger.error('Input attr (attr_foreign) is not in df_foreign')
        return False

    if any(pd.isnull(df_foreign[attr_foreign])):
        logger.warning('The attribute %s in foreign table contains null values' %attr_foreign)
        return False

    uniq_fk_vals = set(pd.unique(df_foreign[attr_foreign]))
    base_attr_vals = df_base[attr_base].values
    d = uniq_fk_vals.difference(base_attr_vals)
    if len(d) > 0:
        logger.warning('For some attr. values in (%s) in the foreign table there are no values in '
                       '(%s) in the base table' %(attr_foreign, attr_base))
        return False

    # check whether those values are unique in the base table.
    t = df_base[df_base[attr_base].isin(pd.unique(df_foreign[attr_foreign]))]
    status = is_key_attribute(t, attr_base)

    if status == False:
        logger.warning('Key attr. constraint for the subset of values (derived from. %s)'
                       'in %s is not satisifed' %(attr_foreign, attr_base))
        return False
    else:
        return True



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
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    return len(df) > 0


def get_name_for_key(columns, key_val='_id'):
    k = key_val
    i = 0

    # try attribute name of the form "_id", "_id0", "_id1", ... and
    # return the first available name

    while True:
        if k not in columns:
            break
        else:
            k = key_val + str(i)
        i += 1
    return k


def add_key_column(table, key):
    validate_object_type(table, pd.DataFrame)

    validate_object_type(key, six.string_types, error_prefix='Input key')

    table.insert(0, key, range(0, len(table)))
    return table