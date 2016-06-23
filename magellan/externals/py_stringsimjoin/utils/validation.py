"""Validation utilities"""

import pandas as pd

from py_stringmatching.tokenizer.tokenizer import Tokenizer


def validate_input_table(table, table_label):
    """Check if the input table is a dataframe."""
    if not isinstance(table, pd.DataFrame):
        raise TypeError(table_label + ' is not a dataframe')
    return True


def validate_attr(attr, table_cols, attr_label, table_label):
    """Check if the attribute exists in the table."""
    if attr not in table_cols:
        raise AssertionError(attr_label + ' \'' + attr + '\' not found in ' + \
                             table_label) 
    return True


def validate_key_attr(key_attr, table, table_label):
    """Check if the attribute is a valid key attribute."""
    unique_flag = len(table[key_attr].unique()) == len(table)
    nan_flag = sum(table[key_attr].isnull()) == 0 
    if not (unique_flag and nan_flag):
        raise AssertionError('\'' + key_attr + '\' is not a key attribute ' + \
                             'in ' + table_label)
    return True


def validate_output_attrs(l_out_attrs, l_columns, r_out_attrs, r_columns):
    """Check if the output attributes exist in the original tables."""
    if l_out_attrs:
        for attr in l_out_attrs:
            if attr not in l_columns:
                raise AssertionError('output attribute \'' + attr + \
                                     '\' not found in left table')

    if r_out_attrs:
        for attr in r_out_attrs:
            if attr not in r_columns:
                raise AssertionError('output attribute \'' + attr + \
                                     '\' not found in right table')
    return True


def validate_threshold(threshold, sim_measure_type):
    """Check if the threshold is valid for the sim_measure_type."""
    if sim_measure_type == 'OVERLAP' or sim_measure_type == 'EDIT_DISTANCE':
        if threshold <= 0:
            raise AssertionError('threshold for ' + sim_measure_type + \
                                 ' should be greater than 0')
    else:
        if threshold <= 0 or threshold > 1:
            raise AssertionError('threshold for ' + sim_measure_type + \
                                 ' should be in (0, 1]')
    return True


def validate_tokenizer(tokenizer):
    """Check if the input tokenizer is a valid tokenizer."""
    if not isinstance(tokenizer, Tokenizer):
        raise TypeError('Invalid tokenizer provided as input')
    return True


def validate_sim_measure_type(sim_measure_type):
    """Check if the input sim_measure_type is one of the supported types."""
    sim_measure_types = ['COSINE', 'DICE', 'EDIT_DISTANCE', 'JACCARD',
                         'OVERLAP']
    if sim_measure_type not in sim_measure_types:
        raise TypeError('\'' + sim_measure_type + '\' is not a valid ' + \
                        'sim_measure_type. Supported types are COSINE, DICE' + \
                        ', EDIT_DISTANCE, JACCARD and OVERLAP.')
    return True 
