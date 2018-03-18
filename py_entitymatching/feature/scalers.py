"""
This module contains functions for re-scaling/normalizing features.
"""
import six
import logging
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler
from py_entitymatching.utils.catalog_helper import check_attrs_present
from py_entitymatching.utils.generic_helper import list_diff, list_drop_duplicates
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def scale_features(table=None, exclude_attrs=None,
                   scaling_method=None, scaler=None):
    """
    Scale features with the specified scaling function.
    """
    project_attrs = _attrs_to_project(table, exclude_attrs)
    table[project_attrs], scaler = scale_vectors(table[project_attrs],
                                                 scaling_method, scaler)

    return table, scaler


def scale_vectors(x, scaling_method=None, scaler=None):
    """
    Scale X with the specified scaling function.
    """
    if scaler is not None:
        try:
            x = scaler.transform(x)
        except ValueError("Error with scaler!"):
            return x
    elif scaling_method is None:
        # no scaling is specified
        pass
    elif isinstance(scaling_method, six.string_types):
        # get the lookup table for scaling methods
        lookup_table = _get_scaler_funs()
        if scaling_method in lookup_table:
            scale = lookup_table[scaling_method]
            scaler = scale().fit(x)
            x = scaler.transform(x)
        else:
            raise AssertionError('Scaling method is not present in lookup table')
    else:
        raise AssertionError('Missing scaling methods or' 
                             'wrong data type of scaling method found')
    return x, scaler


def _get_scaler_funs():
    """
    This function returns the scaling functions specified by scaling method.

    """
    # Get all the scaling methods
    scaler_names = ['MinMax',
                    'MaxAbs',
                    'Standard']
    # Get all the scaling functions
    scaler_funs = [MinMaxScaler,
                   MaxAbsScaler,
                   StandardScaler]
    # Return a dictionary with the functions names as the key and the actual
    # functions as values.
    return dict(zip(scaler_names, scaler_funs))


def _attrs_to_project(table, exclude_attrs):
    """
    Gets X that can be used for scikit-feature function based on raw table
    """
    # Validate the input parameters
    # # We expect the input table to be of type pandas DataFrame
    validate_object_type(table, pd.DataFrame)
    # We expect exclude attributes to be of type list. If not convert it into
    #  a list.
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    # Check if the exclude attributes are present in the input table
    if not check_attrs_present(table, exclude_attrs):
        logger.error('The attributes mentioned in exclude_attrs '
                     'is not present '
                     'in the input table')
        raise AssertionError(
            'The attributes mentioned in exclude_attrs '
            'is not present '
            'in the input table')

    # Drop the duplicates from the exclude attributes
    exclude_attrs = list_drop_duplicates(exclude_attrs)

    # Project the list of attributes that should be used for scikit-learn's
    # functions.
    attrs_to_project = list_diff(list(table.columns), exclude_attrs)

    return attrs_to_project
