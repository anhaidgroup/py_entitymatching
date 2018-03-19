"""
This module contains functions for re-scaling/normalizing features.
"""
import six
import logging

from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler
from py_entitymatching.feature.attributeutils import get_attrs_to_project


logger = logging.getLogger(__name__)


def scale_features(table=None, exclude_attrs=None,
                   scaling_method=None, scaler=None):
    """
    Scale features with the specified scaling function.
    """
    project_attrs = get_attrs_to_project(table=table, exclude_attrs=exclude_attrs)
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
        except RuntimeError:
            print('Could not transform the feature vectors with the specified scaler')
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
