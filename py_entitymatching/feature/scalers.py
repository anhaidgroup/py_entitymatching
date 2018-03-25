"""
This module contains functions for re-scaling/normalizing features.
"""
import six
import logging
import numpy as np
import pandas as pd
from py_entitymatching.utils.validation_helper import validate_object_type
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler, StandardScaler
from py_entitymatching.feature.attributeutils import get_attrs_to_project


logger = logging.getLogger(__name__)


def scale_features(table, exclude_attrs=None,
                   scaling_method=None, scaler=None):
    """
    This function will re-scale/normalize features with the specified scaling function.

    Specifically, this function will project the table by excluding attributes
    that are to our concerns. Then, it will first try to apply an existing scaler
    (if any). If it fails to apply any existing scaler, it will try to fit a scaler
    with the specified scaling_method. If that failed again, it will return the table
    and a None scaler.

    Args:
        table (DataFrame): The pandas DataFrames which contains all the features,
            keys, and target.
        exclude_attrs (list): A list of attributes to be excluded from the table,
            these attributes will keep the same during re-scaling/normalizing.
        scaling_method (str): The scaling method specified by user, can be one of
            "MinMax", "MaxAbs", "Standard".
        scaler (object): An existing pre-fitted scaler, note that this is not
            guaranteed to fit table used in this function.

    Returns:
        A pandas DataFrame with projected attributes scaled according to the
        specified scaling method or pre-fitted scaler.
        A scaler Object that is fitted with the given projected table. This scaler
        Object can be passed to other scale_features functions.

    Raises:
        AssertionError: If `table` is not of type pandas
            DataFrame.
        AssertionError: If `table[attr]` is not of type
            numeric.

    Examples:

        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> C = em.read_csv_metadata('path_to_csv_dir/table_C.csv', key='_id')
        >>> feature_table = get_features_for_matching(A, B, validate_inferred_attr_types=False)
        >>> F = extract_feature_vecs(C,
        >>>                          attrs_before=['_id', 'ltable.id', 'rtable.id'],
        >>>                          attrs_after=['gold'],
        >>>                          feature_table=feature_table)
        >>> x, scaler = scale_features(F,
        >>>                            exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
        >>>                            scaling_method='MinMax')
        >>> y, _ = scale_features(F,
        >>>                       exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
        >>>                       scaler=scaler)

    See Also:
     :meth:`py_entitymatching.get_features_for_matching`,
     :meth:`py_entitymatching.extract_feature_vecs`,
     :meth:`py_entitymatching.scale_vectors`

    Note:
        `exclude_attrs` serves as a mask before applying a pre-fitted scaler or
        the specified scaling_method. If there exists any pre-fitted scaler, it will
        be applied to the table regardless of the scaling_method specified. If not,
        scaling_method is applied.

    """
    # Validate the input parameters
    # We expect the input object ltable to be of type pandas DataFrame
    validate_object_type(table, pd.DataFrame, 'Input table')

    # Get projected table
    project_attrs = get_attrs_to_project(table=table, exclude_attrs=exclude_attrs)
    # Check any non-numeric columns in the table
    for attr in project_attrs:
        if not np.issubdtype(table[attr].dtype, np.number):
            raise AssertionError('Projected columns must be numeric vectors')

    # Scale vectors contained in the projected table and return both features and scaler
    table[project_attrs], scaler = scale_vectors(table[project_attrs],
                                                 scaling_method, scaler)

    return table, scaler


def scale_vectors(x: object, scaling_method: object = None, scaler: object = None) -> object:
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
