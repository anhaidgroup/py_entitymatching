"""
This module contains functions for selecting most relevant features.
"""

import six
import pandas as pd
from py_entitymatching.utils.validation_helper import validate_object_type
from py_entitymatching.feature.attributeutils import get_attrs_to_project
from sklearn.feature_selection import chi2, f_classif, mutual_info_classif
from sklearn.feature_selection import GenericUnivariateSelect
from sklearn.feature_selection import mutual_info_classif as mi_d
from sklearn.feature_selection import mutual_info_regression as mi_c


def select_features_mrmr(feature_table, table,
                         target_attr=None, exclude_attrs=None,
                         number=2):

    # get attributes to project, validate parameters
    project_attrs = get_attrs_to_project(table=table,
                                         target_attr=target_attr,
                                         exclude_attrs=exclude_attrs)

    # project feature vectors into features:x and target:y
    x, y = table[project_attrs], table[target_attr]

    feature_names_selected = []

    #
    feature_names = project_attrs
    mutual_info = list(mi_d(x, y))
    scored_features = list(zip(mutual_info, feature_names))

    max_rel = max(scored_features, key=lambda x: x[0])
    feature_names_selected.append(max_rel[1])

    # iteratively select features based on
    # minimum redundancy maximum relevance (mRMR) criteria
    for _ in range(number - 1):
        feature_names = [fn for fn in feature_names
                         if fn not in feature_names_selected]
        mutual_info = list(mi_d(x[feature_names], y))
        scored_features = list(zip(mutual_info, feature_names))

        mrmr_scored_features = []
        for mi, fn in scored_features:
            xx = x[feature_names_selected]
            yy = x[fn]
            dep = mi_c(xx, yy)
            mi -= sum(dep) / len(dep)
            mrmr_scored_features.append((mi, fn))

        mrmr_mi, fn = max(mrmr_scored_features, key=lambda x: x[0])
        feature_names_selected.append(fn)

    # get selected features in feature_table
    # feature_table_selected = \
    #     feature_table.loc[feature_table['feature_name'].isin(feature_names_selected)]
    feature_table_selected = pd.DataFrame(columns=feature_table.columns)
    for fn in feature_names_selected:
        ft = feature_table.loc[feature_table['feature_name'] == fn]
        feature_table_selected = pd.concat([feature_table_selected, ft])

    feature_table_selected.reset_index(inplace=True, drop=True)

    return feature_table_selected


def select_features_univariate(feature_table, table,
                               target_attr=None, exclude_attrs=None,
                               score='f_score', mode='k_best', parameter=2):
    """
    This function will select a certain number of most relevant features according
    to the criteria specified by users.

    Specifically, this function will project the table into feature vectors (by
     excluding exclude_attrs and target_attr) and target values. It will then call
     GenericUnivariateSelect provided by scikit-learn with specified scoring function,
     selection mode, and one specified parameter. The selected features is singled out
     in the given feature_table and returned.

    Args:
        feature_table (DataFrame): The pandas DataFrame which contains the metadata
            of all the features.
        table (DataFrame): The pandas DataFrames which contains all the features,
            keys, and target.
        target_attr (str): One attribute to be used as labels in the selection
            process, often with names `label` or `gold`.
        exclude_attrs (list): A list of attributes to be excluded from the table,
            these attributes will not be considered for feature selection.
        score (str): The scoring method specified by user, can be one of
            "chi_square", "f_score", "mutual_info". Defaults to be "f_score".
        mode (str): The selection mode specified by user, can be one of
            "percentile", "k_best", "fpr", "fdr", "fwe". Defaults to be "k_best".
        parameter (int or float): The parameter specified by the user according to
            the chosen mode.  Defaults to be 2.

    Returns:
        A pandas DataFrame of features selected as most relevant features, including
        all the metadata generated with the data.

    Raises:
        AssertionError: If `feature_table` is not of type pandas
            DataFrame.
        AssertionError: If `table` is not of type pandas
            DataFrame.
        AssertionError: If `score` is not of type
            str.
        AssertionError: If `mode` is not of type
            str.
        AssertionError: If `parameter` is not of type
            int or float.
        AssertionError: If `score` is not in score_dict
        AssertionError: If `mode` is not in mode_list

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
        >>> x, scaler = scale_features(H,
        >>>                            exclude_attrs=['_id', 'ltable.id', 'rtable.id'],
        >>>                            scaling_method='MinMax')
        >>> feature_table_selected = select_features_univariate(
        >>>     feature_table=feature_table, table=x,
        >>>     target_attr='gold', exclude_attrs=['_id', 'ltable.id', 'rtable.id'],
        >>>     score='f_score', mode='k_best', parameter=2)

    See Also:
     :meth:`py_entitymatching.get_features_for_matching`,
     :meth:`py_entitymatching.extract_feature_vecs`,
     :meth:`py_entitymatching.scale_features`


    Note:
        The function applies only univariate feature selection methods. And returns
        only the metadata of selected features. To proceed, users need to used the
        selected features to extract feature vectors.

    """
    # Validate the input parameters
    # We expect the input object feature_table and table to be of type pandas DataFrame
    validate_object_type(feature_table, pd.DataFrame, 'Input feature_table')
    validate_object_type(table, pd.DataFrame, 'Input table')

    # validate parameters
    if not isinstance(score, six.string_types):
        raise AssertionError("Received wrong type of score function")
    if not isinstance(mode, six.string_types):
        raise AssertionError("Received wrong type of mode function")
    if not (isinstance(parameter, int) or isinstance(parameter, float)):
        raise AssertionError("Received wrong type of parameter")

    # get score function
    score_dict = _get_score_funs()
    # get mode names allowed
    mode_list = _get_mode_names()
    if score not in score_dict:
        raise AssertionError("Unknown score functions specified")
    if mode not in mode_list:
        raise AssertionError("Unknown mode specified")

    if target_attr not in list(table.columns):
        raise AssertionError("Must specify the target attribute for feature selection")

    # get attributes to project, validate parameters
    project_attrs = get_attrs_to_project(table=table,
                                         target_attr=target_attr,
                                         exclude_attrs=exclude_attrs)

    score_fun = score_dict[score]
    # initialize selector with the given specification
    selector = GenericUnivariateSelect(score_func=score_fun,
                                       mode=mode,
                                       param=parameter)

    # project feature vectors into features:x and target:y
    x, y = table[project_attrs], table[target_attr]

    # fit and select most relevant features
    selector.fit(x, y)
    idx = selector.get_support(indices=True)

    # get selected features in feature_table
    feature_names_selected = x.columns[idx]
    feature_table_selected = feature_table.loc[feature_table['feature_name'].isin(feature_names_selected)]
    feature_table_selected.reset_index(inplace=True, drop=True)

    return feature_table_selected


def _get_score_funs():
    """
    This function returns the score functions specified by score.

    """
    # Get all the score names
    score_names = ['chi_square',
                   'f_score',
                   'mutual_info']
    # Get all the score functions
    score_funs = [chi2,
                  f_classif,
                  mutual_info_classif]
    # Return a dictionary with the scores names as the key and the actual
    # score functions as values.
    return dict(zip(score_names, score_funs))


def _get_mode_names():
    # Get names of all modes allowed
    return ['percentile', 'k_best', 'fpr', 'fdr', 'fwe']