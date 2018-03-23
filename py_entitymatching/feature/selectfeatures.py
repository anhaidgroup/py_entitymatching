"""
This module contains functions for selecting most relevant features.
"""

import six
from py_entitymatching.feature.attributeutils import get_attrs_to_project
from sklearn.feature_selection import chi2, f_classif, mutual_info_classif
from sklearn.feature_selection import GenericUnivariateSelect


def select_features_univariate(feature_table=None, table=None,
                              target_attr=None, exclude_attrs=None,
                              score='f_score', mode='k_best', parameter=2):
    # get attributes to project, validate parameters
    project_attrs = get_attrs_to_project(table=table,
                                         target_attr=target_attr,
                                         exclude_attrs=exclude_attrs)
    # validate parameters
    if not isinstance(score, six.string_types):
        raise AssertionError("Received wrong type of score function")
    if not isinstance(mode, six.string_types):
        raise AssertionError("Received wrong type of mode function")
    if not (isinstance(parameter, int) or isinstance(parameter, float)):
        raise AssertionError("Received wrong type of parameter")

    # get score function
    score_dict = _get_score_funs()
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
