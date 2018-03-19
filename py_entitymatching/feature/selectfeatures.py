"""
This module contains functions for selecting most relevant features.
"""

import six
import logging
# import pandas as pd
from skfeature.function.information_theoretical_based import CIFE, CMIM, DISR, ICAP, MRMR, JMI
from skfeature.function.statistical_based import CFS
from skfeature.function.statistical_based import chi_square, f_score, gini_index, t_score
from py_entitymatching.feature.attributeutils import get_attrs_to_project
# from py_entitymatching.utils.catalog_helper import check_attrs_present
# from py_entitymatching.utils.generic_helper import list_diff, list_drop_duplicates
# from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def select_features(feature_table=None, table=None, target_attr=None, exclude_attrs=None,
                   selecting_method=None, num_features=None):
    # get attributes to project, validate parameters
    project_attrs = get_attrs_to_project(table=table,
                                         target_attr=target_attr,
                                         exclude_attrs=exclude_attrs)
    # validate selecting_method
    if not isinstance(selecting_method, six.string_types):
        raise AssertionError("Received wrong type of selecting_method")

    info_based = _get_info_based_funs()
    stat_based = _get_stat_based_funs()

    x, y = table[project_attrs], table[target_attr]
    if selecting_method in info_based:
        selecter = info_based[selecting_method]
        idx, _, _ = selecter(x.values, y.values, n_selected_features=num_features)
        idx = idx[0:num_features]
    elif selecting_method in stat_based:
        selecter, feature_rank = stat_based[selecting_method]
        score = selecter(x.values, y.values)
        idx = feature_rank(score)
    elif selecting_method == 'CFS':
        selecter = CFS.cfs
        idx = selecter(x.values, y.values)
    else:
        raise AssertionError('Selecting method is not present in lookup table')

    feature_names_selected = x.columns[idx]
    feature_table_selected = feature_table.loc[feature_table['feature_name'].isin(feature_names_selected)]
    feature_table_selected.reset_index(inplace=True, drop=True)

    return feature_table_selected


def _get_info_based_funs():
    """
    This function returns the feature selection functions specified by selecting method.

    """
    # Get all the scaling methods
    selecter_names = ['CIFE',
                      'CMIM',
                      'DISR',
                      'JMI',
                      'ICAP',
                      'MRMR']
    # Get all the scaling functions
    selecter_funs = [CIFE.cife,
                     CMIM.cmim,
                     DISR.disr,
                     JMI.jmi,
                     ICAP.icap,
                     MRMR.mrmr]
    # Return a dictionary with the functions names as the key and the actual
    # functions as values.
    return dict(zip(selecter_names, selecter_funs))


def _get_stat_based_funs():
    """
    This function returns the feature selection functions specified by selecting method.

    """
    # Get all the scaling methods
    selecter_names = ['chi_square',
                      'f_score',
                      'gini_index',
                      't_score']
    # Get all the scaling functions
    selecter_funs = [(chi_square.chi_square, chi_square.feature_ranking),
                     (f_score.f_score, f_score.feature_ranking),
                     (gini_index.gini_index, gini_index.feature_ranking),
                     (t_score.t_score, t_score.feature_ranking)]
    # Return a dictionary with the functions names as the key and the actual
    # functions as values.
    return dict(zip(selecter_names, selecter_funs))