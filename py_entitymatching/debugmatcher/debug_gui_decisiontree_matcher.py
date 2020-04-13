from collections import OrderedDict
import logging

import six

import pandas as pd

import py_entitymatching as em
from py_entitymatching.evaluation.evaluation import eval_matches
from py_entitymatching import DTMatcher
from py_entitymatching.debugmatcher.debug_gui_utils import _get_code_vis, _get_metric,\
    _get_dataframe, _get_dbg_fn_vis, get_name_for_predict_column

import py_entitymatching.utils.catalog_helper as ch
import py_entitymatching.utils.generic_helper as gh
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def vis_debug_dt(matcher, train, test, exclude_attrs, target_attr):
    """
    Visual debugger for Decision Tree matcher.

    Args:
        matcher (DTMatcher): The Decision tree matcher that should be debugged.
        train (DataFrame): The pandas DataFrame that will be used to train the
            matcher.
        test (DataFrame): The pandas DataFrame that will be used to test the
            matcher.
        exclude_attrs (list): The list of attributes to be excluded from train
            and test, for training and testing.
        target_attr (string): The attribute name in the 'train' containing the
            true labels.

    Examples:
        >>> import py_entitymatching as em
        >>> dt = em.DTMatcher()
        # 'devel' is the labeled set used for development (e.g., selecting the best matcher) purposes
        >>> train_test = em.split_train_test(devel, 0.5)
        >>> train, test = train_test['train'], train_test['test']
        >>> em.vis_debug_dt(dt, train, test, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels')

    """
    # Call a wrapper for visualization
    _vis_debug_dt(matcher, train, test, exclude_attrs, target_attr)


def _vis_debug_dt(matcher, train, test, exclude_attrs, target_attr,
                  show_window=True):
    """
    Wrapper function for debugging the Random Forest matcher visually.
    """

    try:
        from PyQt5 import QtWidgets
        from py_entitymatching.gui.debug_gui_base import MainWindowManager
    except ImportError:
        raise ImportError('PyQt5 is not installed. Please install PyQt5 to use '
                          'GUI related functions in py_entitymatching.')

    # Validate the input parameters
    # # We expect the matcher to be of type DTMatcher
    if not isinstance(matcher, DTMatcher):
        logger.error('Input matcher is not of type Decision Tree matcher')
        raise AssertionError('Input matcher is not of type '
                             'Decision Tree matcher')

    # # We expect the target attribute to be of type string.
    validate_object_type(target_attr, six.string_types, error_prefix='Target attribute')

    # # Check whether the exclude attributes are indeed present in the train
    #  DataFrame.
    if not ch.check_attrs_present(train, exclude_attrs):
        logger.error('The exclude attrs are not in train table columns')
        raise AssertionError('The exclude attrs are not in the '
                             'train table columns')

    # # Check whether the target attribute is indeed present in the train
    #  DataFrame.
    if not ch.check_attrs_present(train, target_attr):
        logger.error('The target attr is not in train table columns')
        raise AssertionError('The target attr is not in the '
                             'train table columns')

    # # Check whether the exclude attributes are indeed present in the test
    #  DataFrame.
    if not ch.check_attrs_present(test, exclude_attrs):
        logger.error('The exclude attrs are not in test table columns')
        raise AssertionError('The exclude attrs are not in the '
                             'test table columns')

    # The exclude attributes is expected to be of type list, if not
    # explicitly convert this into a list.
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    # Drop the duplicates from the exclude attributes
    exclude_attrs = gh.list_drop_duplicates(exclude_attrs)

    # If the target attribute is not present in the exclude attributes,
    # then explicitly add it to the exclude attributes.
    if target_attr not in exclude_attrs:
        exclude_attrs.append(target_attr)

    # Now, fit using training data
    matcher.fit(table=train, exclude_attrs=exclude_attrs,
                target_attr=target_attr)

    # Get a column name to store the predictions.
    predict_attr_name = get_name_for_predict_column(test.columns)

    # Predict using the test data
    predicted = matcher.predict(table=test, exclude_attrs=exclude_attrs,
                                target_attr=predict_attr_name, append=True,
                                inplace=False)

    # Get the evaluation summary.
    eval_summary = eval_matches(predicted, target_attr, predict_attr_name)

    # Get metric in a form that can be displayed from the evaluation summary
    metric = _get_metric(eval_summary)

    # Get false negatives and false positives as a DataFrame
    fp_dataframe = _get_dataframe(predicted, eval_summary['false_pos_ls'])
    fn_dataframe = _get_dataframe(predicted, eval_summary['false_neg_ls'])

    em._viewapp = QtWidgets.QApplication.instance()

    if em._viewapp is None:
        em._viewapp = QtWidgets.QApplication([])
    app = em._viewapp

    # Get the main window application
    app = em._viewapp
    m = MainWindowManager(matcher, "dt", exclude_attrs, metric, predicted,
                          fp_dataframe,
                          fn_dataframe)
    # If the show window is true, then display the window.
    if show_window:
        m.show()
        app.exec_()


def vis_tuple_debug_dt_matcher(matcher, t, exclude_attrs):
    """
    Visualize a matcher debugging a tuple.
    """
    # Get the classifier from the input object
    if isinstance(matcher, DTMatcher):
        clf = matcher.clf
    else:
        clf = matcher
    # Get the feature vector columns, based on the input
    if isinstance(t, pd.Series):
        fv_columns = t.index
    else:
        fv_columns = t.columns

    # Get the feature names based on the exclude attributes.
    if exclude_attrs is None:
        feature_names = fv_columns
    else:
        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]

    # Get the code for visualization and wrap that function
    code = _get_code_vis(clf, feature_names, ['False', 'True'])
    code = _get_dbg_fn_vis(code)

    # Link the code with feature vaues
    feat_vals = OrderedDict(t.loc[t.index.values[0], feature_names])
    wrapper_code = {}
    wrapper_code.update(feat_vals)
    six.exec_(code, wrapper_code)
    # Finally, execute the code and relay the return result.
    ret_val, node_list = wrapper_code['debug_fn']()
    return ret_val, node_list
