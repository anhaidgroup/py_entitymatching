import logging


import six

import py_entitymatching as em
from py_entitymatching import RFMatcher
from py_entitymatching.debugmatcher.debug_gui_decisiontree_matcher \
    import vis_tuple_debug_dt_matcher
from py_entitymatching.debugmatcher.debug_gui_utils import _get_metric, \
    get_name_for_predict_column, _get_dataframe
from py_entitymatching.utils.catalog_helper import check_attrs_present
from py_entitymatching.utils.generic_helper import list_drop_duplicates
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def vis_debug_rf(matcher, train, test, exclude_attrs, target_attr):
    """
    Visual debugger for Random Forest matcher.

    Args:
        matcher (RFMatcher): The Random Forest matcher that should be debugged.
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
        >>> rf = em.RFMatcher()
        # 'devel' is the labeled set used for development (e.g., selecting the best matcher) purposes
        >>> train_test = em.split_train_test(devel, 0.5)
        >>> train, test = train_test['train'], train_test['test']
        >>> em.vis_debug_rf(rf, train, test, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels')


    """
    # Call the wrapper function.
    _vis_debug_rf(matcher, train, test, exclude_attrs, target_attr)


def _vis_debug_rf(matcher, train, test, exclude_attrs, target_attr,
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
    # # We expect the matcher to be of type RfMatcher
    if not isinstance(matcher, RFMatcher):
        logger.error('Input matcher is not of type '
                     'Random Forest matcher')
        raise AssertionError('Input matcher is not of type '
                             'Random Forest matcher')

    # # We expect the target attribute to be of type string.
    validate_object_type(target_attr, six.string_types, error_prefix='Target attribute')

    # # Check whether the exclude attributes are indeed present in the train
    #  DataFrame.
    if not check_attrs_present(train, exclude_attrs):
        logger.error('The exclude attrs are not in train table columns')
        raise AssertionError('The exclude attrs are not in the train table columns')

    # # Check whether the target attribute is indeed present in the train
    #  DataFrame.
    if not check_attrs_present(train, target_attr):
        logger.error('The target attr is not in train table columns')
        raise AssertionError('The target attr is not in the train table columns')

    # # Check whether the exclude attributes are indeed present in the test
    #  DataFrame.
    if not check_attrs_present(test, exclude_attrs):
        logger.error('The exclude attrs are not in test table columns')
        raise AssertionError('The exclude attrs are not in the test table columns')


    # The exclude attributes is expected to be of type list, if not
    # explicitly convert this into a list.
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    # Drop the duplicates from the exclude attributes
    exclude_attrs = list_drop_duplicates(exclude_attrs)

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
    eval_summary = em.eval_matches(predicted, target_attr, predict_attr_name)
    em._viewapp = QtWidgets.QApplication.instance()
    if em._viewapp is None:
        em._viewapp = QtWidgets.QApplication([])

    # Get metric in a form that can be displayed from the evaluation summary
    metric = _get_metric(eval_summary)

    # Get false negatives and false positives as a DataFrame
    fp_dataframe = _get_dataframe(predicted, eval_summary['false_pos_ls'])
    fn_dataframe = _get_dataframe(predicted, eval_summary['false_neg_ls'])

    # Get the main window application

    app = em._viewapp

    m = MainWindowManager(matcher, "rf", exclude_attrs, metric, predicted, fp_dataframe,
                          fn_dataframe)

    # If the show window is true, then display the window.
    if show_window:
        m.show()
        app.exec_()


def vis_tuple_debug_rf_matcher(matcher, t, exclude_attrs):
    """
    Visualize a matcher debugging a tuple.
    """
    # Get the classifier from the input object
    if isinstance(matcher, RFMatcher):
        clf = matcher.clf
    else:
        clf = matcher

    # Initialize the consolidate node lists and the statuses.
    consol_node_list = []
    consol_status = []

    # For each estimator (i.e the decision tree) call the decision tree
    # counter part
    for estimator in clf.estimators_:
        # Get the result from debugging the estimator
        ret_val, node_list = vis_tuple_debug_dt_matcher(estimator, t,
                                                        exclude_attrs)

        # Update the lists
        consol_status.append(ret_val)
        consol_node_list.append([ret_val, node_list])

    ret_val = False
    # Compute the prob. for match and non-match
    prob_true = float(sum(consol_status)) / len(clf.estimators_)
    prob_false = 1 - prob_true
    if prob_true > prob_false:
        ret_val = True
    # Finally return the status, and the consolidated node list.
    return ret_val, consol_node_list
