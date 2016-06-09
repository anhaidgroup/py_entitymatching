import logging

import six

import magellan as mg
from magellan import RFMatcher
from magellan.debugmatcher.debug_gui_decisiontree_matcher import vis_tuple_debug_dt_matcher
from magellan.debugmatcher.debug_gui_utils import *
from magellan.gui.debug_gui_base import MainWindowManager
from magellan.utils.catalog_helper import check_attrs_present
from magellan.utils.generic_helper import list_drop_duplicates

logger = logging.getLogger(__name__)


def vis_debug_rf(matcher, train, test, exclude_attrs, target_attr):
    """
    Visual debugger for random forest matcher

    Parameters
    ----------
    matcher : object, RFMatcher object
    train : MTable, containing training data with "True" labels
    test : MTable, containing test data with "True labels.
            The "True" labels are used for evaluation.
    exclude_attrs : List, attributes to be excluded from train and test,
        for training and testing.

    target_attr : String, column name in validation_set containing 'True' labels

    """
    _vis_debug_rf(matcher, train, test, exclude_attrs, target_attr)


def _vis_debug_rf(matcher, train, test, exclude_attrs, target_attr, show_window=True):
    if not isinstance(matcher, RFMatcher):
        logger.error('Input matcher is not of type Decision Tree matcher')
        raise AssertionError('Input matcher is not of type Decision Tree matcher')

    if not isinstance(target_attr, six.string_types):
        logger.error('Target attribute is not of type string')
        raise AssertionError('Target attribute is not of type string')

    if not check_attrs_present(train, exclude_attrs):
        logger.error('The exclude attrs are not in train table columns')
        raise AssertionError('The exclude attrs are not in the train table columns')

    if not check_attrs_present(train, target_attr):
        logger.error('The target attr is not in train table columns')
        raise AssertionError('The target attr is not in the train table columns')

    if not check_attrs_present(test, exclude_attrs):
        logger.error('The exclude attrs are not in test table columns')
        raise AssertionError('The exclude attrs are not in the test table columns')

    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    exclude_attrs = list_drop_duplicates(exclude_attrs)

    if target_attr not in exclude_attrs:
        exclude_attrs.append(target_attr)

    # fit using training data
    matcher.fit(table=train, exclude_attrs=exclude_attrs, target_attr=target_attr)
    predict_attr_name = get_name_for_predict_column(test.columns)
    predicted = matcher.predict(table=test, exclude_attrs=exclude_attrs,
                                target_attr=predict_attr_name, append=True,
                                inplace=False)
    eval_summary = mg.eval_matches(predicted, target_attr, predict_attr_name)
    metric = get_metric(eval_summary)
    fp_dataframe = get_dataframe(predicted, eval_summary['false_pos_ls'])
    fn_dataframe = get_dataframe(predicted, eval_summary['false_neg_ls'])
    app = mg._viewapp
    m = MainWindowManager(matcher, "rf", exclude_attrs, metric, predicted, fp_dataframe,
                          fn_dataframe)
    if show_window == True:
        m.show()
        app.exec_()


def vis_tuple_debug_rf_matcher(matcher, t, exclude_attrs):
    if isinstance(matcher, RFMatcher):
        clf = matcher.clf
    else:
        clf = matcher
    consol_node_list = []
    consol_status = []

    for e in clf.estimators_:
        # print t
        ret_val, node_list = vis_tuple_debug_dt_matcher(e, t, exclude_attrs)
        consol_status.append(ret_val)
        consol_node_list.append([ret_val, node_list])
    ret_val = False
    prob_true = float(sum(consol_status)) / len(clf.estimators_)
    prob_false = 1 - prob_true
    if prob_true > prob_false:
        ret_val = True
    return ret_val, consol_node_list
