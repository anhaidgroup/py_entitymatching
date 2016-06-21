import logging

import six
from PyQt4 import QtGui

import magellan as mg
from magellan import DTMatcher
from magellan.debugmatcher.debug_gui_utils import *
from magellan.gui.debug_gui_base import MainWindowManager
from magellan.utils.catalog_helper import check_attrs_present
from magellan.utils.generic_helper import list_drop_duplicates

logger = logging.getLogger(__name__)


def vis_debug_dt(matcher, train, test, exclude_attrs, target_attr):
    """
    Visual debugger for decision tree matcher

    Parameters
    ----------
    matcher : object, DTMatcher object
    train : MTable, containing training data with "True" labels
    test : MTable, containing test data with "True labels.
            The "True" labels are used for evaluation.
    exclude_attrs : List, attributes to be excluded from train and test,
        for training and testing.

    target_attr : String, column name in validation_set containing 'True' labels

    """
    _vis_debug_dt(matcher, train, test, exclude_attrs, target_attr)


def _vis_debug_dt(matcher, train, test, exclude_attrs, target_attr, show_window=True):
    if not isinstance(matcher, DTMatcher):
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

    # predict using the test data
    predicted = matcher.predict(table=test, exclude_attrs=exclude_attrs,
                                target_attr=predict_attr_name, append=True,
                                inplace=False)

    eval_summary = mg.eval_matches(predicted, target_attr, predict_attr_name)

    metric = get_metric(eval_summary)
    fp_dataframe = get_dataframe(predicted, eval_summary['false_pos_ls'])

    fn_dataframe = get_dataframe(predicted, eval_summary['false_neg_ls'])

    mg._viewapp = QtGui.QApplication.instance()
    if mg._viewapp is None:
        mg._viewapp = QtGui.QApplication([])
    app = mg._viewapp


    m = MainWindowManager(matcher, "dt", exclude_attrs, metric, predicted, fp_dataframe,
                          fn_dataframe)
    if show_window == True:
        m.show()
        app.exec_()


def vis_tuple_debug_dt_matcher(matcher, t, exclude_attrs):
    if isinstance(matcher, DTMatcher):
        clf = matcher.clf
    else:
        clf = matcher
    if isinstance(t, pd.Series):
        fv_columns = t.index
    else:
        fv_columns = t.columns
    if exclude_attrs is None:
        feature_names = fv_columns
    else:
        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]

    code = get_code_vis(clf, feature_names, ['False', 'True'])
    code = get_dbg_fn_vis(code)
    feat_vals = OrderedDict(t.ix[t.index.values[0], feature_names])
    # print feat_vals
    d = {}
    d.update(feat_vals)
    six.exec_(code, d)
    # print code
    ret_val, node_list = d['debug_fn']()

    return ret_val, node_list
