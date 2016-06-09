import logging
from collections import OrderedDict

import pandas as pd
import six

import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import check_attrs_present

logger = logging.getLogger(__name__)


def eval_matches(X, gold_label_attr, predicted_label_attr):
    if not isinstance(X, pd.DataFrame):
        logger.error('The input table is not of type dataframe')
        raise AssertionError('The input is not of type dataframe')

    if not isinstance(gold_label_attr, six.string_types):
        logger.error('The input gold_label_attr is not of type string')
        raise AssertionError('The input gold_label_attr is not of type string')

    if not isinstance(predicted_label_attr, six.string_types):
        logger.error('The input predicted_label_attr is not of type string')
        raise AssertionError('The input predicted_label_attr is not of type string')

    if not check_attrs_present(X, gold_label_attr):
        logger.error('The gold_label_attr is not present in the input table')
        raise AssertionError('The gold_label_attr is not present in the input table')

    if not check_attrs_present(X, predicted_label_attr):
        logger.error('The predicted_label_attr is not present in the input table')
        raise AssertionError('The predicted_label_attr is not present in the input table')

    Y = X.reset_index(drop=False, inplace=False)
    g = Y[gold_label_attr]
    # if isinstance(g, pd.DataFrame):
    #     g = g.T
    #     assert len(g) == 1, 'Error: Column is picked as dataframe and the num rows > 1'
    #     g = g.iloc[0]

    p = Y[predicted_label_attr]
    # if isinstance(p, pd.DataFrame):
    #     p = p.T
    #     assert len(p) == 1, 'Error: Column is picked as dataframe and the num rows > 1'
    #     p = p.iloc[0]

    # get false label (0) indices
    gf = g[g == 0].index.values

    pf = p[p == 0].index.values

    # get true label (1) indices
    gt = g[g == 1].index.values

    pt = p[p == 1].index.values

    # get false positive indices
    fp_indices = list(set(gf).intersection(pt))

    # get true positive indices
    tp_indices = list(set(gt).intersection(pt))

    # get false negative indices
    fn_indices = list(set(gt).intersection(pf))

    # get true negative indices
    tn_indices = list(set(gf).intersection(pf))

    n_tp = float(len(tp_indices))
    n_fp = float(len(fp_indices))
    n_fn = float(len(fn_indices))
    n_tn = float(len(tn_indices))
    prec_num = n_tp
    prec_den = n_tp + n_fp
    rec_num = n_tp
    rec_den = n_tp + n_fn
    if prec_den == 0.0:
        precision = 0.0
    else:
        precision = prec_num / prec_den
    if rec_den == 0.0:
        recall = 0.0
    else:
        recall = rec_num / rec_den

    if precision == 0.0 and recall == 0.0:
        f1 = 0.0
    else:
        f1 = (2.0 * precision * recall) / (precision + recall)

    fk_ltable, fk_rtable = cm.get_property(X, 'fk_ltable'), cm.get_property(X, 'fk_rtable')

    Y.set_index([fk_ltable, fk_rtable], drop=False, inplace=True)
    false_pos_ls = list(Y.ix[fp_indices].index.values)
    false_neg_ls = list(Y.ix[fn_indices].index.values)
    ret_dict = OrderedDict()
    ret_dict['prec_numerator'] = prec_num
    ret_dict['prec_denominator'] = prec_den
    ret_dict['precision'] = precision
    ret_dict['recall_numerator'] = rec_num
    ret_dict['recall_denominator'] = rec_den
    ret_dict['recall'] = recall
    ret_dict['f1'] = f1
    ret_dict['pred_pos_num'] = n_tp + n_fp
    ret_dict['false_pos_num'] = n_fp
    ret_dict['false_pos_ls'] = false_pos_ls
    ret_dict['pred_neg_num'] = n_fn + n_tn
    ret_dict['false_neg_num'] = n_fn
    ret_dict['false_neg_ls'] = false_neg_ls
    return ret_dict
