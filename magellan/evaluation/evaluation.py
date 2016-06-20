"""
This module contains evaluation related functions.
"""
import collections
import logging

import pandas as pd
import six

import magellan.catalog.catalog_manager as cm
import magellan.utils.catalog_helper as ch
import magellan.utils.generic_helper as gh

logger = logging.getLogger(__name__)


def eval_matches(data_frame, gold_label_attr, predicted_label_attr):
    """
    Evaluates the matches from the matcher.

    Specifically, given a DataFrame containing golden labels and predicted
    labels, this function would evaluate the matches and return the accuracy
    results such as precision, recall and F1.

    Args:
        data_frame (DataFrame): Input pandas DataFrame containing "gold"
            labels and "predicted" labels.
        gold_label_attr (str): An attribute in the input DataFrame containing
            "gold" labels.
        predicted_label_attr (str): An attribute in the input DataFrame
            containing "predicted" labels.

    Returns:
        A python dictionary containing the accuracy measures such as
        precision, recall, F1.

    Raises:
        AssertionError: If the input object (data_frame) is not of type
            pandas DataFrame.
        AssertionError: If the input attribute (gold_label_attr) is not of
            type string.
        AssertionError: If the input attribute (predicted_label_attr) is not of
            type string.
        AssertionError: If the input attribute (gold_label_attr) is not in
            the input dataFrame.
        AssertionError: If the input attribute (predicted_label_attr) is not in
            the input dataFrame.
    """
    # Validate input parameters

    # # We expect the input object to be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('The input table is not of type DataFrame')
        raise AssertionError('The input is not of type DataFrame')

    # # We expect the input attribute (gold_label_attr) to be of type string
    if not isinstance(gold_label_attr, six.string_types):
        logger.error('The input gold_label_attr is not of type string')
        raise AssertionError('The input gold_label_attr is not of type string')

    # # We expect the input attribute (predicted_label_attr) to be of type
    # string
    if not isinstance(predicted_label_attr, six.string_types):
        logger.error('The input predicted_label_attr is not of type string')
        raise AssertionError(
            'The input predicted_label_attr is not of type string')

    # Check whether the gold label attribute is present in the input table
    if not ch.check_attrs_present(data_frame, gold_label_attr):
        logger.error(
            'The gold_label_attr is not present in the input DataFrame')
        raise AssertionError(
            'The gold_label_attr is not present in the input DataFrame')

    # Check whether the predicted label attribute is present in the input table
    if not ch.check_attrs_present(data_frame, predicted_label_attr):
        logger.error(
            'The predicted_label_attr is not present in the input DataFrame')
        raise AssertionError(
            'The predicted_label_attr is not present in the input DataFrame')

    # Reset the index to get the indices set as 0..len(table)
    new_data_frame = data_frame.reset_index(drop=False, inplace=False)

    # Project out the gold and label attributes.
    gold = new_data_frame[gold_label_attr]
    predicted = new_data_frame[predicted_label_attr]

    # Get gold negatives, positives
    gold_negative = gold[gold == 0].index.values
    gold_positive = gold[gold == 1].index.values

    # Get predicted negatives, positives
    predicted_negative = predicted[predicted == 0].index.values
    predicted_positive = predicted[predicted == 1].index.values

    # get false positive indices
    false_positive_indices = \
        list(set(gold_negative).intersection(predicted_positive))

    # get true positive indices
    true_positive_indices = \
        list(set(gold_positive).intersection(predicted_positive))

    # get false negative indices
    false_negative_indices = \
        list(set(gold_positive).intersection(predicted_negative))

    # get true negative indices
    true_negative_indices = \
        list(set(gold_negative).intersection(predicted_negative))

    # Get the number of TP, FP, FN, TN
    num_true_positives = float(len(true_positive_indices))
    num_false_positives = float(len(false_positive_indices))
    num_false_negatives = float(len(false_negative_indices))
    num_true_negatives = float(len(true_negative_indices))

    # Precision = num_tp/ (num_tp + num_fp)

    # Get precision numerator, denominator
    precision_numerator = num_true_positives
    precision_denominiator = num_true_positives + num_false_positives

    # Precision = num_tp/ (num_tp + num_fn)
    # Get recall numerator, denominator
    recall_numerator = num_true_positives
    recall_denominator = num_true_positives + num_false_negatives

    # Compute precision
    if precision_denominiator == 0.0:
        precision = 0.0
    else:
        precision = precision_numerator / precision_denominiator

    # Compute recall
    if recall_denominator == 0.0:
        recall = 0.0
    else:
        recall = recall_numerator / recall_denominator

    # Compute F1
    if precision == 0.0 and recall == 0.0:
        F1 = 0.0
    else:
        F1 = (2.0 * precision * recall) / (precision + recall)

    # Get the fk_ltable and fk_rtable
    fk_ltable = cm.get_property(data_frame, 'fk_ltable')
    fk_rtable = cm.get_property(data_frame, 'fk_rtable')

    # Check if the fk_ltable contain any missing values
    if ch.does_contain_missing_vals(data_frame, fk_ltable):
        logger.error('The fk_ltable (%s) contains missing values' %fk_ltable)
        raise AssertionError('The fk_ltable (%s) contains missing values' %
                             fk_ltable)

    # Check if the fk_rtable contain any missing values
    if ch.does_contain_missing_vals(data_frame, fk_rtable):
        logger.error('The fk_rtable (%s) contains missing values' %fk_rtable)
        raise AssertionError('The fk_rtable (%s) contains missing values' %
                             fk_rtable)


    # Set the index values to fk_ltable and fk_rtable
    new_data_frame.set_index([fk_ltable, fk_rtable], drop=False, inplace=True)

    # Get the list of false positives and false negatives.
    false_pos_ls = list(new_data_frame.ix[false_positive_indices].index.values)
    false_neg_ls = list(new_data_frame.ix[false_negative_indices].index.values)

    # Store and return the accuracy results.
    accuracy_results = collections.OrderedDict()
    accuracy_results['prec_numerator'] = precision_numerator
    accuracy_results['prec_denominator'] = precision_denominiator
    accuracy_results['precision'] = precision
    accuracy_results['recall_numerator'] = recall_numerator
    accuracy_results['recall_denominator'] = recall_denominator
    accuracy_results['recall'] = recall
    accuracy_results['f1'] = F1
    accuracy_results['pred_pos_num'] = num_true_positives + num_false_positives
    accuracy_results['false_pos_num'] = num_false_positives
    accuracy_results['false_pos_ls'] = false_pos_ls
    accuracy_results['pred_neg_num'] = num_false_negatives + num_true_negatives
    accuracy_results['false_neg_num'] = num_false_negatives
    accuracy_results['false_neg_ls'] = false_neg_ls
    return accuracy_results
