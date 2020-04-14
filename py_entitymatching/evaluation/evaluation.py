"""
This module contains evaluation related functions.
"""
import collections
import logging

import pandas as pd
import six

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.debugmatcher.debug_gui_utils import _get_dataframe, _get_metric
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def eval_matches(data_frame, gold_label_attr, predicted_label_attr):
    """
    Evaluates the matches from the matcher.

    Specifically, given a DataFrame containing golden labels and predicted
    labels, this function would evaluate the matches and return the accuracy
    results such as precision, recall and F1.

    Args:
        data_frame (DataFrame): The input pandas DataFrame containing "gold"
            labels and "predicted" labels.
        gold_label_attr (string): An attribute in the input DataFrame containing
            "gold" labels.
        predicted_label_attr (string): An attribute in the input DataFrame
            containing "predicted" labels.

    Returns:
        A Python dictionary containing the accuracy measures such as
        precision, recall, F1.

    Raises:
        AssertionError: If `data_frame` is not of type
            pandas DataFrame.
        AssertionError: If `gold_label_attr` is not of
            type string.
        AssertionError: If `predicted_label_attr` is not of
            type string.
        AssertionError: If the `gold_label_attr` is not in
            the input dataFrame.
        AssertionError: If the `predicted_label_attr` is not in
            the input dataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # G is the labeled data used for development purposes, match_f is the feature table
        >>> H = em.extract_feat_vecs(G, feat_table=match_f, attrs_after='gold_labels')
        >>> dt = em.DTMatcher()
        >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> pred_table = dt.predict(table=H,  exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'],  append=True, target_attr='predicted_labels')
        >>> eval_summary = em.eval_matches(pred_table, 'gold_labels', 'predicted_labels')
    """
    # Validate input parameters

    # # We expect the input object to be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame, 'The input table')

    # # We expect the input attribute (gold_label_attr) to be of type string
    validate_object_type(gold_label_attr, six.string_types, 'The input gold_label_attr')

    # # We expect the input attribute (predicted_label_attr) to be of type
    # string
    validate_object_type(predicted_label_attr, six.string_types, 'The input predicted_label_attr')

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
    false_pos_ls = list(new_data_frame.iloc[false_positive_indices].index.values)
    false_neg_ls = list(new_data_frame.iloc[false_negative_indices].index.values)

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


def get_false_positives_as_df(table, eval_summary, verbose=False):
    """
    Select only the false positives from the input table and return as a
    DataFrame based on the evaluation results.

    Args:
        table (DataFrame): The input table (pandas DataFrame) that was used for
            evaluation.
        eval_summary (dictionary): A Python dictionary containing evaluation
            results, typically from 'eval_matches' command.

    Returns:
        A pandas DataFrame containing only the False positives from
        the input table.

        Further,
        this function sets the output DataFrame's properties same as input
        DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # G is the labeled data used for development purposes, match_f is the feature table
        >>> H = em.extract_feat_vecs(G, feat_table=match_f, attrs_after='gold_labels')
        >>> dt = em.DTMatcher()
        >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> pred_table = dt.predict(table=H,  exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'],  append=True, target_attr='predicted_labels')
        >>> eval_summary = em.eval_matches(pred_table, 'gold_labels', 'predicted_labels')
        >>> false_pos_df = em.get_false_positives_as_df(H, eval_summary)



    """
    # Validate input parameters

    # # We expect the input candset to be of type pandas DataFrame.
    validate_object_type(table, pd.DataFrame, error_prefix='Input cand.set')

    # Do metadata checking
    # # Mention what metadata is required to the user
    ch.log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                        'fk rtable, '
                        'ltable, rtable, ltable key, rtable key', verbose)

    # # Get metadata
    ch.log_info(logger, 'Getting metadata from catalog', verbose)

    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
        cm.get_metadata_for_candset(
        table, logger, verbose)

    # # Validate metadata
    ch.log_info(logger, 'Validating metadata', verbose)
    cm._validate_metadata_for_candset(table, key, fk_ltable, fk_rtable,
                                      ltable, rtable, l_key, r_key,
                                      logger, verbose)


    data_frame =  _get_dataframe(table, eval_summary['false_pos_ls'])

    # # Update catalog
    ch.log_info(logger, 'Updating catalog', verbose)

    cm.init_properties(data_frame)
    cm.copy_properties(table, data_frame)

    return data_frame


def get_false_negatives_as_df(table, eval_summary, verbose=False):

    """
    Select only the false negatives from the input table and return as a
    DataFrame based on the evaluation results.

    Args:
        table (DataFrame): The input table (pandas DataFrame) that was used for
            evaluation.
        eval_summary (dictionary): A Python dictionary containing evaluation
            results, typically from 'eval_matches' command.

    Returns:
        A pandas DataFrame containing only the false negatives from
        the input table.

        Further,
        this function sets the output DataFrame's properties same as input
        DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> # G is the labeled data used for development purposes, match_f is the feature table
        >>> H = em.extract_feat_vecs(G, feat_table=match_f, attrs_after='gold_labels')
        >>> dt = em.DTMatcher()
        >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> pred_table = dt.predict(table=H,  exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'],  append=True, target_attr='predicted_labels')
        >>> eval_summary = em.eval_matches(pred_table, 'gold_labels', 'predicted_labels')
        >>> false_neg_df = em.get_false_negatives_as_df(H, eval_summary)


    """
    # Validate input parameters

    # # We expect the input candset to be of type pandas DataFrame.
    validate_object_type(table, pd.DataFrame, error_prefix='Input cand.set')

    # Do metadata checking
    # # Mention what metadata is required to the user
    ch.log_info(logger, 'Required metadata: cand.set key, fk ltable, '
                        'fk rtable, '
                        'ltable, rtable, ltable key, rtable key', verbose)

    # # Get metadata
    ch.log_info(logger, 'Getting metadata from the catalog', verbose)

    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
        cm.get_metadata_for_candset(
        table, logger, verbose)

    # # Validate metadata
    ch.log_info(logger, 'Validating metadata', verbose)
    cm._validate_metadata_for_candset(table, key, fk_ltable, fk_rtable,
                                      ltable, rtable, l_key, r_key,
                                      logger, verbose)

    data_frame =  _get_dataframe(table, eval_summary['false_neg_ls'])

    # # Update catalog
    ch.log_info(logger, 'Updating catalog', verbose)

    cm.init_properties(data_frame)
    cm.copy_properties(table, data_frame)

    # # Update catalog
    ch.log_info(logger, 'Returning the dataframe', verbose)

    return data_frame




def print_eval_summary(eval_summary):
    """
    Prints a summary of evaluation results.

    Args:
        eval_summary (dictionary): Dictionary containing evaluation results,
            typically from 'eval_matches' function.

    Examples:
        >>> import py_entitymatching as em
        >>> # G is the labeled data used for development purposes, match_f is the feature table
        >>> H = em.extract_feat_vecs(G, feat_table=match_f, attrs_after='gold_labels')
        >>> dt = em.DTMatcher()
        >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
        >>> pred_table = dt.predict(table=H,  exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'],  append=True, target_attr='predicted_labels')
        >>> eval_summary = em.eval_matches(pred_table, 'gold_labels', 'predicted_labels')
        >>> em.print_eval_summary(eval_summary)

    """
    m = _get_metric(eval_summary)
    for key, value in six.iteritems(m):
        print(key + " : " + value)
