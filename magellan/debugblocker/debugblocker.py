from collections import namedtuple
import heapq as hq
import logging
import numpy
from operator import attrgetter
import pandas as pd
#import six
#import sys
#sys.path.append('/Users/lihan/Documents/Magellan/magellan/')

import magellan as mg
import magellan.catalog.catalog_manager as cm
from magellan.utils.catalog_helper import log_info, get_name_for_key, add_key_column
from magellan.utils.generic_helper import rem_nan

logger = logging.getLogger(__name__)


def debug_blocker(ltable, rtable, candidate_set, output_size=200,
                  attr_corres=None, verbose=True):

    # Check input types
    validate_types(ltable, rtable, candidate_set, output_size,
                   attr_corres, verbose)

    # basic checks.
    if len(ltable) == 0:
        raise AssertionError('Error: ltable is empty!')
    if len(rtable) == 0:
        raise AssertionError('Error: rtable is empty!')
    if output_size <= 0:
        raise AssertionError('The input parameter: \'output_size\''
                            ' is less than or equal to 0. Nothing needs'
                            ' to be done!')

    # Get metadata
    l_key, r_key = cm.get_keys_for_ltable_rtable(ltable, rtable, logger, verbose)

    # Validate metadata
    cm.validate_metadata_for_table(ltable, l_key, 'ltable', logger, verbose)
    cm.validate_metadata_for_table(rtable, r_key, 'rtable', logger, verbose)

    # Check the user input field correst list (if exists) and get the raw
    # version of our internal correst list.
    check_input_field_correspondence_list(ltable, rtable, attr_corres)
    corres_list = get_field_correspondence_list(ltable, rtable,
                                                l_key, r_key, attr_corres)

    # Build the (col_name: col_index) dict to speed up locating a field in
    # the schema.
    ltable_col_dict = build_col_name_index_dict(ltable)
    rtable_col_dict = build_col_name_index_dict(rtable)

    # Filter correspondence list to remove numeric types. We only consider
    # string types for document concatenation.
    filter_corres_list(ltable, rtable, l_key, r_key,
                       ltable_col_dict, rtable_col_dict, corres_list)

    # Get field filtered new table.
    ltable_filtered, rtable_filtered = get_filtered_table(
        ltable, rtable, l_key, r_key, corres_list)

    feature_list = select_features(ltable_filtered, rtable_filtered, l_key, r_key)
    #if len(feature_list) == 1:
    #    raise AssertionError('\nError: the selected field list is empty,'
    #                        ' nothing could be done! Please check if all'
    #                        ' table fields are numeric types.')
    # print 'selected_fields:', ltable_filtered.columns[feature_list]

    lrecord_id_to_index_map = get_record_id_to_index_map(ltable_filtered, l_key)
    rrecord_id_to_index_map = get_record_id_to_index_map(rtable_filtered, r_key)
    lrecord_list = get_tokenized_table(ltable_filtered, l_key, feature_list)
    rrecord_list = get_tokenized_table(rtable_filtered, r_key, feature_list)

    new_formatted_candidate_set = index_candidate_set(
         candidate_set, lrecord_id_to_index_map, rrecord_id_to_index_map, verbose)

    order_dict = {}
    build_global_token_order(lrecord_list, order_dict)
    build_global_token_order(rrecord_list, order_dict)

    sort_record_tokens_by_global_order(lrecord_list, order_dict)
    sort_record_tokens_by_global_order(rrecord_list, order_dict)

    topk_heap = topk_sim_join(
        lrecord_list, rrecord_list, new_formatted_candidate_set, output_size)

    ret_dataframe = assemble_topk_table(topk_heap, ltable_filtered, rtable_filtered)
    return ret_dataframe

def validate_types(ltable, rtable, candidate_set, output_size,
                   attr_corres, verbose):
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input left table is not of type pandas data frame')
        raise AssertionError('Input left table is not of type pandas data frame')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input right table is not of type pandas data frame')
        raise AssertionError('Input right table is not of type pandas data frame')

    if not isinstance(candidate_set, pd.DataFrame):
        logger.error('Input candidate set is not of type pandas dataframe')
        raise AssertionError('Input candidate set is not of type pandas dataframe')

    if not isinstance(output_size, int):
        logging.error('Output size is not of type int')
        raise AssertionError('Output size is not of type int')

    if attr_corres is not None:
        if not isinstance(attr_corres, list):
            logging.error('Input attribute correspondence is not of'
                          ' type list')
            raise AssertionError('Input attribute correspondence is'
                                 ' not of type list')

        for pair in attr_corres:
            if not isinstance(pair, tuple):
                logging.error('Pair in attribute correspondence list is not'
                              ' of type tuple')
                raise AssertionError('Pair in attribute correspondence list'
                                     ' is not of type tuple')

    if not isinstance(verbose, bool):
        logger.error('Parameter verbose is not of type bool')
        raise AssertionError('Parameter verbose is not of type bool')


def assemble_topk_table(topk_heap, ltable, rtable, ret_key='_id',
                        l_output_prefix='ltable_', r_output_prefix='rtable_'):
    topk_heap.sort(key=lambda tup: tup[0], reverse = True)
    ret_data_col_name_list = ['_id', 'similarity']
    ltable_col_names = list(ltable.columns)
    rtable_col_names = list(rtable.columns)
    lkey = mg.get_key(ltable)
    rkey = mg.get_key(rtable)
    lkey_index = 0
    rkey_index = 0
    for i in range(len(ltable_col_names)):
        if ltable_col_names[i] == lkey:
            lkey_index = i

    for i in range(len(rtable_col_names)):
        if rtable_col_names[i] == rkey:
            rkey_index = i

    ret_data_col_name_list.append(l_output_prefix + lkey)
    ret_data_col_name_list.append(r_output_prefix + rkey)
    ltable_col_names.remove(lkey)
    rtable_col_names.remove(rkey)

    for i in range(len(ltable_col_names)):
        ret_data_col_name_list.append(l_output_prefix + ltable_col_names[i])
    for i in range(len(rtable_col_names)):
        ret_data_col_name_list.append(r_output_prefix + rtable_col_names[i])

    ret_tuple_list = []
    for i in range(len(topk_heap)):
        tuple = topk_heap[i]
        lrecord = list(ltable.ix[tuple[1]])
        rrecord = list(rtable.ix[tuple[2]])
        ret_tuple = [i, tuple[0]]
        ret_tuple.append(lrecord[lkey_index])
        ret_tuple.append(rrecord[rkey_index])
        for j in range(len(lrecord)):
            if j != lkey_index:
                ret_tuple.append(lrecord[j])
        for j in range(len(rrecord)):
            if j != rkey_index:
                ret_tuple.append(rrecord[j])
        ret_tuple_list.append(ret_tuple)

    data_frame = pd.DataFrame(ret_tuple_list)
    # When the ret data frame is empty, we cannot assign column names.
    if len(data_frame) == 0:
        return data_frame

    data_frame.columns = ret_data_col_name_list
    lkey = mg.get_key(ltable)
    rkey = mg.get_key(rtable)
    cm.set_candset_properties(data_frame, ret_key, l_output_prefix + lkey,
        r_output_prefix + rkey, ltable, rtable)

    return data_frame


def topk_sim_join(lrecord_list, rrecord_list, cand_set, output_size):
    prefix_events = generate_prefix_events(lrecord_list, rrecord_list)
    topk_heap = topk_sim_join_impl(lrecord_list, rrecord_list,
                                   prefix_events, cand_set, output_size)

    return topk_heap


def topk_sim_join_impl(lrecord_list, rrecord_list, prefix_events,
                       cand_set, output_size):
    total_compared_pairs = 0
    compared_set = set()
    l_inverted_index = {}
    r_inverted_index = {}
    topk_heap = []

    while len(prefix_events) > 0:
        if len(topk_heap) == output_size and\
                        topk_heap[0][0] >= prefix_events[0][0] * -1:
            break
        event = hq.heappop(prefix_events)
        table_indicator = event[1]
        rec_idx = event[2]
        tok_idx = event[3]
        if table_indicator == 0:
            token = lrecord_list[rec_idx][tok_idx]
            if token in r_inverted_index:
                r_records = r_inverted_index[token]
                for r_rec_idx in r_records:
                    pair = (rec_idx, r_rec_idx)
                    if pair in cand_set:
                        continue
                    if pair in compared_set:
                        continue
                    sim = jaccard_sim(
                        set(lrecord_list[rec_idx]), set(rrecord_list[r_rec_idx]))
                    if len(topk_heap) == output_size:
                        hq.heappushpop(topk_heap, (sim, rec_idx, r_rec_idx))
                    else:
                        hq.heappush(topk_heap, (sim, rec_idx, r_rec_idx))

                    total_compared_pairs += 1
                    # if total_compared_pairs % 100000 == 0:
                    #     print total_compared_pairs, topk_heap[0], prefix_events[0]
                    compared_set.add(pair)

            if token not in l_inverted_index:
                l_inverted_index[token] = set()
            l_inverted_index[token].add(rec_idx)
        else:
            token = rrecord_list[rec_idx][tok_idx]
            if token in l_inverted_index:
                l_records = l_inverted_index[token]
                for l_rec_idx in l_records:
                    pair = (l_rec_idx, rec_idx)
                    if pair in cand_set:
                        continue
                    if pair in compared_set:
                        continue
                    sim = jaccard_sim(
                        set(lrecord_list[l_rec_idx]), set(rrecord_list[rec_idx]))
                    if len(topk_heap) == output_size:
                        hq.heappushpop(topk_heap, (sim, l_rec_idx, rec_idx))
                    else:
                        hq.heappush(topk_heap, (sim, l_rec_idx, rec_idx))

                    total_compared_pairs += 1
                    # if total_compared_pairs % 100000 == 0:
                    #     print total_compared_pairs, topk_heap[0], prefix_events[0]
                    compared_set.add(pair)
            if token not in r_inverted_index:
                r_inverted_index[token] = set()
            r_inverted_index[token].add(rec_idx)

    return topk_heap


def jaccard_sim(l_token_set, r_token_set):
    l_len = len(l_token_set)
    r_len = len(r_token_set)
    intersect_size = len(l_token_set & r_token_set)
    if l_len + r_len == 0:
        return 0.0

    return intersect_size * 1.0 / (l_len + r_len - intersect_size)


def check_input_field_correspondence_list(ltable, rtable, field_corres_list):
    if field_corres_list is None:
        return
    true_ltable_fields = list(ltable.columns)
    true_rtable_fields = list(rtable.columns)
    for pair in field_corres_list:
        if type(pair) != tuple or len(pair) != 2:
            raise AssertionError('Error in checking user input field'
                                ' correspondence: the input field pairs'
                                 'are not in the required tuple format!')

    given_ltable_fields = [field[0] for field in field_corres_list]
    given_rtable_fields = [field[1] for field in field_corres_list]
    for given_field in given_ltable_fields:
        if given_field not in true_ltable_fields:
            raise AssertionError('Error in checking user input field'
                                ' correspondence: the field \'%s\' is'
                                ' not in the ltable!' %(given_field))
    for given_field in given_rtable_fields:
        if given_field not in true_rtable_fields:
            raise AssertionError('Error in checking user input field'
                                ' correspondence:'
                                ' the field \'%s\' is not in the'
                                ' rtable!' %(given_field))
    return


def get_field_correspondence_list(ltable, rtable, lkey, rkey, attr_corres):
    corres_list = []
    if attr_corres is None or len(attr_corres) == 0:
        corres_list = mg.get_attr_corres(ltable, rtable)['corres']
        if len(corres_list) == 0:
            raise AssertionError('Error: the field correspondence list'
                                 ' is empty. Please specify the field'
                                 ' correspondence!')
    else:
        for tu in attr_corres:
            corres_list.append(tu)

    key_pair = (lkey, rkey)
    if key_pair not in corres_list:
        corres_list.append(key_pair)

    return corres_list


def filter_corres_list(ltable, rtable, ltable_key, rtable_key,
                       ltable_col_dict, rtable_col_dict, corres_list):
    ltable_dtypes = list(ltable.dtypes)
    rtable_dtypes = list(rtable.dtypes)
    for i in reversed(range(len(corres_list))):
        lcol_name = corres_list[i][0]
        rcol_name = corres_list[i][1]
        # Filter the pair where both fields are numeric types.
        if ltable_dtypes[ltable_col_dict[lcol_name]] != numpy.dtype('O')\
                and rtable_dtypes[rtable_col_dict[rcol_name]] != numpy.dtype('O'):
            if lcol_name != ltable_key and rcol_name != rtable_key:
                corres_list.pop(i)

    if len(corres_list) == 1 and corres_list[0][0] == ltable_key\
                             and corres_list[0][1] == rtable_key:
        raise AssertionError('The field correspondence list is empty after'
                             ' filtering: please verify your correspondence'
                             ' list, or check if each field is of numeric'
                             ' type!')

def get_filtered_table(ltable, rtable, lkey, rkey, corres_list):
    ltable_cols = [col_pair[0] for col_pair in corres_list]
    rtable_cols = [col_pair[1] for col_pair in corres_list]
    lfiltered_table = ltable[ltable_cols]
    rfiltered_table = rtable[rtable_cols]
    #if lkey not in lfiltered_table.columns:
    #    raise AssertionError('lkey not in the filtered table:', lkey)
    #if rkey not in rfiltered_table.columns:
    #    raise AssertionError('rkey not in the filtered table:', rkey)
    mg.set_key(lfiltered_table, lkey)
    mg.set_key(rfiltered_table, rkey)
    return lfiltered_table, rfiltered_table


def build_col_name_index_dict(table):
    col_dict = {}
    col_names = list(table.columns)
    for i in range(len(col_names)):
        col_dict[col_names[i]] = i
    return col_dict


def select_features(ltable, rtable, lkey, rkey):
    lcolumns = list(ltable.columns)
    rcolumns = list(rtable.columns)
    lkey_index = -1
    rkey_index = -1
    if len(lcolumns) != len(rcolumns):
        raise AssertionError('Error: FILTERED ltable and FILTERED rtable'
                            ' have different number of fields!')
    for i in range(len(lcolumns)):
        if lkey == lcolumns[i]:
            lkey_index = i
    #if lkey_index < 0:
    #    raise AssertionError('Error: cannot find key in the FILTERED'
    #                        ' ltable schema!')
    for i in range(len(rcolumns)):
        if rkey == rcolumns[i]:
            rkey_index = i
    #if rkey_index < 0:
    #    raise AssertionError('Error: cannot find key in the FILTERED'
    #                        ' rtable schema!')

    lweight = get_feature_weight(ltable)
    #logging.info('\nFinish calculate ltable feature weights.')
    rweight = get_feature_weight(rtable)
    #logging.info('\nFinish calculate rtable feature weights.')
    #if len(lweight) != len(rweight):
    #    raise AssertionError('Error: ltable and rtable don\'t have the'
    #                        ' same schema')

    Rank = namedtuple('Rank', ['index', 'weight'])
    rank_list = []
    for i in range(len(lweight)):
        rank_list.append(Rank(i, lweight[i] * rweight[i]))
    # if lkey_index != rkey_index:
    #     raise AssertionError('lkey and rkey doesn\'t have the same index')
    rank_list.pop(lkey_index)

    rank_list = sorted(rank_list, key=attrgetter('weight'), reverse=True)
    rank_index_list = []
    num_selected_fields = 0
    if len(rank_list) <= 3:
        num_selected_fields = len(rank_list)
    elif len(rank_list) <= 5:
        num_selected_fields = 3
    else:
        num_selected_fields = int(len(rank_list) / 2)
    for i in range(num_selected_fields):
        rank_index_list.append(rank_list[i].index)
    return sorted(rank_index_list)


def get_feature_weight(table):
    num_records = len(table)
    if num_records == 0:
        raise AssertionError('Error: empty table!')
    weight = []
    for col in table.columns:
        value_set = set()
        non_empty_count = 0;
        col_values = table[col]
        for value in col_values:
            if not pd.isnull(value) and value != '':
                value_set.add(value)
                non_empty_count += 1
        selectivity = 0.0
        if non_empty_count != 0:
            selectivity = len(value_set) * 1.0 / non_empty_count
        non_empty_ratio = non_empty_count * 1.0 / num_records
        weight.append(non_empty_ratio + selectivity)
    return weight


def get_record_id_to_index_map(table, table_key):
    record_id_to_index = {}
    id_col = list(table[table_key])
    for i in range(len(id_col)):
        # id_col[i] = str(id_col[i])
        if id_col[i] in record_id_to_index:
            raise AssertionError('Duplicate keys found:', id_col[i])
        record_id_to_index[id_col[i]] = i
    return record_id_to_index


def get_tokenized_table(table, table_key, feature_list):
    record_list = []
    columns = table.columns[feature_list]
    tmp_table = []
    for col in columns:
        column_token_list = get_tokenized_column(table[col])
        tmp_table.append(column_token_list)

    num_records = len(table[table_key])
    for i in range(num_records):
        token_list = []
        index_map = {}

        for j in range(len(columns)):
            tmp_col_tokens = tmp_table[j][i]
            for token in tmp_col_tokens:
                if token != '':
                    if token in index_map:
                        token_list.append(token + '_' + str(index_map[token]))
                        index_map[token] += 1
                    else:
                        token_list.append(token)
                        index_map[token] = 1
        record_list.append(token_list)

    return record_list


def get_tokenized_column(column):
    column_token_list = []
    for value in list(column):
        tmp_value = replace_nan_to_empty(value)
        if tmp_value != '':
            tmp_list = list(tmp_value.lower().split(' '))
            column_token_list.append(tmp_list)
        else:
            column_token_list.append([''])
    return column_token_list


def replace_nan_to_empty(field):
    if pd.isnull(field):
        return ''
    elif type(field) in [float, numpy.float64, int, numpy.int64]:
        return str('{0:.0f}'.format(field))
    else:
        return str(field)


def index_candidate_set(candidate_set, lrecord_id_to_index_map, rrecord_id_to_index_map, verbose):
    new_formatted_candidate_set = set()
    # # get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key =\
        cm.get_metadata_for_candset(candidate_set, logger, verbose)

    # # validate metadata
    cm.validate_metadata_for_candset(candidate_set, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                     logger, verbose)

    ltable_key_data = list(candidate_set[fk_ltable])
    rtable_key_data = list(candidate_set[fk_rtable])

    for i in range(len(ltable_key_data)):
        new_formatted_candidate_set.add((lrecord_id_to_index_map[ltable_key_data[i]],
                                         rrecord_id_to_index_map[rtable_key_data[i]]))

    # pair_list = []
    # ltable_key_data = list(candidate_set[fk_ltable])
    # for i in range(len(ltable_key_data)):
    #     lkey_value = str(ltable_key_data[i])
    #     if lkey_value not in lrecord_id_to_index_map:
    #         raise AssertionError('Error: the left key in the candidate'
    #                              ' set doesn\'t appear in the left'
    #                              ' table:', lkey_value)
    #     pair_list.append([lrecord_id_to_index_map[lkey_value]])
    # rtable_key_data = list(candidate_set[fk_rtable])
    # for i in range(len(rtable_key_data)):
    #     rkey_value = str(rtable_key_data[i])
    #     if rkey_value not in rrecord_id_to_index_map:
    #         raise AssertionError('Error: the left key in the candidate'
    #                              ' set doesn\'t appear in the left'
    #                              ' table: ', rkey_value)
    #     pair_list[i].append(rrecord_id_to_index_map[rkey_value])
    # for i in range(len(pair_list)):
    #     new_formatted_candidate_set.add((pair_list[i][0], pair_list[i][1]))

    return new_formatted_candidate_set


def build_global_token_order(record_list, order_dict):
    for record in record_list:
        for token in record:
            if token in order_dict:
                order_dict[token] = order_dict[token] + 1
            else:
                order_dict[token] = 1


def sort_record_tokens_by_global_order(record_list, order_dict):
    for i in range(len(record_list)):
        tmp_record = []
        for token in record_list[i]:
            if token in order_dict:
                tmp_record.append(token)
        record_list[i] = sorted(tmp_record, key=lambda x: order_dict[x])


def generate_prefix_events(lrecord_list, rrecord_list):
    prefix_events = []
    generate_prefix_events_impl(lrecord_list, prefix_events, 0)
    generate_prefix_events_impl(rrecord_list, prefix_events, 1)

    return prefix_events


def generate_prefix_events_impl(record_list, prefix_events, table_indicator):
    for i in range(len(record_list)):
        length = len(record_list[i])
        for j in range(length):
            threshold = calc_threshold(j, length)
            hq.heappush(prefix_events,
                        (-1.0 * threshold, table_indicator, i, j, record_list[i][j]))


def calc_threshold(token_index, record_length):
    return 1 - token_index * 1.0 / record_length


#if __name__ == "__main__":
    #ltable = mg.read_csv_metadata('../../datasets/test_datasets/A.csv', key='ID')
    #rtable = mg.read_csv_metadata('../../datasets/test_datasets/B.csv', key='ID')
    #cand_set = mg.read_csv_metadata('../../datasets/test_datasets/C.csv',
    #                                ltable=ltable, rtable=rtable, fk_ltable='ltable_ID',
    #                                fk_rtable='rtable_ID', key='_id')

    #ltable = mg.read_csv_metadata('../../datasets/CS784/S_hanli/tableA.csv', key='id')
    #rtable = mg.read_csv_metadata('../../datasets/CS784/S_hanli/tableB.csv', key='id')
    #cand_set = mg.read_csv_metadata('../../datasets/CS784/S_hanli/tableC_new.csv',
    #                                ltable=ltable, rtable=rtable, fk_ltable='ltable.id',
    #                                fk_rtable='rtable.id', key='_id')

    #ltable = mg.read_csv_metadata('../../datasets/CS784/S_shaleen/tableA.csv', key='id')
    #rtable = mg.read_csv_metadata('../../datasets/CS784/S_shaleen/tableB.csv', key='id')
    #cand_set = mg.read_csv_metadata('../../datasets/CS784/S_shaleen/tableC_new.csv',
    #                                ltable=ltable, rtable=rtable, fk_ltable='ltable.id',
    #                                fk_rtable='rtable.id', key='_id')

    #debug_blocker(ltable, rtable, cand_set)