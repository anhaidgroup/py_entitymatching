
import os

from six.moves import xrange
import pandas as pd

from magellan.externals.py_stringsimjoin.utils import install_path


def get_output_row_from_tables(l_row, r_row,
                               l_key_attr_index, r_key_attr_index, 
                               l_out_attrs_indices=None,
                               r_out_attrs_indices=None):
    output_row = []
    
    # add ltable id attr
    output_row.append(l_row[l_key_attr_index])

    # add ltable output attributes
    if l_out_attrs_indices:
        for l_attr_index in l_out_attrs_indices:
            output_row.append(l_row[l_attr_index])

    # add rtable id attr
    output_row.append(r_row[r_key_attr_index])

    # add rtable output attributes
    if r_out_attrs_indices:
        for r_attr_index in r_out_attrs_indices:
            output_row.append(r_row[r_attr_index])

    return output_row


def get_output_row_from_candset(row_dict, out_attrs):
    output_row = []

    for attr in out_attrs:
        output_row.append(row_dict[attr])

    return output_row


def get_output_header_from_tables(l_key_attr, r_key_attr,
                                  l_out_attrs, r_out_attrs,
                                  l_out_prefix, r_out_prefix):
    output_header = []

    output_header.append(l_out_prefix + l_key_attr)

    if l_out_attrs:
        for l_attr in l_out_attrs:
            output_header.append(l_out_prefix + l_attr)

    output_header.append(r_out_prefix + r_key_attr)

    if r_out_attrs:
        for r_attr in r_out_attrs:
            output_header.append(r_out_prefix + r_attr)

    return output_header


def convert_dataframe_to_list(table, join_attr_index,
                              remove_null=True, remove_empty=True):
    table_list = []
    for row in table.itertuples(index=False):
        if remove_null and pd.isnull(row[join_attr_index]):
            continue
        if remove_empty and len(str(row[join_attr_index])) == 0:
            continue
        table_list.append(tuple(row))
    return table_list


def build_dict_from_table(table, key_attr_index, join_attr_index,
                          remove_null=True, remove_empty=True):
    table_dict = {}
    for row in table.itertuples(index=False):
        if remove_null and pd.isnull(row[join_attr_index]):
            continue
        if remove_empty and len(str(row[join_attr_index])) == 0:
            continue
        table_dict[row[key_attr_index]] = tuple(row)
    return table_dict


def find_output_attribute_indices(original_columns, output_attributes):
    output_attribute_indices = []
    if output_attributes is not None:
        for attr in output_attributes:
            output_attribute_indices.append(original_columns.index(attr))
    return output_attribute_indices


def split_table(table, num_splits):
    splits = []
    split_size = 1.0/num_splits*len(table)
    for i in xrange(num_splits):
        splits.append(table[int(round(i*split_size)):int(round((i+1)*split_size))])
    return splits


def get_install_path():
    path_list = install_path.split(os.sep)
    return os.sep.join(path_list[0:len(path_list)-1])
