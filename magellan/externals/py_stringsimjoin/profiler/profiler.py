"""Profiling tools"""

import pandas as pd

from py_stringsimjoin.utils.validation import validate_attr, \
                                              validate_input_table


def profile_table_for_join(input_table, profile_attrs=None):
    """Profiles the attributes in the table for join.
 
    Args:
        input table (dataframe): input table to profile.
        profile_attrs (list of strings): list of attributes to be profiled (defaults to None). If
                                         not provided, all attributes in the input table will be profiled.

    Returns:
        A dataframe consisting of profile output. Specifically, the dataframe contains three columns:
        1) 'Unique values' column, which shows the number of unique values in each attribute,
        2) 'Missing values' column, which shows the number of missing values in each attribute, and
        3) 'Comments' column, which contains comments about each attribute.
        The dataframe is indexed by attribute name.
    """
    # check if the input table is a dataframe
    validate_input_table(input_table, 'input table')

    profile_output = []

    if profile_attrs is None:
        profile_attrs = list(input_table.columns.values)
    else:
        # check if the profile attributes exist
        for attr in profile_attrs:
            validate_attr(attr, input_table.columns,
                          'profile attribute', 'input table')
            
    num_rows = len(input_table)

    for attr in profile_attrs:
        # compute number of unique values in the column
        unique_values = len(input_table[attr].unique())

        # compute number of missing values in the column
        missing_values = sum(pd.isnull(input_table[attr]))

        # compute percentage of unique values in the column
        unique_percent = round((float(unique_values) / float(num_rows)) * 100,
                               2)

        # compute percentage of missing values in the column
        missing_percent = round((float(missing_values) / float(num_rows)) * 100,
                                2)

        # format stats for better display
        formatted_unique_stat = _format_statistic(unique_values, unique_percent)
        formatted_missing_stat = _format_statistic(missing_values,
                                                   missing_percent)

        comments = ''
        # if there are missing values in the column, add a comment.
        if missing_percent > 0:
            comments = ''.join(['Joining on this attribute will ignore ',
                                formatted_missing_stat, ' rows.'])
        # if the column consists of unique values, add a comment. 
        if unique_percent == 100.0:
            comments = 'This attribute can be used as a key attribute.'

        profile_output.append((attr, formatted_unique_stat,
                               formatted_missing_stat, comments))

    # compose output dataframe containing the profiling results.
    output_header = ['Attribute', 'Unique values', 'Missing values', 'Comments']
    output_df = pd.DataFrame(profile_output, columns=output_header)
    return output_df.set_index('Attribute')

def _format_statistic(stat, stat_percent):
    return ''.join([str(stat), ' (', str(stat_percent), '%)'])
