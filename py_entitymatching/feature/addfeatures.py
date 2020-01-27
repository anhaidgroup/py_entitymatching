"""
This module contains functions to add a feature to feature table.
"""
import logging

import pandas as pd
import six

from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def get_feature_fn(feature_string, tokenizers, similarity_functions):
    """
    This function creates a feature in a declarative manner.

    Specifically, this function uses the feature string, parses it and
    compiles it into a function using the given tokenizers and similarity
    functions. This compiled function will take in two tuples and return a
    feature value (typically a number).

    Args:
        feature_string (string): A feature expression
            to be converted into a function.
        tokenizers (dictionary): A Python dictionary containing tokenizers.
            Specifically, the dictionary contains tokenizer names as keys and
            tokenizer functions as values. The tokenizer function typically
            takes in a string and returns a list of tokens.
        similarity_functions (dictionary): A Python dictionary containing
            similarity functions. Specifically, the dictionary contains
            similarity function names as keys and similarity functions as
            values. The similarity function typically
            takes in a string or two lists of tokens and returns a number.

    Returns:

        This function returns a Python dictionary which contains sufficient
        information (such as attributes, tokenizers, function code) to be added
        to the feature table.

        Specifically the Python dictionary contains the following keys:
        'left_attribute', 'right_attribute',
        'left_attr_tokenizer',
        'right_attr_tokenizer', 'simfunction', 'function', and
        'function_source'.

        For all the keys except the 'function' and 'function_source' the
        value will be either a valid string (if the input feature string is
        parsed correctly) or PARSE_EXP (if the parsing was not successful).
        The 'function' will have a valid Python function as value,
        and 'function_source' will have the Python function's source in
        string format.

        The created function is a self-contained function
        which means that the tokenizers and sim functions that it calls are
        bundled along with the returned function code.

    Raises:
        AssertionError: If `feature_string` is not of type string.
        AssertionError: If the input `tokenizers` is not of type
            dictionary.
        AssertionError: If the input `similarity_functions` is not of
            type dictionary.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')

        >>> block_t = em.get_tokenizers_for_blocking()
        >>> block_s = em.get_sim_funs_for_blocking()
        >>> block_f = em.get_features_for_blocking(A, B)
        >>> r = get_feature_fn('jaccard(qgm_3(ltuple.name), qgm_3(rtuple.name)', block_t, block_s)
        >>> em.add_feature(block_f, 'name_name_jac_qgm3_qgm3', r)

        >>> match_t = em.get_tokenizers_for_matching()
        >>> match_s = em.get_sim_funs_for_matching()
        >>> match_f = em.get_features_for_matching(A, B)
        >>> r = get_feature_fn('jaccard(qgm_3(ltuple.name), qgm_3(rtuple.name)', match_t, match_s)
        >>> em.add_feature(match_f, 'name_name_jac_qgm3_qgm3', r)


    See Also:
        :meth:`py_entitymatching.get_sim_funs_for_blocking`,
        :meth:`py_entitymatching.get_tokenizers_for_blocking`,
        :meth:`py_entitymatching.get_sim_funs_for_matching`,
        :meth:`py_entitymatching.get_tokenizers_for_matching`
    """
    # Validate input parameters
    # # We expect the input feature string to be of type string
    validate_object_type(feature_string, six.string_types, error_prefix='Input feature')

    # # We expect the input object tokenizers to be of type python dictionary
    validate_object_type(tokenizers, dict, error_prefix='Input object (tokenizers)')

    # # We expect the input object similarity functions to be of type python
    # dictionary
    validate_object_type(similarity_functions, dict, error_prefix='Input object (similarity_functions)')

    # Initialize a dictionary to have tokenizers/similarity functions
    dict_to_compile = {}
    # Update the dictionary with similarity functions
    if len(similarity_functions) > 0:
        dict_to_compile.update(similarity_functions)
    # Update the dictionary with tokenizers
    if len(tokenizers) > 0:
        dict_to_compile.update(tokenizers)

    # Create a python function string based on the input feature string
    function_string = 'def fn(ltuple, rtuple):\n'
    function_string += '    '
    function_string += 'return ' + feature_string

    # Parse the feature string to get the tokenizer, sim. function, and the
    # attribute that it is operating on
    parsed_dict = _parse_feat_str(feature_string, tokenizers,
                                  similarity_functions)

    # Compile the function string using the constructed dictionary
    six.exec_(function_string, dict_to_compile)

    # Update the parsed dict with the function and the function source
    parsed_dict['function'] = dict_to_compile['fn']
    parsed_dict['function_source'] = function_string

    # Finally, return the parsed dictionary
    return parsed_dict


# parse input feature string
def _parse_feat_str(feature_string, tokenizers, similarity_functions):
    """
    This function parses the feature string to get left attribute,
    right attribute, tokenizer, similarity function
    """
    # Validate the input parameters
    # # We expect the input feature string to be of type string
    validate_object_type(feature_string, six.string_types, error_prefix='Input feature')

    # # We expect the input object tokenizers to be of type python dictionary
    validate_object_type(tokenizers, dict, error_prefix='Input object (tokenizers)')

    # # We expect the input object similarity functions to be of type python
    # dictionary
    validate_object_type(similarity_functions, dict, error_prefix='Input object (similarity_functions)')

    # We will have to parse the feature string. Specifically we use pyparsing
    #  module for the parsing purposes

    from pyparsing import Word, alphanums, ParseException

    # initialization attributes, tokenizers and similarity function parsing
    # result
    left_attribute = 'PARSE_EXP'
    right_attribute = 'PARSE_EXP'
    left_attr_tokenizer = 'PARSE_EXP'
    right_attr_tokenizer = 'PARSE_EXP'
    sim_function = 'PARSE_EXP'

    exception_flag = False

    # Define structures for each type such as attribute name, tokenizer
    # function
    attr_name = Word(alphanums + "_" + "." + "[" + "]" + '"' + "'")
    tok_fn = Word(alphanums + "_") + "(" + attr_name + ")"
    wo_tok = Word(alphanums + "_") + "(" + attr_name + "," + attr_name + ")"
    wi_tok = Word(alphanums + "_") + "(" + tok_fn + "," + tok_fn + ")"
    feat = wi_tok | wo_tok
    # Try to parse the string
    try:
        parsed_string = feat.parseString(feature_string)
    except ParseException as _:
        exception_flag = True

    if not exception_flag:
        # Parse the tokenizers
        parsed_tokenizers = [value for value in parsed_string
                             if value in tokenizers.keys()]
        if len(parsed_tokenizers) is 2:
            left_attr_tokenizer = parsed_tokenizers[0]
            right_attr_tokenizer = parsed_tokenizers[1]

        # Parse the similarity functions
        parsed_similarity_function = [value for value in parsed_string
                                      if value in similarity_functions.keys()]
        if len(parsed_similarity_function) == 1:
            sim_function = parsed_similarity_function[0]
        # Parse the left attribute
        attribute = [value for value in parsed_string
                     if value.startswith('ltuple[')]
        if len(attribute) == 1:
            attribute = attribute[0]
            left_attribute = attribute[7:len(attribute) - 1].strip('"').strip(
                "'")

        # Parse the right attribute
        attribute = [val for val in parsed_string if val.startswith('rtuple[')]
        if len(attribute) == 1:
            attribute = attribute[0]
            right_attribute = attribute[7:len(attribute) - 1].strip('"').strip(
                "'")
    else:
        pass

    # Return the parsed information in a dictionary format.

    parsed_dict = {'left_attribute': left_attribute,
                   'right_attribute': right_attribute,
                   'left_attr_tokenizer': left_attr_tokenizer,
                   'right_attr_tokenizer': right_attr_tokenizer,
                   'simfunction': sim_function,
                   'is_auto_generated': False}

    return parsed_dict


def add_feature(feature_table, feature_name, feature_dict):
    """
    Adds a feature to the feature table.

    Specifically, this function is used in combination with
    :meth:`~py_entitymatching.get_feature_fn`.
    First the user creates a dictionary using :meth:`~py_entitymatching.get_feature_fn`,
    then the user uses this function to add feature_dict to the feature table.

    Args:
        feature_table (DataFrame): A DataFrame containing features.
        feature_name (string): The name that should be given to the feature.
        feature_dict (dictionary): A Python dictionary, that is typically
            returned by executing :meth:`~py_entitymatching.get_feature_fn`.

    Returns:
        A Boolean value of True is returned if the addition was successful.

    Raises:
        AssertionError: If the input `feature_table` is not of type
            pandas DataFrame.
        AssertionError: If `feature_name` is not of type
            string.
        AssertionError: If `feature_dict` is not of type
            Python dictionary.
        AssertionError: If the `feature_table` does not have necessary columns
            such as 'feature_name', 'left_attribute', 'right_attribute',
            'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function', and
            'function_source' in the DataFrame.
        AssertionError: If the `feature_name` is already present in the feature
            table.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')

        >>> block_t = em.get_tokenizers_for_blocking()
        >>> block_s = em.get_sim_funs_for_blocking()
        >>> block_f = em.get_features_for_blocking(A, B)
        >>> r = get_feature_fn('jaccard(qgm_3(ltuple.name), qgm_3(rtuple.name)', block_t, block_s)
        >>> em.add_feature(block_f, 'name_name_jac_qgm3_qgm3', r)

        >>> match_t = em.get_tokenizers_for_matching()
        >>> match_s = em.get_sim_funs_for_matching()
        >>> match_f = em.get_features_for_matching(A, B)
        >>> r = get_feature_fn('jaccard(qgm_3(ltuple.name), qgm_3(rtuple.name)', match_t, match_s)
        >>> em.add_feature(match_f, 'name_name_jac_qgm3_qgm3', r)
    """
    # Validate input parameters
    # # We expect the feature_table to be of pandas DataFrame
    validate_object_type(feature_table, pd.DataFrame, 'Input feature table')

    # # We expect the feature_name to be of type string
    validate_object_type(feature_name, six.string_types, 'Input feature name')

    # # We expect the feature_dict to be of type python dictionary
    validate_object_type(feature_dict, dict, 'Input feature dictionary')

    # # We expect the feature table to contain certain columns
    missing_columns = get_missing_column_values(feature_table.columns)
    if missing_columns:
        error_msg = "Feature table does not have all required columns\n The following columns are missing: {0}".format(", ".join(missing_columns))
        raise AssertionError(error_msg)

    feature_names = list(feature_table['feature_name'])
    if feature_name in feature_names:
        logger.error('Input feature name is already present in feature table')
        raise AssertionError(
            'Input feature name is already present in feature table')

    # Add feature to the feature table at last
    feature_dict['feature_name'] = feature_name
    if len(feature_table) > 0:
        feature_table.loc[len(feature_table)] = feature_dict
    else:
        feature_table.columns = ['feature_name', 'left_attribute',
                                 'right_attribute', 'left_attr_tokenizer',
                                 'right_attr_tokenizer', 'simfunction',
                                 'function',
                                 'function_source',
                                 'is_auto_generated']
        feature_table.loc[len(feature_table)] = feature_dict
    # Finally, return True if everything was fine
    return True


def get_missing_column_values(column):
    required_columns_items = ['feature_name', 'left_attribute', 'right_attribute',
                    'left_attr_tokenizer',
                    'right_attr_tokenizer', 'simfunction', 'function',
                    'function_source', 'is_auto_generated']
    return [item for item in required_columns_items if item not in column]


def create_feature_table():
    """
    Creates an empty feature table.
    """
    # Fix the column names
    column_names = ['feature_name', 'left_attribute', 'right_attribute',
                    'left_attr_tokenizer',
                    'right_attr_tokenizer', 'simfunction', 'function',
                    'function_source', 'is_auto_generated']
    # Create a pandas DataFrame using the column names
    feature_table = pd.DataFrame(columns=column_names)

    # Finally, return the feature table
    return feature_table


def add_blackbox_feature(feature_table, feature_name, feature_function, **kwargs):
    """
    Adds a black box feature to the feature table.

    Args:
        feature_table (DataFrame): The input DataFrame (typically a feature
            table) to which the feature must be added.
        feature_name (string): The name that should be given to the feature.
        feature_function (Python function): A Python function for the black box
            feature.

    Returns:
        A Boolean value of True is returned if the addition was successful.

    Raises:
        AssertionError: If the input `feature_table` is not of type
            DataFrame.
        AssertionError: If the input `feature_name` is not of type
            string.
        AssertionError: If the `feature_table` does not have necessary columns
            such as 'feature_name', 'left_attribute', 'right_attribute',
            'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function', and
            'function_source' in the DataFrame.
        AssertionError: If the `feature_name` is already present in the
            feature table.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> block_f = em.get_features_for_blocking(A, B)
        >>> def age_diff(ltuple, rtuple):
        >>>     # assume that the tuples have age attribute and values are valid numbers.
        >>>   return ltuple['age'] - rtuple['age']
        >>> status = em.add_blackbox_feature(block_f, 'age_difference', age_diff)

    """
    # Validate input parameters
    # # We expect the feature_table to be of type pandas DataFrame
    validate_object_type(feature_table, pd.DataFrame, 'Input feature table')

    # # We expect the feature_name to be of type string
    validate_object_type(feature_name, six.string_types, 'Input feature name')

    # Check if the input feature table contains necessary columns
    dummy_feature_table = create_feature_table()
    if sorted(dummy_feature_table.columns) != sorted(feature_table.columns):
        logger.error('Input feature table does not have the necessary columns')
        raise AssertionError(
            'Input feature table does not have the necessary columns')

    # Check if the feature table already contains the given feature name
    feat_names = list(feature_table['feature_name'])
    if feature_name in feat_names:
        logger.error('Input feature name is already present in feature table')
        raise AssertionError(
            'Input feature name is already present in feature table')

    feature_dict = {}
    feature_dict['feature_name'] = feature_name
    feature_dict['function'] = feature_function
    feature_dict['left_attribute'] = kwargs.get('left_attribute')
    feature_dict['right_attribute'] = kwargs.get('right_attribute')
    feature_dict['left_attr_tokenizer'] = kwargs.get('left_attr_tokenizer')
    feature_dict['right_attr_tokenizer'] = kwargs.get('right_attr_tokenizer')
    feature_dict['simfunction'] = kwargs.get('simfunction')
    feature_dict['function_source'] = kwargs.get('function_source')
    feature_dict['is_auto_generated'] = False

    # Add the feature to the feature table as a last entry.
    if len(feature_table) > 0:
        feature_table.loc[len(feature_table)] = feature_dict
    else:
        feature_table.columns = ['feature_name', 'left_attribute',
                                 'right_attribute', 'left_attr_tokenizer',
                                 'right_attr_tokenizer', 'simfunction',
                                 'function',
                                 'function_source', 'is_auto_generated']
        feature_table.loc[len(feature_table)] = feature_dict
    # Finally return True if the addition was successful
    return True
