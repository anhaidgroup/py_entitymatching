"""
This module contains functions to add a feature to feature table.
"""
import logging

import pandas as pd
import six

logger = logging.getLogger(__name__)


def get_feature_fn(feature_string, tokenizers, similarity_functions):
    """
    This function creates a feature in a declarative manner.

    Specifically, this function uses the feature string and compiles it into
    an function using tokenizers and similarity functions. This compiled
    function will take in two tuples and return the feature (typically a
    number).

    Args:
        feature_string (str): Feature expression to be converted into a
            function.
        tokenizers (dictionary): A python dictionary containing tokenizers.
            Specifically, the dictionary contains tokenizer names as keys and
            tokenizer functions as values. The tokenizer function typically
            takes in a string and returns a list of tokens.
        similarity_functions (dictionary): A python dictionary containing
            similarity functions. Specifically, the dictionary contains
            similarity function names as keys and similarity functions as
            values. The similarity function typically
            takes in a string or two lists and returns a number.

    Returns:
        This function returns a dictionary which contains sufficient information
        (such as parsed information, function code) to be added to the
        feature table. The created function is ”self contained” function
        which means that it the tokenizers or sim function that it calls is
        bundled along with the returned function code.

    Raises:
        AssertionError: If the input feature string is not of type string.
        AssertionError: If th input object tokenizers is not of type
            dictionary.
        AssertionError: If the input object similarity_functions is not of
            type dictionary.
    """
    # Validate input parameters
    # # We expect the input feature string to be of type string
    if not isinstance(feature_string, six.string_types):
        logger.error('Input feature string is not of type string')
        raise AssertionError('Input feature string is not of type string')

    # # We expect the input object tokenizers to be of type python dictionary
    if not isinstance(tokenizers, dict):
        logger.error('Input object (tokenizers) is not of type dict')
        raise AssertionError('Input object (tokenizers) is not of type dict')

    # # We expect the input object similarity functions to be of type python
    # dictionary
    if not isinstance(similarity_functions, dict):
        logger.error('Input object (similarity_functions) is not of type dict')
        raise AssertionError('Input object (similarity_functions) is not of '
                             'type dict')

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
    if not isinstance(feature_string, six.string_types):
        logger.error('Input feature string is not of type string')
        raise AssertionError('Input feature string is not of type string')

    # # We expect the input object (tokenizers) to be of type dict
    if not isinstance(tokenizers, dict):
        logger.error('Input tokenizers is not of type dict')
        raise AssertionError('Input tokenizers is not of type dict')

    # # We expect the input object (similarity functions) to be of type dict
    if not isinstance(similarity_functions, dict):
        logger.error('Input similarity functions is not of type dict')
        raise AssertionError('Input sim. is not of type dict')

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
                   'simfunction': sim_function}
    return parsed_dict


def add_feature(feature_table, feature_name, feature_dict):
    """
    Adds feature to the feature table.

    Specifically, this function is used in combination with  get_feature_fn.
    First the user creates a dictionary using get_feature_fn, then the user
    uses this function to add it to the feature table.

    Args:
        feature_table (DataFrame): A DataFrame containing features.
        feature_names (str): Name that should be given to the feature.
        feature_dict (dictionary): Python dictionary, that is typically
            returned by executing get_feature_fn

    Returns:
        A boolean value of True is returned if the addition was successful.

    Raises:
        AssertionError: If the input object (feature_table) is not of type
            pandas DataFrame.
        AssertionError: If the input object (feature_name) is not of type
            string.
        AssertionError: If the input object (feature_dict) is not of type
            python dictionary.
        AssertionError: If the feature_table does not have necessary columns
            such as 'feature_name', 'left_attribute', 'right_attribute',
            'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function',
            'function_source' in the DataFrame.
        AssertionError: If the feature name is already present in the feature
            table.

    """
    # Validate input parameters
    # # We expect the feature_table to be of pandas DataFrame
    if not isinstance(feature_table, pd.DataFrame):
        logger.error('Input feature table is not of type DataFrame')
        raise AssertionError('Input feature table is not of type DataFrame')

    # # We expect the feature_name to be of type string
    if not isinstance(feature_name, six.string_types):
        logger.error('Input feature name is not of type string')
        raise AssertionError('Input feature name is not of type string')

    # # We expect the feature_dict to be of type python dictionary
    if not isinstance(feature_dict, dict):
        logger.error('Input feature dictionary is not of type dict')
        raise AssertionError('Input feature dictionary is not of type dict')

    # # We expect the feature table to contain certain columns
    dummy_feature_table = create_feature_table()
    if sorted(dummy_feature_table.columns) != sorted(feature_table.columns):
        logger.error('Input feature table does not have the necessary columns')
        raise AssertionError(
            'Input feature table does not have the necessary columns')
    # Check whether the feature name is already present in the feature table
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
                                 'function_source']
        feature_table.loc[len(feature_table)] = feature_dict
    # Finally, return True if everything was fine
    return True


def create_feature_table():
    """
    Creates an empty feature table.
    """
    # Fix the column names
    column_names = ['feature_name', 'left_attribute', 'right_attribute',
            'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function',
            'function_source']
    # Create a pandas DataFrame using the column names
    feature_table = pd.DataFrame(columns=column_names)

    # Finally, return the feature table
    return feature_table


def add_blackbox_feature(feature_table, feature_name, feature_function):
    """
    Adds black box feature to the feature table.
    Args:
        feature_table (DataFrame): Input DataFrame to which the feature must
            be added.
        feature_name (str): Name that should be given to the feature.
        feature_function (Python function): Python function for the black box
            feature.
    Returns:
        A boolean value of True is returned if the addition was successful.

    Raises:
        AssertionError: If the input object (feature_table) is not of type
            DataFrame.
        AssertionError: If the input object (feature_name) is not of type
            string.
        AssertionError: If the feature_table does not have necessary columns
            such as 'feature_name', 'left_attribute', 'right_attribute',
            'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function',
            'function_source' in the DataFrame.
        AssertionError: If the feature name is already present in the feature
            table.


    """
    # Validate input parameters
    # # We expect the feature_table to be of type pandas DataFrame
    if not isinstance(feature_table, pd.DataFrame):
        logger.error('Input feature table is not of type DataFrame')
        raise AssertionError('Input feature table is not of type DataFrame')

    # # We expect the feature_name to be of type string
    if not isinstance(feature_name, six.string_types):
        logger.error('Input feature name is not of type string')
        raise AssertionError('Input feature name is not of type string')

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
    feature_dict['left_attribute'] = None
    feature_dict['right_attribute'] = None
    feature_dict['left_attr_tokenizer'] = None
    feature_dict['right_attr_tokenizer'] = None
    feature_dict['simfunction'] = None
    feature_dict['function_source'] = None

    # Add the feature to the feature table as a last entry.
    if len(feature_table) > 0:
        feature_table.loc[len(feature_table)] = feature_dict
    else:
        feature_table.columns = ['feature_name', 'left_attribute',
                              'right_attribute', 'left_attr_tokenizer',
                              'right_attr_tokenizer', 'simfunction', 'function',
                              'function_source']
        feature_table.loc[len(feature_table)] = feature_dict
    # Finally return True if the addition was successful
    return True
