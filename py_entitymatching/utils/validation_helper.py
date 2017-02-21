import pandas as pd
import six


def type_name(expected_type):
    messages = {
        six.string_types: 'string',
        pd.DataFrame: 'pandas dataframe',
        list: 'list',
        bool: 'bool',
        int: 'int',
        dict: 'dictionary'
    }
    return messages[expected_type]


def validate_object_type(input_object, expected_type, param_name='Input object'):
    if not isinstance(input_object, expected_type):
        error_message = '{0}: {1} \nis not of type {2}'.format(param_name, str(input_object), type_name(expected_type))
        raise AssertionError(error_message)