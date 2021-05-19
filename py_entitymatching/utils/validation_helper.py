import pandas as pd
import six


def type_name(expected_type):
    messages = {
        six.string_types: 'string',
        pd.DataFrame: 'pandas dataframe',
        list: 'list',
        bool: 'bool',
        int: 'int',
        dict: 'dictionary',
        str: 'str',
    }
    return messages[expected_type]


def validate_object_type(input_object, expected_type, error_prefix='Input object'):
    if not isinstance(input_object, expected_type):
        error_message = '{0}: {1} \nis not of type {2}'.format(error_prefix, str(input_object), type_name(expected_type))
        raise AssertionError(error_message)


def validate_subclass(input_class, expected_class, error_prefix='Input class'):
    if not issubclass(input_class, expected_class):
        error_message = f'{error_prefix}: {str(input_class)}\nis is not a sublcass of {str(expected_class)}'
        raise AssertionError(error_message)
