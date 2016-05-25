import cython

from magellan.externals.py_stringmatching.compat import _range
from magellan.externals.py_stringmatching import utils


# @todo: add examples in the comments

def qgram(input_string, qval=2):
    """
    Tokenizes input string into q-grams.

    A q-gram is defined as all sequences of q characters. Q-grams are also known as n-grams and
    k-grams.

    Args:
        input_string (str): Input string

        qval (int): Q-gram length (defaults to 2)

    Returns:
        Token list (list)

    Raises:
        TypeError : If the input is not a string

    Examples:
        >>> qgram('database')
        ['da','at','ta','ab','ba','as','se']
        >>> qgram('a')
        []
        >>> qgram('database', 3)
        ['dat', 'ata', 'tab', 'aba', 'bas', 'ase']


    """
    utils.tok_check_for_none(input_string)
    utils.tok_check_for_string_input(input_string)

    qgram_list = []

    if len(input_string) < qval or qval < 1:
        return qgram_list

    qgram_list = [input_string[i:i + qval] for i in _range(len(input_string) - (qval - 1))]
    return qgram_list


def delimiter(input_string, delim_str=' '):
    """
    Tokenizes input string based on the given delimiter.

    Args:
        input_string (str): Input string

        delim_str (str): Delimiter string


    Returns:
        Token list (list)

    Raises:
        TypeError : If the input is not a string

    Examples:
        >>> delimiter('data science')
        ['data', 'science']
        >>> delimiter('data$#$science', '$#$')
        ['data', 'science']
        >>> delimiter('data science', ',')
        ['data science']

    """
    utils.tok_check_for_none(input_string)
    utils.tok_check_for_string_input(input_string)

    return input_string.split(delim_str)


def whitespace(input_string):
    """
    Tokenizes input string based on white space.

    Args:
        input_string (str): Input string

    Returns:
        Token list (list)

    Raises:
        TypeError : If the input is not a string

    Examples:
        >>> whitespace('data science')
        ['data', 'science']
        >>> whitespace('data        science')
        ['data', 'science']
        >>> whitespace('data\tscience')
        ['data', 'science']

    """
    utils.tok_check_for_none(input_string)
    utils.tok_check_for_string_input(input_string)

    return input_string.split()
