"""Tokenizer utilities"""

from functools import partial

from magellan.externals.py_stringsimjoin.externals.py_stringmatching.tokenizers import delimiter
from magellan.externals.py_stringsimjoin.externals.py_stringmatching.tokenizers import qgram


class Tokenizer:
    """Tokenizer class.

    Attributes:
        tokenizer_type: String, type of tokenizer 'QGRAM' or 'DELIMITER'.
        tokenizer_function: tokenizer function.
        q_val: int, Q-gram length (used when tokenizer_type is 'QGRAM').
        delim_str: String, delimiter string (used when tokenizer_type is 'DELIMITER')
    """
    def __init__(self, tokenizer_type, tokenizer_function,
                 q_val=-1, delim_str=''):
        self.tokenizer_type = tokenizer_type
        self.tokenizer_function = tokenizer_function
        if self.tokenizer_type == 'QGRAM':
            self.q_val = q_val
        elif self.tokenizer_type == 'DELIMITER':
            self.delim_str = delim_str

    def tokenize(self, input_string):
        """Tokenize input string.

        Args:
            input_string (str): Input string to be tokenized

        Returns:
            List of tokens

        Usage:
            >>> delim_tokenizer = create_delimiter_tokenizer(' ')
            >>> tokens = delim_tokenizer.tokenize('hello world !!')
        """
        return self.tokenizer_function(input_string)


def create_delimiter_tokenizer(delim_str=' '):
    """Creates a delimiter based tokenizer using the given delimiter.

    Args:
        delim_str (str): Delimiter string

    Returns:
        Tokenizer object

    Examples:
        >>> delim_tokenizer = create_delimiter_tokenizer(',')
    """
    return Tokenizer('DELIMITER',
                     partial(delimiter, delim_str=delim_str),
                     delim_str = delim_str)


def create_qgram_tokenizer(q_val=2):
    """Creates a qgram based tokenizer using the given q value.

    Args:
        qval (int): Q-gram length (defaults to 2)

    Returns:
        Tokenizer object

    Examples:
        >>> qg_tokenizer = create_qgram_tokenizer(3)
    """
    return Tokenizer('QGRAM',
                     partial(qgram, qval=q_val),
                     q_val = q_val)


def tokenize(input_string, tokenizer, sim_measure_type='OVERLAP'):
    if sim_measure_type == 'EDIT_DISTANCE':
        return tokenizer.tokenize(input_string)
    else:
        return list(set(tokenizer.tokenize(input_string)))
