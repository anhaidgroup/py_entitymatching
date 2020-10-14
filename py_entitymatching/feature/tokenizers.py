# coding=utf-8
"""
This module contains the tokenizer functions supported by py_entitymatching.
"""
import logging

import pandas as pd
import numpy as np
import six

import py_stringmatching as sm
import py_entitymatching.utils.generic_helper as gh

logger = logging.getLogger(__name__)

# Initialize global tokenizers
_global_tokenizers = pd.DataFrame(
    {'function_name': ['tok_qgram', 'tok_delim', 'tok_wspace'],
     'short_name': ['qgm', 'dlm', 'wsp']})


def get_tokenizers_for_blocking(q=[2, 3], dlm_char=[' ']):
    """
    This function returns the single argument tokenizers that can be used for
    blocking purposes (typically in rule-based blocking).

    Args:
        q (list): The list of integers (i.e q value) for which the q-gram
            tokenizer must be generated (defaults to [2, 3]).
        dlm_char (list): The list of characters (i.e delimiter character) for
            which the delimiter tokenizer must be generated (defaults to [` ']).

    Returns:
        A Python dictionary with tokenizer name as the key and tokenizer
        function as the value.

    Raises:
        AssertionError: If both `q` and `dlm_char` are set to None.

    Examples:
        >>> import py_entitymatching as em
        >>> block_t = em.get_tokenizers_for_blocking()
        >>> block_t = em.get_tokenizers_for_blocking(q=[3], dlm_char=None)
        >>> block_t = em.get_tokenizers_for_blocking(q=None, dlm_char=[' '])

    """
    # Validate inputs
    if q is None and dlm_char is None:
        logger.error('Both q and dlm_char cannot be null')
        raise AssertionError('Both q and dlm_char cannot be null')
    else:
        # Return single arg tokenizers for the given inputs.
        return _get_single_arg_tokenizers(q, dlm_char)


def get_tokenizers_for_matching(q=[2, 3], dlm_char=[' ']):
    """
    This function returns the single argument tokenizers that can be used for
    matching purposes.

    Args:
        q (list): The list of integers (i.e q value) for which the q-gram
            tokenizer must be generated (defaults to [2, 3]).
        dlm_char (list): The list of characters (i.e delimiter character) for
            which the delimiter tokenizer must be generated (defaults to [` ']).

    Returns:
        A Python dictionary with tokenizer name as the key and tokenizer
        function as the value.

    Raises:
        AssertionError: If both `q` and `dlm_char` are set to None.

    Examples:
        >>> import py_entitymatching as em
        >>> match_t = em.get_tokenizers_for_blocking()
        >>> match_t = em.get_tokenizers_for_blocking(q=[3], dlm_char=None)
        >>> match_t = em.get_tokenizers_for_blocking(q=None, dlm_char=[' '])

    """

    if q is None and dlm_char is None:
        logger.error('Both q and dlm_char cannot be null')
        raise AssertionError('Both q and dlm_char cannot be null')
    else:
        # Return single arg tokenizers for the given inputs.
        return _get_single_arg_tokenizers(q, dlm_char)


def _get_single_arg_tokenizers(q=[2, 3], dlm_char=[' ']):
    """
    This function creates single argument tokenizers for the given input
    parameters.
    """
    # Validate the input parameters
    if q is None and dlm_char is None:
        logger.error('Both q and dlm_char cannot be null')
        raise AssertionError('Both q and dlm_char cannot be null')
    # Initialize the key (function names) and value dictionaries (tokenizer
    # functions).
    names = []
    functions = []

    if q is not None:
        if not isinstance(q, list):
            q = [q]

        # Create a qgram function for the given list of q's
        qgm_fn_list = [_make_tok_qgram(k) for k in q]
        qgm_names = ['qgm_' + str(x) for x in q]
        # Update the tokenizer name, function lists
        names.extend(qgm_names)
        functions.extend(qgm_fn_list)

    names.append('wspace')
    functions.append(tok_wspace)

    names.append('alphabetic')
    functions.append(tok_alphabetic)

    names.append('alphanumeric')
    functions.append(tok_alphanumeric)



    if dlm_char is not None:
        if not isinstance(dlm_char, list) and isinstance(dlm_char,
                                                         six.string_types):
            dlm_char = [dlm_char]
        # Create a delimiter function for the given list of q's
        dlm_fn_list = [_make_tok_delim(k) for k in dlm_char]

        # Update the tokenizer name, function lists
        dlm_names = ['dlm_dc' + str(i) for i in range(len(dlm_char))]
        names.extend(dlm_names)
        functions.extend(dlm_fn_list)


    if len(names) > 0 and len(functions) > 0:
        return dict(zip(names, functions))
    else:
        logger.warning('Didnot create any tokenizers, returning empty dict.')
        return dict()



def _make_tok_delim(d):
    """
    This function returns a delimiter-based tokenizer with a fixed delimiter
    """
    def tok_delim(s):
        # check if the input is of type base string
        if pd.isnull(s):
            return s
        # Remove non ascii  characters. Note: This should be fixed in the
        # next version.
        #s = remove_non_ascii(s)

        s = gh.convert_to_str_unicode(s)

        # Initialize the tokenizer measure object
        measure = sm.DelimiterTokenizer(delim_set=[d])
        # Call the function that will tokenize the input string.
        return measure.tokenize(s)

    return tok_delim


# return a qgram-based tokenizer with a fixed q
def _make_tok_qgram(q):
    """
    This function returns a qgran-based tokenizer with a fixed delimiter
    """

    def tok_qgram(s):
        # check if the input is of type base string
        if pd.isnull(s):
            return s

        s = gh.convert_to_str_unicode(s)

        measure = sm.QgramTokenizer(qval=q)
        return measure.tokenize(s)
    return tok_qgram


# q-gram tokenizer
def tok_qgram(input_string, q):
    """
    This function splits the input string into a list of q-grams. Note that,
    by default the input strings are padded and then tokenized.

    Args:
        input_string (string): Input string that should be tokenized.
        q (int): q-val that should be used to tokenize the input string.

    Returns:
        A list of tokens, if the input string is not NaN,
        else returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.tok_qgram('database', q=2)
        ['#d', 'da', 'at', 'ta', 'ab', 'ba', 'as', 'se', 'e$']
        >>> em.tok_qgram('database', q=3)
        ['##d', '#da', 'dat', 'ata', 'tab', 'aba', 'bas', 'ase', 'se$', 'e$$']
        >>> em.tok_qgram(None, q=2)
        nan
    """

    if pd.isnull(input_string):
        return np.NaN

    input_string = gh.convert_to_str_unicode(input_string)
    measure = sm.QgramTokenizer(qval=q)

    return measure.tokenize(input_string)


def tok_delim(input_string, d):
    """
    This function splits the input string into a list of tokens
    (based on the delimiter).

    Args:
        input_string (string): Input string that should be tokenized.
        d (string): Delimiter string.

    Returns:
        A list of tokens, if the input string is not NaN ,
        else returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.tok_delim('data science', ' ')
        ['data', 'science']
        >>> em.tok_delim('data$#$science', '$#$')
        ['data', 'science']
        >>> em.tok_delim(None, ' ')
        nan


    """

    if pd.isnull(input_string):
        return np.NaN

    input_string = gh.convert_to_str_unicode(input_string)

    measure = sm.DelimiterTokenizer(delim_set=[d])

    return measure.tokenize(input_string)

def tok_wspace(input_string):
    """
    This function splits the input string into a list of tokens
    (based on the white space).

    Args:
        input_string (string): Input string that should be tokenized.

    Returns:
        A list of tokens, if the input string is not NaN ,
        else returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.tok_wspace('data science')
        ['data', 'science']
        >>> em.tok_wspace('data         science')
        ['data', 'science']
        >>> em.tok_wspace(None)
        nan


    """
    if pd.isnull(input_string):
        return np.NaN

    # input_string = remove_non_ascii(input_string)
    input_string = gh.convert_to_str_unicode(input_string)

    measure = sm.WhitespaceTokenizer()
    return measure.tokenize(input_string)

def tok_alphabetic(input_string):
    """
    This function returns a list of tokens that are maximal sequences of
    consecutive alphabetical characters.

    Args:
        input_string (string): Input string that should be tokenized.

    Returns:
        A list of tokens, if the input string is not NaN ,
        else returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.tok_alphabetic('data99science, data#integration.')
        ['data', 'science', 'data', 'integration']
        >>> em.tok_alphabetic('99')
        []
        >>> em.tok_alphabetic(None)
        nan


    """
    if pd.isnull(input_string):
        return np.NaN
    measure = sm.AlphabeticTokenizer()

    input_string = gh.convert_to_str_unicode(input_string)

    return measure.tokenize(input_string)


def tok_alphanumeric(input_string):
    """
    This function returns a list of tokens that are maximal sequences of
    consecutive alphanumeric characters.

    Args:
        input_string (string): Input string that should be tokenized.

    Returns:
        A list of tokens, if the input string is not NaN ,
        else returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.tok_alphanumeric('data9,(science), data9#.(integration).88')
        ['data9', 'science', 'data9', 'integration', '88']
        >>> em.tok_alphanumeric('#.$')
        []
        >>> em.tok_alphanumeric(None)
        nan

    """
    if pd.isnull(input_string):
      return np.NaN

    input_string = gh.convert_to_str_unicode(input_string)

    measure = sm.AlphanumericTokenizer()
    return measure.tokenize(input_string)
