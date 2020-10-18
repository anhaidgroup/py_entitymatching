# coding=utf-8
"""
This module contains similarity functions supported by py_entitymatching
"""

import pandas as pd
import numpy as np
import six

import py_stringmatching as sm
import py_entitymatching.utils.generic_helper as gh

# These are the sim. function names
sim_function_names = ['affine',
                'hamming_dist', 'hamming_sim',
                'lev_dist', 'lev_sim',
                'jaro',
                'jaro_winkler',
                'needleman_wunsch',
                'smith_waterman',
                'overlap_coeff', 'jaccard', 'dice',
                'monge_elkan', 'cosine',
                'exact_match', 'rel_diff', 'abs_norm'
                      ]

# abbreviations for sim. functions
abbreviations = ['aff',
       'ham_dist', 'ham_sim',
       'lev_dist', 'lev_sim',
       'jar',
       'jwn',
       'nmw',
       'swn',
       'ovrlp', 'jac', 'dice',
       'mel', 'cos',
       'exm', 'rdf', 'anm']

# global function names
_global_sim_fns = pd.DataFrame({'function_name': sim_function_names,
                                'short_name': abbreviations})


def get_sim_funs_for_blocking():
    """
    This function returns the similarity functions that can be used for
    blocking purposes.

    Returns:

        A Python dictionary containing the similarity functions.


        Specifically, the key is the similarity function name and the value
        is the actual similary function.

    Examples:
        >>> import py_entitymatching as em
        >>> block_s = em.get_sim_funs_for_blocking()

    """
    return get_sim_funs()


def get_sim_funs_for_matching():
    """
    This function returns the similarity functions that can be used for
    matching purposes.

    Returns:

        A Python dictionary containing the similarity functions.

        Specifically, the key is the similarity function name and the value
        is the actual similarity function.

    Examples:
        >>> import py_entitymatching as em
        >>> match_s = em.get_sim_funs_for_matching()


    """
    return get_sim_funs()


def get_sim_funs():
    """
    This function returns all the similarity functions supported by py_entitymatching.

    """
    # Get all the functions
    functions = [affine,
           hamming_dist, hamming_sim,
           lev_dist, lev_sim,
           jaro,
           jaro_winkler,
           needleman_wunsch,
           smith_waterman,
           overlap_coeff, jaccard, dice,
           monge_elkan, cosine,
           exact_match, rel_diff, abs_norm]
    # Return a dictionary with the functions names as the key and the actual
    # functions as values.
    return dict(zip(sim_function_names, functions))


## String based similarity measures
def affine(s1, s2):
    """
    This function computes the affine measure between the two input strings.

    Args:
        s1,s2 (string ): The input strings for which the similarity measure
            should be computed.

    Returns:
        The affine measure if both the strings are not missing (i.e NaN or
        None), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.affine('dva', 'deeva')
        1.5
        >>> em.affine(None, 'deeva')
        nan
    """
    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.Affine()

    # if not isinstance(s1, six.string_types):
    #     s1 = six.u(str(s1))
    #
    # if isinstance(s1, bytes):
    #     s1 = s1.decode('utf-8', 'ignore')
    #
    # if not isinstance(s2, six.string_types):
    #     s2 = six.u(str(s2))
    #
    # if isinstance(s2, bytes):
    #     s2 = s2.decode('utf-8', 'ignore')

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity
    return measure.get_raw_score(s1, s2)

def hamming_dist(s1, s2):
    """
    This function computes the Hamming distance between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Hamming distance if both the strings are not missing (i.e NaN),
        else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.hamming_dist('alex', 'john')
        4
        >>> em.hamming_dist(None, 'john')
        nan


    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.HammingDistance()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)


    # Call the function to compute the distance
    return measure.get_raw_score(s1, s2)


def hamming_sim(s1, s2):
    """
    This function computes the Hamming similarity between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Hamming similarity if both the strings are not missing (i.e NaN),
        else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.hamming_sim('alex', 'alxe')
        0.5
        >>> em.hamming_sim(None, 'alex')
        nan

    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.HammingDistance()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity score.
    return measure.get_sim_score(s1, s2)


def lev_dist(s1, s2):
    """
    This function computes the Levenshtein distance between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Levenshtein distance if both the strings are not missing (i.e NaN),
        else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.lev_dist('alex', 'alxe')
        2
        >>> em.lev_dist(None, 'alex')
        nan

    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.Levenshtein()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the distance measure.
    return measure.get_raw_score(s1, s2)


def lev_sim(s1, s2):
    """
    This function computes the Levenshtein similarity between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Levenshtein similarity if both the strings are not missing (i.e
        NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.lev_sim('alex', 'alxe')
        0.5
        >>> em.lev_dist(None, 'alex')
        nan

    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.Levenshtein()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity measure
    return measure.get_sim_score(s1, s2)


def jaro(s1, s2):
    """
    This function computes the Jaro measure between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Jaro measure if both the strings are not missing (i.e NaN),
        else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.jaro('MARTHA', 'MARHTA')
        0.9444444444444445
        >>> em.jaro(None, 'MARTHA')
        nan
    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.Jaro()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity measure
    return measure.get_raw_score(s1, s2)


def jaro_winkler(s1, s2):
    """
    This function computes the Jaro Winkler measure between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Jaro Winkler measure if both the strings are not missing (i.e NaN),
        else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.jaro_winkler('MARTHA', 'MARHTA')
        0.9611111111111111
        >>> >>> em.jaro_winkler('MARTHA', None)
        nan

    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.JaroWinkler()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity measure
    return measure.get_raw_score(s1, s2)


def needleman_wunsch(s1, s2):
    """
    This function computes the Needleman-Wunsch measure between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Needleman-Wunsch measure if both the strings are not missing (i.e
        NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.needleman_wunsch('dva', 'deeva')
        1.0
        >>> em.needleman_wunsch('dva', None)
        nan


    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.NeedlemanWunsch()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity measure
    return measure.get_raw_score(s1, s2)


def smith_waterman(s1, s2):
    """
    This function computes the Smith-Waterman measure between the two input
    strings.

    Args:
        s1,s2 (string): The input strings for which the similarity measure should
            be computed.

    Returns:
        The Smith-Waterman measure if both the strings are not missing (i.e
        NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.smith_waterman('cat', 'hat')
        2.0
        >>> em.smith_waterman('cat', None)
        nan
    """

    if s1 is None or s2 is None:
        return np.NaN
    if pd.isnull(s1) or pd.isnull(s2):
        return np.NaN

    # Create the similarity measure object
    measure = sm.SmithWaterman()

    s1 = gh.convert_to_str_unicode(s1)
    s2 = gh.convert_to_str_unicode(s2)

    # Call the function to compute the similarity measure
    return measure.get_raw_score(s1, s2)


# Token-based measures
def jaccard(arr1, arr2):
    """
    This function computes the Jaccard measure between the two input
    lists/sets.

    Args:
        arr1,arr2 (list or set): The input list or sets for which the Jaccard
            measure should be computed.

    Returns:
        The Jaccard measure if both the lists/set are not None and do not have
        any missing tokens (i.e NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.jaccard(['data', 'science'], ['data'])
        0.5
        >>> em.jaccard(['data', 'science'], None)
        nan
    """

    if arr1 is None or arr2 is None:
        return np.NaN
    if not isinstance(arr1, list):
        arr1 = [arr1]
    if any(pd.isnull(arr1)):
        return np.NaN
    if not isinstance(arr2, list):
        arr2 = [arr2]
    if any(pd.isnull(arr2)):
        return np.NaN
    # Create jaccard measure object
    measure = sm.Jaccard()
    # Call a function to compute a similarity score
    return measure.get_raw_score(arr1, arr2)


def cosine(arr1, arr2):
    """
    This function computes the cosine measure between the two input
    lists/sets.

    Args:
        arr1,arr2 (list or set): The input list or sets for which the cosine
         measure should be computed.

    Returns:
        The cosine measure if both the lists/set are not None and do not have
        any missing tokens (i.e NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.cosine(['data', 'science'], ['data'])
        0.7071067811865475
        >>> em.cosine(['data', 'science'], None)
        nan

    """

    if arr1 is None or arr2 is None:
        return np.NaN
    if not isinstance(arr1, list):
        arr1 = [arr1]
    if any(pd.isnull(arr1)):
        return np.NaN
    if not isinstance(arr2, list):
        arr2 = [arr2]
    if any(pd.isnull(arr2)):
        return np.NaN
    # Create cosine measure object
    measure = sm.Cosine()
    # Call the function to compute the cosine measure.
    return measure.get_raw_score(arr1, arr2)


def overlap_coeff(arr1, arr2):
    """
    This function computes the overlap coefficient between the two input
    lists/sets.

    Args:
        arr1,arr2 (list or set): The input lists or sets for which the overlap
            coefficient should be computed.

    Returns:
        The overlap coefficient if both the lists/sets are not None and do not
        have any missing tokens (i.e NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.overlap_coeff(['data', 'science'], ['data'])
        1.0
        >>> em.overlap_coeff(['data', 'science'], None)
        nan

    """

    if arr1 is None or arr2 is None:
        return np.NaN
    if not isinstance(arr1, list):
        arr1 = [arr1]
    if any(pd.isnull(arr1)):
        return np.NaN
    if not isinstance(arr2, list):
        arr2 = [arr2]
    if any(pd.isnull(arr2)):
        return np.NaN
    # Create overlap coefficient measure object
    measure = sm.OverlapCoefficient()
    # Call the function to return the overlap coefficient
    return measure.get_raw_score(arr1, arr2)

def dice(arr1, arr2):
    """
    This function computes the Dice score between the two input
    lists/sets.

    Args:
        arr1,arr2 (list or set): The input list or sets for which the Dice
            score should be computed.

    Returns:
        The Dice score if both the lists/set are not None and do not
        have any missing tokens (i.e NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.dice(['data', 'science'], ['data'])
        0.6666666666666666
        >>> em.dice(['data', 'science'], None)
        nan

    """

    if arr1 is None or arr2 is None:
        return np.NaN
    if not isinstance(arr1, list):
        arr1 = [arr1]
    if any(pd.isnull(arr1)):
        return np.NaN
    if not isinstance(arr2, list):
        arr2 = [arr2]
    if any(pd.isnull(arr2)):
        return np.NaN

    # Create Dice object
    measure = sm.Dice()
    # Call the function to return the dice score
    return measure.get_raw_score(arr1, arr2)

# Hybrid measure
def monge_elkan(arr1, arr2):
    """
    This function computes the Monge-Elkan measure between the two input
    lists/sets. Specifically, this function uses Jaro-Winkler measure as the
    secondary function to compute the similarity score.

    Args:
        arr1,arr2 (list or set): The input list or sets for which the
            Monge-Elkan measure should be computed.

    Returns:
        The Monge-Elkan measure if both the lists/set are not None and do not
        have any missing tokens (i.e NaN), else  returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.monge_elkan(['Niall'], ['Neal'])
        0.8049999999999999
        >>> em.monge_elkan(['Niall'], None)
        nan
    """

    if arr1 is None or arr2 is None:
        return np.NaN
    if not isinstance(arr1, list):
        arr1 = [arr1]
    if any(pd.isnull(arr1)):
        return np.NaN
    if not isinstance(arr2, list):
        arr2 = [arr2]
    if any(pd.isnull(arr2)):
        return np.NaN
    # Create Monge-Elkan measure object
    measure = sm.MongeElkan()
    # Call the function to compute the Monge-Elkan measure
    return measure.get_raw_score(arr1, arr2)




# boolean/string/numeric similarity measure
def exact_match(d1, d2):
    """
    This function check if two objects are match exactly. Typically the
    objects are string, boolean and ints.

    Args:
        d1,d2 (str, boolean, int): The input objects which should checked
            whether they match exactly.

    Returns:
        A value of 1 is returned if they match exactly,
        else returns 0. Further if one of the objects is NaN or None,
        it returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.exact_match('Niall', 'Neal')
        0
        >>> em.exact_match('Niall', 'Niall')
        1
        >>> em.exact_match(10, 10)
        1
        >>> em.exact_match(10, 20)
        0
        >>> em.exact_match(True, True)
        1
        >>> em.exact_match(False, True)
        0
        >>> em.exact_match(10, None)
        nan
    """
    if d1 is None or d2 is None:
        return np.NaN
    if pd.isnull(d1) or pd.isnull(d2):
        return np.NaN
    # Check if they match exactly
    if d1 == d2:
        return 1
    else:
        return 0


# numeric similarity measure
def rel_diff(d1, d2):
    """
    This function computes the relative difference between two numbers

    Args:
        d1,d2 (float): The input numbers for which the relative difference
         must be computed.

    Returns:
        A float value of relative difference between the input numbers (if
        they are valid). Further if one of the input objects is NaN or None,
        it returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.rel_diff(100, 200)
        0.6666666666666666
        >>> em.rel_diff(100, 100)
        0.0
        >>> em.rel_diff(100, None)
        nan
    """

    if d1 is None or d2 is None:
        return np.NaN
    if pd.isnull(d1) or pd.isnull(d2):
        return np.NaN
    try:
        d1 = float(d1)
        d2 = float(d2)
    except ValueError:
        return np.NaN
    if d1 == 0.0 and d2 == 0.0:
        return 0
    else:
        # Compute the relative difference between two numbers
        # ref: https://en.wikipedia.org/wiki/Relative_change_and_difference
        x = (2*abs(d1 - d2)) / (d1 + d2)
        return x


# compute absolute norm similarity
def abs_norm(d1, d2):
    """
    This function computes the absolute norm similarity between two numbers

    Args:
        d1,d2 (float): Input numbers for which the absolute norm must
            be computed.

    Returns:
        A float value of absolute norm between the input numbers (if
        they are valid). Further if one of the input objects is NaN or None,
        it returns NaN.

    Examples:
        >>> import py_entitymatching as em
        >>> em.abs_norm(100, 200)
        0.5
        >>> em.abs_norm(100, 100)
        1.0
        >>> em.abs_norm(100, None)
        nan

    """

    if d1 is None or d2 is None:
        return np.NaN
    if pd.isnull(d1) or pd.isnull(d2):
        return np.NaN
    try:
        d1 = float(d1)
        d2 = float(d2)
    except ValueError:
        return np.NaN
    if d1 == 0.0 and d2 == 0.0:
        return 0
    else:
        # Compute absolute norm similarity between two numbers.
        x = (abs(d1 - d2) / max(abs(d1), abs(d2)))
        if x <= 10e-5:
            x = 0
        return 1.0 - x
