# coding=utf-8
from __future__ import division
from __future__ import unicode_literals

import collections
import math
import re
import unicodedata

import numpy as np

from magellan.externals.py_stringsimjoin.externals.py_stringmatching import utils
# noinspection PyProtectedMember,PyProtectedMember
from .compat import _range, _unicode


def sim_ident(s1, s2):
    return int(s1 == s2)


# ---------------------- sequence based similarity measures  ----------------------


def affine(string1, string2, gap_start=1, gap_continuation=0.5, sim_score=sim_ident):
    """
    Computes the Affine gap score between two strings.

    The Affine gap measure is an extension of the Needleman-Wunsch measure that handles the longer gaps more
    gracefully.

    For more information refer to string matching chapter in the DI book.

    Args:
        string1,string2 (str) : Input strings

        gap_start (float): Cost for the gap at the start (defaults to 1)

        gap_continuation (float) : Cost for the gap continuation (defaults to 0.5)

        sim_score (function) : Function computing similarity score between two chars, represented as strings
            (defaults to identity).

    Returns:
        Affine gap score (float)

    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.

    Examples:
        >>> affine('dva', 'deeva')
        1.5
        >>> affine('dva', 'deeve', gap_start=2, gap_continuation=0.5)
        -0.5
        >>> affine('AAAGAATTCA', 'AAATCA', gap_continuation=0.2, sim_score=lambda s1, s2: (int(1 if s1 == s2 else 0)))
        4.4
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.tok_check_for_string_input(string1, string2)
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(string1, string2):
        return 0

    gap_start = -gap_start
    gap_continuation = -gap_continuation
    m = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.float)
    x = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.float)
    y = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.float)
    # DP initialization
    for i in _range(1, len(string1) + 1):
        m[i][0] = -float("inf")
        x[i][0] = gap_start + (i - 1) * gap_continuation
        y[i][0] = -float("inf")
    # DP initialization
    for j in _range(1, len(string2) + 1):
        m[0][j] = -float("inf")
        x[0][j] = -float("inf")
        y[0][j] = gap_start + (j - 1) * gap_continuation
    # affine gap calculation using DP
    for i in _range(1, len(string1) + 1):
        for j in _range(1, len(string2) + 1):
            # best score between x_1....x_i and y_1....y_j given that x_i is aligned to y_j
            m[i][j] = sim_score(string1[i - 1], string2[j - 1]) + max(m[i - 1][j - 1], x[i - 1][j - 1], y[i - 1][j - 1])
            # the best score given that x_i is aligned to a gap
            x[i][j] = max(gap_start + m[i - 1][j], gap_continuation + x[i - 1][j])
            # the best score given that y_j is aligned to a gap
            y[i][j] = max(gap_start + m[i][j - 1], gap_continuation + y[i][j - 1])
    return max(m[len(string1)][len(string2)], x[len(string1)][len(string2)], y[len(string1)][len(string2)])


def bag_distance(string1, string2):
    """
    Computes the bag distance between two strings.
    For two strings X and Y, the Bag distance is:
    :math:`max( |multiset(string1)-multiset(string2)|, |multiset(string2)-multiset(string1)| )`
    Args:
        string1,string2 (str): Input strings
    Returns:
        Bag distance (int)
    Raises:
        TypeError : If the inputs are not strings
    Examples:
        >>> bag_distance('cat', 'hat')
        1
        >>> bag_distance('Niall', 'Neil')
        2
        >>> bag_distance('aluminum', 'Catalan')
        5
        >>> bag_distance('ATCG', 'TAGC')
        0
        >>> bag_distance('abcde', 'xyz')
        5
    References:
        * http://www.icmlc.org/icmlc2011/018_icmlc2011.pdf
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)
    if utils.sim_check_for_exact_match(string1, string2):
        return 0

    len_str1 = len(string1)
    len_str2 = len(string2)

    if len_str1 == 0:
        return len_str2

    if len_str2 == 0:
        return len_str1

    bag1 = collections.Counter(string1)
    bag2 = collections.Counter(string2)

    size1 = sum((bag1 - bag2).values())
    size2 = sum((bag2 - bag1).values())
    # returning the max of difference of sets
    return max(size1, size2)


def editex(string1, string2, match_cost=0, group_cost=1, mismatch_cost=2, local=False):
    """
    Computes the editex distance between two strings.

    As described on pages 3 & 4 of
    Zobel, Justin and Philip Dart. 1996. Phonetic string matching: Lessons from
    information retrieval. In: Proceedings of the ACM-SIGIR Conference on
    Research and Development in Information Retrieval, Zurich, Switzerland.
    166–173. http://goanna.cs.rmit.edu.au/~jz/fulltext/sigir96.pdf

    The local variant is based on
    Ring, Nicholas and Alexandra L. Uitdenbogerd. 2009. Finding ‘Lucy in
    Disguise’: The Misheard Lyric Matching Problem. In: Proceedings of the 5th
    Asia Information Retrieval Symposium, Sapporo, Japan. 157-167.
    http://www.seg.rmit.edu.au/research/download.php?manuscript=404

    Args:
        string1,string2 (str): Input strings
        match_cost (int): Weight to give the correct char match, default=0
        group_cost (int): Weight to give if the chars are in the same editex group, default=1
        mismatch_cost (int): Weight to give the incorrect char match, default=2
        local (boolean): Local variant on/off, default=False

    Returns:
        Editex distance (int)

    Raises:
        TypeError : If the inputs are not strings

    Examples:
        >>> editex('cat', 'hat')
        2
        >>> editex('Niall', 'Neil')
        2
        >>> editex('aluminum', 'Catalan')
        12
        >>> editex('ATCG', 'TAGC')
        6

    References:
        * Abydos Library - https://github.com/chrislit/abydos/blob/master/abydos/distance.py
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)
    if utils.sim_check_for_exact_match(string1, string2):
        return 0
    # convert both the strings to NFKD normalized unicode
    string1 = unicodedata.normalize('NFKD', _unicode(string1.upper()))
    string2 = unicodedata.normalize('NFKD', _unicode(string2.upper()))
    # convert ß to SS (for Python2)
    string1 = string1.replace('ß', 'SS')
    string2 = string2.replace('ß', 'SS')

    if string1 == string2:
        return 0
    if len(string1) == 0:
        return len(string2) * mismatch_cost
    if len(string2) == 0:
        return len(string1) * mismatch_cost

    d_mat = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.int)
    len1 = len(string1)
    len2 = len(string2)
    string1 = ' ' + string1
    string2 = ' ' + string2
    editex_helper = utils.Editex(match_cost, mismatch_cost, group_cost)
    if not local:
        for i in _range(1, len1 + 1):
            d_mat[i, 0] = d_mat[i - 1, 0] + editex_helper.d_cost(string1[i - 1], string1[i])
    for j in _range(1, len2 + 1):
        d_mat[0, j] = d_mat[0, j - 1] + editex_helper.d_cost(string2[j - 1], string2[j])

    for i in _range(1, len1 + 1):
        for j in _range(1, len2 + 1):
            d_mat[i, j] = min(d_mat[i - 1, j] + editex_helper.d_cost(string1[i - 1], string1[i]),
                              d_mat[i, j - 1] + editex_helper.d_cost(string2[j - 1], string2[j]),
                              d_mat[i - 1, j - 1] + editex_helper.r_cost(string1[i], string2[j]))

    return d_mat[len1, len2]


# jaro
# noinspection PyUnboundLocalVariable,PyUnboundLocalVariable,PyUnboundLocalVariable,PyUnboundLocalVariable
def jaro(string1, string2):
    """
    Computes the Jaro measure between two strings.

    The Jaro measure is a type of edit distance, This was developed mainly to compare short strings,
    such as first and last names.


    Args:
        string1,string2 (str): Input strings

    Returns:
        Jaro measure (float)


    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.


    Examples:
        >>> jaro('MARTHA', 'MARHTA')
        0.9444444444444445
        >>> jaro('DWAYNE', 'DUANE')
        0.8222222222222223
        >>> jaro('DIXON', 'DICKSONX')
        0.7666666666666666


    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.tok_check_for_string_input(string1, string2)
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(string1, string2):
        return 0

    len_s1 = len(string1)
    len_s2 = len(string2)

    max_len = max(len_s1, len_s2)
    search_range = (max_len // 2) - 1
    if search_range < 0:
        search_range = 0

    flags_s1 = [False] * len_s1
    flags_s2 = [False] * len_s2

    common_chars = 0
    for i, ch_s1 in enumerate(string1):
        low = i - search_range if i > search_range else 0
        hi = i + search_range if i + search_range < len_s2 else len_s2 - 1
        for j in _range(low, hi + 1):
            if not flags_s2[j] and string2[j] == ch_s1:
                flags_s1[i] = flags_s2[j] = True
                common_chars += 1
                break
    if not common_chars:
        return 0
    k = trans_count = 0
    for i, f_s1 in enumerate(flags_s1):
        if f_s1:
            for j in _range(k, len_s2):
                if flags_s2[j]:
                    k = j + 1
                    break
            if string1[i] != string2[j]:
                trans_count += 1
    trans_count /= 2
    common_chars = float(common_chars)
    weight = ((common_chars / len_s1 + common_chars / len_s2 +
               (common_chars - trans_count) / common_chars)) / 3
    return weight


# jaro-winkler
def jaro_winkler(string1, string2, prefix_weight=0.1):
    """
    Computes the Jaro-Winkler measure between two strings.

    The Jaro-Winkler measure is designed to capture cases where two strings have a low Jaro score, but share a prefix
    and thus are likely to match.


    Args:
        string1,string2 (str): Input strings

        prefix_weight (float): Weight to give the prefix (defaults to 0.1)

    Returns:
        Jaro-Winkler measure (float)

    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.


    Examples:
        >>> jaro_winkler('MARTHA', 'MARHTA')
        0.9611111111111111
        >>> jaro_winkler('DWAYNE', 'DUANE')
        0.84
        >>> jaro_winkler('DIXON', 'DICKSONX')
        0.8133333333333332

    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.tok_check_for_string_input(string1, string2)
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(string1, string2):
        return 0

    jw_score = jaro(string1, string2)
    min_len = min(len(string1), len(string2))
    # prefix length can be at max 4
    j = min(min_len, 4)
    i = 0
    while i < j and string1[i] == string2[i] and string1[i]:
        i += 1
    if i:
        jw_score += i * prefix_weight * (1 - jw_score)
    return jw_score


def hamming_distance(string1, string2):
    """
    Computes the Hamming distance between two strings.

    The Hamming distance between two strings of equal length is the number of positions at which the corresponding
    symbols are different. In another way, it measures the minimum number of substitutions required to change
    one string into the other, or the minimum number of errors that could have transformed one string into the other.


    Args:
        string1,string2 (str): Input strings

    Returns:
        Hamming distance (int)

    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.
        ValueError : If the input strings are not of same length


    Examples:
        >>> hamming_distance('', '')
        0
        >>> hamming_distance('alex', 'john')
        4
        >>> hamming_distance(' ', 'a')
        0
        >>> hamming_distance('JOHN', 'john')
        4
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.tok_check_for_string_input(string1, string2)
    # for Hamming Distance string length should be same
    utils.sim_check_for_same_len(string1, string2)
    # sum all the mismatch characters at the corresponding index of
    # input strings
    return sum(bool(ord(c1) - ord(c2)) for c1, c2 in zip(string1, string2))


def levenshtein(string1, string2):
    """
    Computes the Levenshtein distance between two strings.

    Levenshtein distance computes the minimum cost of transforming one string into the other. Transforming a string
    is carried out using a sequence of the following operators: delete a character, insert a character, and
    substitute one character for another.

    Args:
        string1,string2 (str): Input strings

    Returns:
        Levenshtein distance (int)

    Raises:
        TypeError : If the inputs are not strings

    Examples:
        >>> levenshtein('a', '')
        1
        >>> levenshtein('example', 'samples')
        3
        >>> levenshtein('levenshtein', 'frankenstein')
        6



    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)
    if utils.sim_check_for_exact_match(string1, string2):
        return 0.0

    ins_cost, del_cost, sub_cost, trans_cost = (1, 1, 1, 1)

    len_str1 = len(string1)
    len_str2 = len(string2)

    if len_str1 == 0:
        return len_str2 * ins_cost

    if len_str2 == 0:
        return len_str1 * del_cost

    d_mat = np.zeros((len_str1 + 1, len_str2 + 1), dtype=np.int)

    for i in _range(len_str1 + 1):
        d_mat[i, 0] = i * del_cost

    for j in _range(len_str2 + 1):
        d_mat[0, j] = j * ins_cost

    for i in _range(len_str1):
        for j in _range(len_str2):
            d_mat[i + 1, j + 1] = min(
                d_mat[i + 1, j] + ins_cost,
                d_mat[i, j + 1] + del_cost,
                d_mat[i, j] + (sub_cost if string1[i] != string2[j] else 0)
            )

    return d_mat[len_str1, len_str2]


def needleman_wunsch(string1, string2, gap_cost=1.0, sim_score=sim_ident):
    """
    Computes the Needleman-Wunsch measure between two strings.

    The Needleman-Wunsch generalizes the Levenshtein distance and considers global alignment between two strings.
    Specifically, it is computed by assigning a score to each alignment between two input strings and choosing the
    score of the best alignment, that is, the maximal score.

    An alignment between two strings is a set of correspondences between the characters of between them, allowing for
    gaps.

    Args:
        string1,string2 (str) : Input strings

        gap_cost (float) : Cost of gap (defaults to 1.0)

        sim_score (function) : Similarity function to give a score for the correspondence between characters. Defaults
            to an identity function, where if two characters are same it returns 1.0 else returns 0.


    Returns:
        Needleman-Wunsch measure (float)


    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.

    Examples:
        >>> needleman_wunsch('dva', 'deeva')
        1.0
        >>> needleman_wunsch('dva', 'deeve', 0.0)
        2.0
        >>> needleman_wunsch('dva', 'deeve', 1.0, sim_score=lambda s1, s2 : (2.0 if s1 == s2 else -1.0))
        1.0
        >>> needleman_wunsch('GCATGCUA', 'GATTACA', gap_cost=0.5, sim_score=lambda s1, s2 : (1.0 if s1 == s2 else -1.0))
        2.5
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)

    dist_mat = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.float)
    # DP initialization
    for i in _range(len(string1) + 1):
        dist_mat[i, 0] = -(i * gap_cost)
    # DP initialization
    for j in _range(len(string2) + 1):
        dist_mat[0, j] = -(j * gap_cost)
    # Needleman-Wunsch DP calculation
    for i in _range(1, len(string1) + 1):
        for j in _range(1, len(string2) + 1):
            match = dist_mat[i - 1, j - 1] + sim_score(string1[i - 1], string2[j - 1])
            delete = dist_mat[i - 1, j] - gap_cost
            insert = dist_mat[i, j - 1] - gap_cost
            dist_mat[i, j] = max(match, delete, insert)
    return dist_mat[dist_mat.shape[0] - 1, dist_mat.shape[1] - 1]


def smith_waterman(string1, string2, gap_cost=1.0, sim_score=sim_ident):
    """
    Computes the Smith-Waterman measure between two strings.

    The Smith–Waterman algorithm performs local sequence alignment; that is, for determining similar regions
    between two strings. Instead of looking at the total sequence, the Smith–Waterman algorithm compares segments of
    all possible lengths and optimizes the similarity measure.


    Args:
        string1,string2 (str) : Input strings

        gap_cost (float) : Cost of gap (defaults to 1.0)

        sim_score (function) : Similarity function to give a score for the correspondence between characters. Defaults
            to an identity function, where if two characters are same it returns 1 else returns 0.

    Returns:
        Smith-Waterman measure (float)

    Raises:
        TypeError : If the inputs are not strings or if one of the inputs is None.

    Examples:
        >>> smith_waterman('cat', 'hat')
        2.0
        >>> smith_waterman('dva', 'deeve', 2.2)
        1.0
        >>> smith_waterman('dva', 'deeve', 1, sim_score=lambda s1, s2 : (2 if s1 == s2 else -1))
        2.0
        >>> smith_waterman('GCATAGCU', 'GATTACA', gap_cost=1.4, sim_score=lambda s1, s2 : (1.5 if s1 == s2 else 0.5))
        6.5
    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)

    dist_mat = np.zeros((len(string1) + 1, len(string2) + 1), dtype=np.float)
    max_value = 0
    # Smith Waterman DP calculations
    for i in _range(1, len(string1) + 1):
        for j in _range(1, len(string2) + 1):
            match = dist_mat[i - 1, j - 1] + sim_score(string1[i - 1], string2[j - 1])
            delete = dist_mat[i - 1, j] - gap_cost
            insert = dist_mat[i, j - 1] - gap_cost
            dist_mat[i, j] = max(0, match, delete, insert)
            max_value = max(max_value, dist_mat[i, j])
    return max_value


def soundex(string1, string2):
    """
    Computes the Soundex phonetic similarity between two strings.

    Phonetic measure such as soundex match string based on their sound. These
    measures have been especially effective in matching names, since names are
    often spelled in different ways that sound the same. For example, Meyer, Meier,
    and Mire sound the same, as do Smith, Smithe, and Smythe.

    Soundex is used primarily to match surnames. It does not work as well for names
    of East Asian origins, because much of the discriminating power of these names
    resides in the vowel sounds, which the code ignores.

    Args:
        string1,string2 (str): Input strings

    Returns:
        Soundex similarity score (int) is returned

    Raises:
        TypeError : If the inputs are not strings

    Examples:
        >>> soundex('Robert', 'Rupert')
        1
        >>> soundex('Sue', 's')
        1
        >>> soundex('Gough', 'Goff')
        0
        >>> soundex('a,,li', 'ali')
        1

    """
    # input validations
    utils.sim_check_for_none(string1, string2)
    utils.sim_check_for_string_inputs(string1, string2)
    if utils.sim_check_for_exact_match(string1, string2):
        return 1
    utils.sim_check_for_zero_len(string1, string2)
    string1, string2 = string1.upper(), string2.upper()
    firstLetter1, firstLetter2 = string1[0], string2[0]
    string1, string2 = string1[1:], string2[1:]
    # remove occurrences of vowels, 'y', 'w' and 'h'
    string1 = re.sub('[AEIOUYWH]', '', string1)
    string2 = re.sub('[AEIOUYWH]', '', string2)

    # replace (B,F,P,V)->1 (C,G,J,K,Q,S,X,Z)->2 (D,T)->3 (L)->4 (M,N)->5 (R)->6
    string1 = re.sub('[BFPV]', '1', string1)
    string1 = re.sub('[CGJKQSXZ]', '2', string1)
    string1 = re.sub('[DT]', '3', string1)
    string1 = re.sub('[L]', '4', string1)
    string1 = re.sub('[MN]', '5', string1)
    string1 = re.sub('[R]', '6', string1)

    string2 = re.sub('[BFPV]', '1', string2)
    string2 = re.sub('[CGJKQSXZ]', '2', string2)
    string2 = re.sub('[DT]', '3', string2)
    string2 = re.sub('[L]', '4', string2)
    string2 = re.sub('[MN]', '5', string2)
    string2 = re.sub('[R]', '6', string2)

    # remove all chars but digits
    string1 = re.sub("\D", "", string1)
    string2 = re.sub("\D", "", string2)

    string1 = firstLetter1 + string1[:3]
    string2 = firstLetter2 + string2[:3]
    return 1 if string1 == string2 else 0


# ---------------------- token based similarity measures  ----------------------

# ---------------------- set based similarity measures  ----------------------
def cosine(set1, set2):
    """
    Computes the cosine similarity between two sets.

    For two sets X and Y, the cosine similarity is:

    :math:`cosine(X, Y) = \\frac{|X \\cap Y|}{\\sqrt{|X| \\cdot |Y|}}`


    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.

    Returns:
        Cosine similarity (float)

    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.

    Examples:
     >>> cosine(['data', 'science'], ['data'])
     0.7071067811865475
     >>> cosine(['data', 'data', 'science'], ['data', 'management'])
     0.4999999999999999
     >>> cosine([], ['data'])
     0.0

    References:
        * String similarity joins: An Experimental Evaluation (VLDB 2014)
        * Project flamingo : Mike carey, Vernica
    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    return float(len(set1 & set2)) / (math.sqrt(float(len(set1))) * math.sqrt(float(len(set2))))


def generalized_jaccard(set1, set2, sim_func=jaro, threshold=0.5):
    """
    Computes the Generalized Jaccard measure between two sets.

    This similarity measure is softened version of the Jaccard measure. The Jaccard measure is
    promising candidate for tokens which exactly match across the sets. However, in practice tokens
    are often misspelled, such as energy vs. eneryg. THe generalized Jaccard measure will enable
    matching in such cases.

    Args:
        set1,set2 (set or list): Input sets (or lists) of strings. Input lists are converted to sets.
        sim_func (func): similarity function. This should return a similarity score between two strings in set (optional),
            default is jaro similarity measure
        threshold (float): Threshold value (defaults to 0.5). If the similarity of a token pair exceeds the threshold,
        then the token pair is considered a match.

    Returns:
        Generalized Jaccard similarity (float)

    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.
        ValueError : If the similarity measure doesn't return values in the range [0.1]

    Examples:
        >>> generalized_jaccard(['data', 'science'], ['data'])
        0.5
        >>> generalized_jaccard(['data', 'management'], ['data', 'data', 'science'])
        0.3333333333333333
        >>> generalized_jaccard(['Niall'], ['Neal', 'Njall'])
        0.43333333333333335
        >>> generalized_jaccard(['Comp', 'Sci.', 'and', 'Engr', 'Dept.,', 'Universty', 'of', 'Cal,', 'San', 'Deigo'],
        ['Department', 'of', 'Computer', 'Science,', 'Univ.', 'Calif.,', 'San', 'Diego'],
        sim_func=jaro_winkler, threshold=0.8)
        0.45810185185185187
    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    set1_x = set()
    set2_y = set()
    match_score = 0.0
    match_count = 0
    list_matches = []
    for element in set1:
        for item in set2:
            score = sim_func(element, item)
            if score > 1 or score < 0:
                raise ValueError('Similarity measure should return value in the range [0,1]')
            if score > threshold:
                list_matches.append(utils.Similarity(element, item, score))
    # sort the score of all the pairs
    list_matches.sort(key=lambda x: x.similarity_score, reverse=True)
    # select score in increasing order of their weightage, do not reselect the same element from either set.
    for element in list_matches:
        if element.first_string not in set1_x and element.second_string not in set2_y:
            set1_x.add(element.first_string)
            set2_y.add(element.second_string)
            match_score += element.similarity_score
            match_count += 1
    return float(match_score) / float(len(set1) + len(set2) - match_count)


def dice(set1, set2):
    """
    Computes the Dice similarity coefficient between two sets.
    The similarity is defined as twice the shared information (intersection) divided by sum of cardinalities.
    For two sets X and Y, the Dice similarity coefficient is:
    :math:`dice(X, Y) = \\frac{2 * |X \\cap Y|}{|X| + |Y|}`
    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.
    Returns:
        Dice similarity coefficient (float)
    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.
    Examples:
        >>> dice(['data', 'science'], ['data'])
        0.6666666666666666
        >>> dice({1, 1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 7, 8})
        0.5454545454545454
        >>> dice(['data', 'management'], ['data', 'data', 'science'])
        0.5
    References:
        * Wikipedia article : https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Dice%27s_coefficient
        * Simmetrics library
    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    return 2.0 * float(len(set1 & set2)) / float(len(set1) + len(set2))


def jaccard(set1, set2):
    """
    Computes the Jaccard measure between two sets.

    The Jaccard measure, also known as the Jaccard similarity coefficient, is a statistic used for comparing
    the similarity and diversity of sample sets. The Jaccard coefficient measures similarity between finite sample
    sets, and is defined as the size of the intersection divided by the size of the union of the sample sets.


    For two sets X and Y, the Jaccard measure is:

    :math:`jaccard(X, Y) = \\frac{|X \\cap Y|}{|X| \\cup |Y|}`


    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.

    Returns:
        Jaccard similarity (float)

    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.

    Examples:
        >>> jaccard(['data', 'science'], ['data'])
        0.5
        >>> jaccard({1, 1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 7, 8})
        0.375
        >>> jaccard(['data', 'management'], ['data', 'data', 'science'])
        0.3333333333333333
    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    return float(len(set1 & set2)) / float(len(set1 | set2))


def overlap_coefficient(set1, set2):
    """
    Computes the overlap coefficient between two sets.

    The overlap coefficient is a similarity measure related to the Jaccard
    measure  that measures the overlap between two sets, and is defined as the size of the intersection divided by
    the smaller of the size of the two sets.

    For two sets X and Y, the overlap coefficient is:

    :math:`overlap\\_coefficient(X, Y) = \\frac{|X \\cap Y|}{\\min(|X|, |Y|)}`

    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.

    Returns:
        Overlap coefficient (float)

    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.

    Examples:
        >>> (overlap_coefficient([], [])
        1.0
        >>> overlap_coefficient([], ['data'])
        0
        >>> overlap_coefficient(['data', 'science'], ['data'])
        1.0

    References:
        * Wikipedia article : https://en.wikipedia.org/wiki/Overlap_coefficient
        * Simmetrics library

    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)

    return float(len(set1 & set2)) / min(len(set1), len(set2))


def tversky_index(set1, set2, alpha=0.5, beta=0.5):
    """
    Computes the Tversky index similarity between two sets.
    The Tversky index is an asymmetric similarity measure on sets that compares a variant to a prototype. The
    Tversky index can be seen as a generalization of Dice's coefficient and Tanimoto coefficient.
    For sets X and Y the Tversky index is a number between 0 and 1 given by:
    :math:`tversky_index(X, Y) = \\frac{|X \\cap Y|}{|X \\cap Y| + \alpha |X-Y| + \beta |Y-X|}`
    where, :math: \alpha, \beta >=0

    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.

    Returns:
        Tversly index similarity (float)

    Raises:
        TypeError : If the inputs are not sets (or lists) or if one of the inputs is None.

    Examples:
        >>> tversky_index(['data', 'science'], ['data'])
        0.6666666666666666
        >>> tversky_index({1, 1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 7, 8})
        0.5454545454545454
        >>> tversky_index({1, 1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 7, 8}, 0.5, 0.5)
        0.5454545454545454
        >>> tversky_index(['data', 'management'], ['data', 'data', 'science'])
        0.5
        >>> tversky_index(['data', 'management'], ['data', 'data', 'science'], beta=0.5)
        0.5
    """
    # input validations
    utils.sim_check_for_none(set1, set2)
    utils.sim_check_for_list_or_set_inputs(set1, set2)
    utils.sim_check_tversky_parameters(alpha, beta)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(set1, set2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(set1, set2):
        return 0
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    intersection = float(len(set1 & set2))
    return 1.0 * intersection / (intersection + (alpha * len(set1 - set2))
                                 + (beta * len(set2 - set1)))


# ---------------------- bag based similarity measures  ----------------------
# noinspection PyArgumentList,PyArgumentList
def tfidf(bag1, bag2, corpus_list=None, dampen=False):
    """
    Compute tfidf measures between two lists given the corpus information.
    This measure employs the notion of TF/IDF score commonly used in information retrieval (IR) to find documents that
    are relevant to keyword queries.
    The intuition underlying the TF/IDF measure is that two strings are similar if they share distinguishing terms.

    Args:
        bag1,bag2 (list): Input lists

        corpus_list (list of lists): Corpus list (default is set to None) of strings. If set to None,
            the input list are considered the only corpus.

        dampen (boolean): Flag to indicate whether 'log' should be applied to tf and idf measure.

    Returns:
        TF-IDF measure between the input lists (float)

    Raises:
        TypeError : If the inputs are not lists or if one of the inputs is None


    Examples:
        >>> tfidf(['a', 'b', 'a'], ['a', 'c'], [['a', 'b', 'a'], ['a', 'c'], ['a']])
        0.17541160386140586
        >>> tfidf(['a', 'b', 'a'], ['a', 'c'], [['a', 'b', 'a'], ['a', 'c'], ['a'], ['b']], True)
        0.11166746710505392
        >>> tfidf(['a', 'b', 'a'], ['a'], [['a', 'b', 'a'], ['a', 'c'], ['a']])
        0.5547001962252291
        >>> tfidf(['a', 'b', 'a'], ['a'], [['x', 'y'], ['w'], ['q']])
        0.0
        >>> tfidf(['a', 'b', 'a'], ['a'], [['x', 'y'], ['w'], ['q']], True)
        0.0
        >>> tfidf(['a', 'b', 'a'], ['a'])
        0.7071067811865475
    """
    # input validations
    utils.sim_check_for_none(bag1, bag2)
    utils.sim_check_for_list_or_set_inputs(bag1, bag2)
    # if the strings match exactly return 1.0
    if utils.sim_check_for_exact_match(bag1, bag2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(bag1, bag2):
        return 0
    # if corpus is not provided treat input string as corpus
    if corpus_list is None:
        corpus_list = [bag1, bag2]
    corpus_size = len(corpus_list)
    # term frequency for input strings
    tf_x, tf_y = collections.Counter(bag1), collections.Counter(bag2)
    # number of documents an element appeared
    element_freq = {}
    # set of unique element
    total_unique_elements = set()
    for document in corpus_list:
        temp_set = set()
        for element in document:
            # adding element only if it is present in one of two input string
            if element in bag1 or element in bag2:
                temp_set.add(element)
                total_unique_elements.add(element)
        # update element document frequency for this document
        for element in temp_set:
            element_freq[element] = element_freq[element] + 1 if element in element_freq else 1
    idf_element, v_x, v_y, v_x_y, v_x_2, v_y_2 = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    # tfidf calculation
    for element in total_unique_elements:
        idf_element = corpus_size * 1.0 / element_freq[element]
        v_x = 0 if element not in tf_x else (math.log(idf_element) * math.log(tf_x[element] + 1)) if dampen else (
            idf_element * tf_x[element])
        v_y = 0 if element not in tf_y else (math.log(idf_element) * math.log(tf_y[element] + 1)) if dampen else (
            idf_element * tf_y[element])
        v_x_y += v_x * v_y
        v_x_2 += v_x * v_x
        v_y_2 += v_y * v_y
    return 0.0 if v_x_y == 0 else v_x_y / (math.sqrt(v_x_2) * math.sqrt(v_y_2))


# hybrid similarity measures
def monge_elkan(bag1, bag2, sim_func=jaro_winkler):
    """
    Compute Monge-Elkan similarity measure between two bags (lists).

    The Monge-Elkan similarity measure is a type of Hybrid similarity measure that combine the benefits of
    sequence-based and set-based methods. This can be effective for domains in which more control is needed
    over the similarity measure. It implicitly uses a secondary similarity measure, such as levenshtein to compute
    over all similarity score.

    Args:
        bag1,bag2 (list): Input lists

        sim_func (function): Secondary similarity function. This is expected to be a sequence-based
            similarity measure (defaults to levenshtein)

    Returns:
        Monge-Elkan similarity score (float)

    Raises:
        TypeError : If the inputs are not lists or if one of the inputs is None


    Examples:
        >>> monge_elkan(['Niall'], ['Neal'])
        0.8049999999999999
        >>> monge_elkan(['Comput.', 'Sci.', 'and', 'Eng.', 'Dept.,', 'University', 'of', 'California,', 'San', 'Diego'], ['Department', 'of', 'Computer', 'Science,', 'Univ.', 'Calif.,', 'San', 'Diego'])
        0.8677218614718616
        >>> monge_elkan(['Comput.', 'Sci.', 'and', 'Eng.', 'Dept.,', 'University', 'of', 'California,', 'San', 'Diego'], ['Department', 'of', 'Computer', 'Science,', 'Univ.', 'Calif.,', 'San', 'Diego'], sim_func=needleman_wunsch)
        2.0
        >>> monge_elkan(['Comput.', 'Sci.', 'and', 'Eng.', 'Dept.,', 'University', 'of', 'California,', 'San', 'Diego'], ['Department', 'of', 'Computer', 'Science,', 'Univ.', 'Calif.,', 'San', 'Diego'], sim_func=affine)
        2.25
        >>> monge_elkan([''], ['a'])
        0.0
        >>> monge_elkan(['Niall'], ['Nigel'])
        0.7866666666666667

    References:
        * Principles of Data Integration book
    """
    # input validations
    utils.sim_check_for_none(bag1, bag2)
    utils.sim_check_for_list_or_set_inputs(bag1, bag2)
    # if exact match return 1.0
    if utils.sim_check_for_exact_match(bag1, bag2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(bag1, bag2):
        return 0
    # aggregated sum of all the max sim score of all the elements in bag1
    # with elements in bag2
    sum_of_maxes = 0
    for t1 in bag1:
        max_sim = float('-inf')
        for t2 in bag2:
            max_sim = max(max_sim, sim_func(t1, t2))
        sum_of_maxes += max_sim
    sim = float(sum_of_maxes) / float(len(bag1))
    return sim


# noinspection PyArgumentList,PyArgumentList
def soft_tfidf(bag1, bag2, corpus_list=None, sim_func=jaro, threshold=0.5):
    """
    Compute Soft-tfidf measures between two lists given the corpus information.

    Args:
        bag1,bag2 (list): Input lists

        corpus_list (list of lists): Corpus list (default is set to None) of strings. If set to None,
            the input list are considered the only corpus

        sim_func (func): Secondary similarity function. This should return a similarity score between two strings (optional),
            default is jaro similarity measure

        threshold (float): Threshold value for the secondary similarity function (defaults to 0.5). If the similarity
            of a token pair exceeds the threshold, then the token pair is considered a match.

    Returns:
        Soft TF-IDF measure between the input lists

    Raises:
        TypeError : If the inputs are not lists or if one of the inputs is None.

    Examples:
        >>> soft_tfidf(['a', 'b', 'a'], ['a', 'c'], [['a', 'b', 'a'], ['a', 'c'], ['a']], sim_func=jaro, threshold=0.8)
        0.17541160386140586
        >>> soft_tfidf(['a', 'b', 'a'], ['a'], [['a', 'b', 'a'], ['a', 'c'], ['a']], threshold=0.9)
        0.5547001962252291
        >>> soft_tfidf(['a', 'b', 'a'], ['a'], [['x', 'y'], ['w'], ['q']])
        0.0
        >>> soft_tfidf(['aa', 'bb', 'a'], ['ab', 'ba'], sim_func=affine, threshold=0.6)
        0.81649658092772592

    References:
        * Principles of Data Integration book
    """
    # input validations
    utils.sim_check_for_none(bag1, bag2)
    utils.sim_check_for_list_or_set_inputs(bag1, bag2)
    # if the strings match exactly return 1.0
    if utils.sim_check_for_exact_match(bag1, bag2):
        return 1.0
    # if one of the strings is empty return 0
    if utils.sim_check_for_empty(bag1, bag2):
        return 0
    # if corpus is not provided treat input string as corpus
    if corpus_list is None:
        corpus_list = [bag1, bag2]
    corpus_size = len(corpus_list) * 1.0
    # term frequency for input strings
    tf_x, tf_y = collections.Counter(bag1), collections.Counter(bag2)
    # number of documents an element appeared
    element_freq = {}
    # set of unique element
    total_unique_elements = set()
    for document in corpus_list:
        temp_set = set()
        for element in document:
            # adding element only if it is present in one of two input string
            if element in bag1 or element in bag2:
                temp_set.add(element)
                total_unique_elements.add(element)
        # update element document frequency for this document
        for element in temp_set:
            element_freq[element] = element_freq[element] + 1 if element in element_freq else 1
    similarity_map = {}
    # calculating the term sim score against the input string 2, construct similarity map
    for x in bag1:
        if x not in similarity_map:
            max_score = 0.0
            for y in bag2:
                score = sim_func(x, y)
                # adding sim only if it is above threshold and highest for this element
                if score > threshold and score > max_score:
                    similarity_map[x] = utils.Similarity(x, y, score)
                    max_score = score
    result, v_x_2, v_y_2 = 0.0, 0.0, 0.0
    # soft-tfidf calculation
    for element in total_unique_elements:
        # numerator
        if element in similarity_map:
            sim = similarity_map[element]
            idf_first = corpus_size if sim.first_string not in element_freq else corpus_size / \
                                                                                 element_freq[sim.first_string]
            idf_second = corpus_size if sim.second_string not in element_freq else corpus_size / \
                                                                                   element_freq[sim.second_string]
            v_x = 0 if sim.first_string not in tf_x else idf_first * tf_x[sim.first_string]
            v_y = 0 if sim.second_string not in tf_y else idf_second * tf_y[sim.second_string]
            result += v_x * v_y * sim.similarity_score
        # denominator
        idf = corpus_size if element not in element_freq else corpus_size / element_freq[element]
        v_x = 0 if element not in tf_x else idf * tf_x[element]
        v_x_2 += v_x * v_x
        v_y = 0 if element not in tf_y else idf * tf_y[element]
        v_y_2 += v_y * v_y
    return result if v_x_2 == 0 else result / (math.sqrt(v_x_2) * math.sqrt(v_y_2))
