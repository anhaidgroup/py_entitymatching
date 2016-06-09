import functools

import six

"""
This module defines a list of decorator functions to check input strings/list. The reason this is separated
from the similarity functions is the implementation of checking functions can change later, depending on
our decision to handle missing values.
"""


# def _sim_check_for_list_or_set_inputs(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if not isinstance(args[0], list):
#             if not isinstance(args[0], set):
#                 raise TypeError('First argument is expected to be a python list or set')
#         if not isinstance(args[1], list):
#             if not isinstance(args[1], set):
#                 raise TypeError('Second argument is expected to be a python list or set')
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _sim_check_for_string_inputs(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if not isinstance(args[0], six.string_types):
#             raise TypeError('First argument is expected to be a string')
#         if not isinstance(args[1], six.string_types):
#             raise TypeError('Second argument is expected to be a string')
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _sim_check_for_same_len(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if args[0] is None:
#             raise TypeError("string1 is None")
#         if args[1] is None:
#             raise TypeError("string2 is None")
#         if len(args[0]) != len(args[1]):
#             raise ValueError("Undefined for sequences of unequal length")
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _sim_check_for_exact_match(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if args[0] == args[1]:
#             return 1.0
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _sim_check_for_empty(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if len(args[0]) == 0 or len(args[1]) == 0:
#             return 0
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _sim_check_for_none(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if args[0] is None:
#             raise TypeError("string1 is None")
#         if args[1] is None:
#             raise TypeError("string2 is None")
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _tok_check_for_none(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         empty_list = []
#         if args[0] is None:
#             return empty_list
#         return func(*args, **kwargs)
#
#     return decorator
#
#
# def _tok_check_for_string_input(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if not isinstance(args[0], six.string_types):
#             raise TypeError('Input is expected to be a string')
#         return func(*args, **kwargs)
#
#     return decorator


def sim_check_for_none(*args):
    if len(args) > 0 and args[0] is None:
        raise TypeError("First argument cannot be None")
    if len(args) > 1 and args[1] is None:
        raise TypeError("Second argument cannot be None")


def sim_check_for_empty(*args):
    if len(args[0]) == 0 or len(args[1]) == 0:
        return True


def sim_check_for_same_len(*args):
    if len(args[0]) != len(args[1]):
        raise ValueError("Undefined for sequences of unequal length")


def sim_check_for_string_inputs(*args):
    if not isinstance(args[0], six.string_types):
        raise TypeError('First argument is expected to be a string')
    if not isinstance(args[1], six.string_types):
        raise TypeError('Second argument is expected to be a string')


def sim_check_for_list_or_set_inputs(*args):
    if not isinstance(args[0], list):
        if not isinstance(args[0], set):
            raise TypeError('First argument is expected to be a python list or set')
    if not isinstance(args[1], list):
        if not isinstance(args[1], set):
            raise TypeError('Second argument is expected to be a python list or set')


def sim_check_tversky_parameters(alpha, beta):
        if alpha < 0 or beta < 0:
            raise ValueError('Tversky parameters should be greater than or equal to zero')

def sim_check_for_exact_match(*args):
    if args[0] == args[1]:
        return True

def sim_check_for_zero_len(*args):
    if len(args[0].strip()) == 0 or len(args[1].strip()) == 0:
        raise ValueError("Undefined for string of zero length")

def tok_check_for_string_input(*args):
    for i in range(len(args)):
        if not isinstance(args[i], six.string_types):
            raise TypeError('Input is expected to be a string')


def tok_check_for_none(*args):
    if args[0] is None:
        raise TypeError("First argument cannot be None")


class Similarity:
    def __init__(self, string1, string2, score):
        self.first_string = string1
        self.second_string = string2
        self.similarity_score = score

class Editex:
    letter_groups = dict()
    letter_groups['A'] = letter_groups['E'] = letter_groups['I'] = letter_groups['O'] \
        = letter_groups['U'] = letter_groups['Y'] = 0
    letter_groups['B'] = letter_groups['P'] = 1
    letter_groups['C'] = letter_groups['K'] = letter_groups['Q'] = 2
    letter_groups['D'] = letter_groups['T'] = 3
    letter_groups['L'] = letter_groups['R'] = 4
    letter_groups['M'] = letter_groups['N'] = 5
    letter_groups['G'] = letter_groups['J'] = 6
    letter_groups['F'] = letter_groups['P'] = letter_groups['V'] = 7
    letter_groups['S'] = letter_groups['X'] = letter_groups['Z'] = 8
    letter_groups['C'] = letter_groups['S'] = letter_groups['J'] = 9
    all_letters = frozenset('AEIOUYBPCKQDTLRMNGJFVSXZ')

    def __init__(self, match_cost, mismatch_cost, group_cost):
        self.match_cost = match_cost
        self.mismatch_cost = mismatch_cost
        self.group_cost = group_cost

    def r_cost(self, ch1, ch2):
        """Return r(a,b) according to Zobel & Dart's definition
        """
        if ch1 == ch2:
            return self.match_cost
        if ch1 in Editex.all_letters and ch2 in Editex.all_letters:
            if Editex.letter_groups[ch1] == Editex.letter_groups[ch2]:
                return self.group_cost
        return self.mismatch_cost

    def d_cost(self, ch1, ch2):
        """Return d(a,b) according to Zobel & Dart's definition
        """
        if ch1 != ch2 and (ch1 == 'H' or ch1 == 'W'):
            return self.group_cost
        return self.r_cost(ch1, ch2)

# # check for NaNs
# def check_strings_for_nulls(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         if np.isnan(args[0]) is True:
#             return np.NaN
#         if np.isnan(args[1]) is None:
#             return np.NaN
#         return func(*args, **kwargs)
#     return decorator
#
# # check for nulls in tokens
# def check_tokens_for_nulls(func):
#     @functools.wraps(func)
#     def decorator(*args, **kwargs):
#         tmp_args0 = args[0]
#         if not isinstance(tmp_args0, list):
#             tmp_args0 = [tmp_args0]
#         if any(np.isnan(tmp_args0)) is True:
#             return np.NaN
#         tmp_args1 = args[1]
#         if not isinstance(tmp_args1, list):
#             tmp_args1 = [tmp_args1]
#         if any(np.isnan(tmp_args1)) is True:
#             return np.NaN
#         return func(*args, **kwargs)
#     return decorator
