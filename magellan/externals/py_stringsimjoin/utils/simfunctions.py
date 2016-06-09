"""Similarity measure utilities"""

from magellan.externals.py_stringsimjoin.externals.py_stringmatching.simfunctions import \
    cosine, dice, jaccard, levenshtein


def get_sim_function(sim_measure_type):
    """Obtain a similarity function.

    Args:
        sim_measure_type : String, similarity measure type ('JACCARD', 'COSINE', 'DICE', 'EDIT_DISTANCE', ''OVERLAP')

    Returns:
        similarity function

    Examples:
        >>> jaccard_fn = get_sim_function('JACCARD')
    """
    if sim_measure_type == 'COSINE':
        return cosine
    elif sim_measure_type == 'DICE':
        return dice
    elif sim_measure_type == 'EDIT_DISTANCE':
        return levenshtein
    elif sim_measure_type == 'JACCARD':
        return jaccard
    elif sim_measure_type == 'OVERLAP':
        return overlap


def overlap(set1, set2):
    """
    Computes the overlap between two sets.

    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.

    Returns:
        overlap (int)
    """
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    return len(set1.intersection(set2))
