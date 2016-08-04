"""
This module contains functions for ML-matcher combiner selection.
Note: This is not going to be there for the first release of py_entitymatching.
"""

import itertools
import six

from py_entitymatching.matcherselector.mlmatcherselection import select_matcher
from py_entitymatching.matcher.ensemblematcher import EnsembleMatcher

def selector_matcher_combiner(matchers, combiners, x=None, y=None, table=None, exclude_attrs=None, target_attr=None,
                              weights=None, threshold=None, k=5):
    if not isinstance(matchers, list):
        matchers = [matchers]
    if not isinstance(combiners, list):
        combiners = [combiners]
    matcher_list = get_matcher_list(matchers, combiners, weights, threshold)
    return select_matcher(matcher_list, x=x,  y=y, table=table, exclude_attrs=exclude_attrs, target_attr=target_attr,
                          k=k)
def get_matcher_list(matchers, combiners, weights, threshold):
    ensemble_len = range(2, len(matchers) + 1)
    matcher_list = []
    matcher_list.extend(matchers)
    for l in ensemble_len:
        iter_combns = itertools.combinations(six.moves.xrange(0,
                                                              len(matchers)), l)
        for ic in iter_combns:
            for c in combiners:
                m = [matchers[i] for i in ic]
                if c is 'Weighted':
                    em = EnsembleMatcher(m, voting=c, weights=weights, threshold=threshold)
                else:
                    em = EnsembleMatcher(m, voting=c)
                matcher_list.append(em)
    return matcher_list