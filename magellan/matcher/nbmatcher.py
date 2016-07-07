"""
This module contains the functions for Naive Bayes classifier.
"""

from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts

from sklearn.naive_bayes import GaussianNB

class NBMatcher(MLMatcher):
    """
    Naive Bayes matcher.

    Args:
        *args,**kwargs: Arguments to scikit-learn's Naive Bayes
         classifier.
        name (str): Name of this matcher (defaults to None).

    """
    def __init__(self, *args, **kwargs):
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'NaiveBayes'+ '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        super(NBMatcher, self).__init__()
        # Set the classifier to the scikit-learn classifier.
        self.clf = GaussianNB(*args, **kwargs)