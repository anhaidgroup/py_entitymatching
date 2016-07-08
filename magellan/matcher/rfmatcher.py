"""
This module contains the functions for Random Forest classifier.
"""

from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts

from sklearn.ensemble import RandomForestClassifier

class RFMatcher(MLMatcher):
    """
    Random Forest matcher.

    Args:
        *args,**kwargs: Arguments to scikit-learn's Random Forest
        classifier.
        name (str): Name of this matcher (defaults to None).

    """
    def __init__(self, *args, **kwargs):
        super(RFMatcher, self).__init__()
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'RandomForest'+ '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        # Set the classifier to the scikit-learn classifier.
        self.clf = RandomForestClassifier(*args, **kwargs)
