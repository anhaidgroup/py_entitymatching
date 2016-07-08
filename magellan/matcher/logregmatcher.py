"""
This module contains the functions for Logistic Regression classifier.
"""
from magellan.matcher.mlmatcher import MLMatcher
from sklearn.linear_model import LogisticRegression
from magellan.matcher.matcherutils import get_ts

class LogRegMatcher(MLMatcher):
    """
    Logistic Regression matcher.

    Args:
        *args,**kwargs: Arguments to scikit-learn's Logistic Regression
        classifier.
        name (str): Name of this matcher (defaults to None).

    """
    def __init__(self, *args, **kwargs):
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'LogisticRegression'+ '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        super(LogRegMatcher, self).__init__()
        # Set the classifier to the scikit-learn classifier.
        self.clf = LogisticRegression(*args, **kwargs)