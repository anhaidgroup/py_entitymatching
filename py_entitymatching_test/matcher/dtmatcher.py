"""
This module contains the functions for Decision Tree learning-based matcher.
"""
from py_entitymatching.matcher.mlmatcher import MLMatcher
from py_entitymatching.matcher.matcherutils import get_ts

from sklearn.tree import DecisionTreeClassifier

class DTMatcher(MLMatcher):
    """
    Decision Tree matcher.

    Args:
        *args,**kwargs: The arguments to scikit-learn's Decision Tree
            classifier.
        name (string): The name of this matcher (defaults to None). If the
            matcher name is None, the class automatically generates a string
            and assigns it as the name.
    Notes:
        For more details please see

    """
    def __init__(self, *args, **kwargs):
        super(DTMatcher, self).__init__()
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'DecisionTree' + '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        # Set the classifier to the scikit-learn classifier.
        self.clf = DecisionTreeClassifier(*args, **kwargs)
