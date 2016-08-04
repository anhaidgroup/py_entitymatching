"""
This module contains the functions for SVM classifier.

"""
from py_entitymatching.matcher.mlmatcher import MLMatcher
from py_entitymatching.matcher.matcherutils import get_ts
from sklearn.svm import SVC


class SVMMatcher(MLMatcher):
    """
    SVM matcher.

    Args:
        *args,**kwargs: The arguments to scikit-learn's SVM
            classifier.
        name (string): The name of this matcher (defaults to None). If the
             matcher name is None, the class automatically generates a string
             and assigns it as the name.


    """
    def __init__(self, *args, **kwargs):
        super(SVMMatcher, self).__init__()
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'SVM'+ '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        # Set the classifier to the scikit-learn classifier.
        self.clf = SVC(*args, **kwargs)