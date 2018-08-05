"""
This module contains the functions for Logistic Regression classifier.
"""
# from py_entitymatching.matcher.mlmatcher import MLMatcher
from py_entitymatching.dask.daskmlmatcher import DaskMLMatcher
from sklearn.linear_model import LogisticRegression
from py_entitymatching.matcher.matcherutils import get_ts


class DaskLogRegMatcher(DaskMLMatcher):
    """
    WARNING THIS MATCHER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.

    Logistic Regression matcher.

    Args:
        *args,**kwargs: THe Arguments to scikit-learn's Logistic Regression
            classifier.
        name (string): The name of this matcher (defaults to None). If the
            matcher name is None, the class automatically generates a string
            and assigns it as the name.


    """

    def __init__(self, *args, **kwargs):
        logger.warning(
            "WARNING THIS MATCHER IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK.")

        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'LogisticRegression' + '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        super(LogRegMatcher, self).__init__()
        # Set the classifier to the scikit-learn classifier.
        self.clf = LogisticRegression(*args, **kwargs)
        self.clf.classes_ = [0, 1]