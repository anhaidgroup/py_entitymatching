"""
This module contains functions for linear regression classifier.
"""
from py_entitymatching.matcher.mlmatcher import MLMatcher
from py_entitymatching.matcher.matcherutils import get_ts

from sklearn.linear_model import LinearRegression
from sklearn.base import BaseEstimator
from sklearn.base import ClassifierMixin
from sklearn.base import TransformerMixin

class LinRegClassifierSKLearn(BaseEstimator, ClassifierMixin, TransformerMixin):
    """
    This class implements Linear Regression classifer.

    Specifically, this class uses Linear Regression matcher from
    scikit-learn, wraps it up to form a classifier.


    """
    def __init__(self, *args, **kwargs):
        # Set the classifier to the scikit-learn Linear Regression matcher.
        self.clf = LinearRegression(*args, **kwargs)
        # Set the threshold to 0
        self.threshold = 0.0

    def fit(self, X, y):
        # Convert 0 and 1s to -1, and 1s
        y = (2 * y) - 1
        # Call the fit method of Linear Regression matcher
        self.clf.fit(X, y)
        # Return the wrapper object
        return self

    def predict(self, X):
        # Call the predict method from the underlying matcher
        y = self.clf.predict(X)
        # Convert back the predictions a number between -1 and 1 to -1 and -1
        y = (2 * (y > self.threshold)) - 1
        # Convert all the -1 to 0s
        y[y == -1] = 0
        # Return back the predictions
        return y

    def get_params(self, deep=True):
        """
        Function to get params. This will be used by other scikit-learn
        matchers.
        """
        return self.clf.get_params(deep=deep)


class LinRegMatcher(MLMatcher):
    """
    Linear regression matcher.

    Args:
        *args,**kwargs: Arguments to scikit-learn's Linear Regression matcher.
        name (string): Name that should be given to this matcher.
    """
    def __init__(self, *args, **kwargs):
        super(LinRegMatcher, self).__init__()
         # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name is not given, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'LinearRegression' + '_' + get_ts()
        else:
            # set the name for the matcher.
            self.name = name
        # Wrap the class implementing linear regression classifer.
        self.clf = LinRegClassifierSKLearn(*args, **kwargs)
