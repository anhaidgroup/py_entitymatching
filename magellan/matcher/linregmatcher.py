from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts
from sklearn.linear_model import LinearRegression
from sklearn.base import BaseEstimator
from sklearn.base import ClassifierMixin
from sklearn.base import TransformerMixin

class LinRegClassifierSKLearn(BaseEstimator, ClassifierMixin, TransformerMixin):
    def __init__(self, *args, **kwargs):
        self.clf = LinearRegression(*args, **kwargs)
        self.threshold = 0.0
    def fit(self, X, y):
        y = (2 * y) - 1
        self.clf.fit(X, y)
        return self

    def predict(self, X):
        y = self.clf.predict(X)
        y = (2 * (y > self.threshold)) - 1
        y[y == -1] = 0
        return y

    def get_params(self, deep=True):
        return self.clf.get_params(deep=deep)


class LinRegMatcher(MLMatcher):
    """
    Linear regression matcher
    """
    def __init__(self, *args, **kwargs):
        super(LinRegMatcher, self).__init__()
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'LinearRegression' + '_' + get_ts()
        else:
            self.name = name
        self.clf = LinRegClassifierSKLearn(*args, **kwargs)
