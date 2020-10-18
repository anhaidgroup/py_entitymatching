"""
This module contains functions for ensembe matcher.
Note: This is not going to be there in the first version of py_entitymatching.
"""

import pandas as pd
import numpy as np
import six

from sklearn.base import BaseEstimator
from sklearn.base import ClassifierMixin
from sklearn.base import TransformerMixin
from sklearn.base import clone
from sklearn.pipeline import _name_estimators

from py_entitymatching.matcher.mlmatcher import MLMatcher
from py_entitymatching.matchercombiner.matchercombiner import MajorityVote, WeightedVote

class EnsembleSKLearn(BaseEstimator, ClassifierMixin, TransformerMixin):
    def __init__(self, clfs, voting, weights=None, threshold=None):
        self.clfs = clfs
        self.named_clfs = {key:value for key,value in _name_estimators(clfs)}
        self.voting=voting
        if voting is 'weighted':
            self.combiner=WeightedVote(weights=weights, threshold=threshold)
        elif voting is 'majority':
            self.combiner=MajorityVote()
        else:
            raise AttributeError('Unrecognized voting method')

    def fit(self, X, y):
        self.clfs_ = []
        for clf in self.clfs:
            fitted_clf = clone(clf).fit(X, y)
            self.clfs_.append(fitted_clf)
        return self

    def predict(self, X):
        return self._predict(X)

    def _predict(self, X):
        """ Collect results from clf.predict calls. """
        predictions =  np.asarray([clf.predict(X) for clf in self.clfs_]).T
        predicted_labels = self.combiner.combine(predictions)
        return predicted_labels

    def get_params(self, deep=True):
        """ Return estimator parameter names for GridSearch support"""
        if not deep:
            return super(EnsembleSKLearn, self).get_params(deep=False)
        else:
            out = self.named_clfs.copy()
            for name, step in six.iteritems(self.named_clfs):
                for key, value in six.iteritems(step.get_params(deep=True)):
                    out['%s__%s' % (name, key)] = value
            return out

class EnsembleMatcher(MLMatcher):
    def __init__(self, matchers, name=None, voting='weighted', weights=None, threshold=None):
        clfs = [m.clf for m in matchers]
        self.clf = EnsembleSKLearn(clfs, voting, weights, threshold)
        if name is None:
            names = [matcher.get_name() for matcher in matchers ]
            self.name = voting+':'
            self.name += ','.join(names)

        else:
            self.name = name
