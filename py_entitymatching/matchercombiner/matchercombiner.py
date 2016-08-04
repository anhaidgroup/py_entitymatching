# import numpy as np
from math import ceil
import pandas as pd

class MajorityVote(object):
    def combine(self, predictions):
        combined_prediction = pd.np.apply_along_axis(lambda x: pd.np.argmax(
            pd.np.bincount(x)), axis=1, arr=predictions)
        return combined_prediction

class WeightedVote(object):
    def __init__(self, weights=None, threshold=None):
        self.weights = weights
        self.threshold = threshold

    def combine(self, predictions):
        num_matchers = predictions.shape[1]
        if self.weights is not None:
            assert num_matchers is len(num_matchers), 'Num matchers and weights do not match'
            w = pd.np.asarray(self.weights)
        else:
            w = pd.np.ones(num_matchers, )

        if self.threshold is None:
            t = ceil((num_matchers+1.0)/2.0)
        else:
            t = self.threshold

        combined_prediction = pd.np.apply_along_axis(lambda x: 1 if
        pd.np.inner(x, w) >= t else 0, axis=1, arr=predictions)
        return combined_prediction
