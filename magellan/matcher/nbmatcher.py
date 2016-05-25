from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts

from sklearn.naive_bayes import GaussianNB

class NBMatcher(MLMatcher):
    """
    Naive bayes matcher.
    """
    def __init__(self, *args, **kwargs):
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'NaiveBayes'+ '_' + get_ts()
        else:
            self.name = name
        super(NBMatcher, self).__init__()
        self.clf = GaussianNB(*args, **kwargs)