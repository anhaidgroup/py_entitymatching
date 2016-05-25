from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts

from sklearn.ensemble import RandomForestClassifier

class RFMatcher(MLMatcher):
    """
    Randomforest matcher
    """
    def __init__(self, *args, **kwargs):
        super(RFMatcher, self).__init__()
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'RandomForest'+ '_' + get_ts()
        else:
            self.name = name
        self.clf = RandomForestClassifier(*args, **kwargs)
