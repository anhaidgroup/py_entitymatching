from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts

from sklearn.tree import DecisionTreeClassifier

class DTMatcher(MLMatcher):
    """
    Decision tree matcher
    """
    def __init__(self, *args, **kwargs):
        super(DTMatcher, self).__init__()

        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'DecisionTree' + '_' + get_ts()
        else:
            self.name = name
        self.clf = DecisionTreeClassifier(*args, **kwargs)
