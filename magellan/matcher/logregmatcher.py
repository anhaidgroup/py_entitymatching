
from magellan.matcher.mlmatcher import MLMatcher
from sklearn.linear_model import LogisticRegression
from magellan.matcher.matcherutils import get_ts

class LogRegMatcher(MLMatcher):
    """
    Logistic regression matcher
    """
    def __init__(self, *args, **kwargs):
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'LogisticRegression'+ '_' + get_ts()
        else:
            self.name = name
        super(LogRegMatcher, self).__init__()
        self.clf = LogisticRegression(*args, **kwargs)