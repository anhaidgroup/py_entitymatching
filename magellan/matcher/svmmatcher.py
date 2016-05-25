from magellan.matcher.mlmatcher import MLMatcher
from magellan.matcher.matcherutils import get_ts
from sklearn.svm import SVC


class SVMMatcher(MLMatcher):
    """
    SVM matcher
    """
    def __init__(self, *args, **kwargs):
        super(SVMMatcher, self).__init__()
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'SVM'+ '_' + get_ts()
        else:
            self.name = name
        self.clf = SVC(*args, **kwargs)