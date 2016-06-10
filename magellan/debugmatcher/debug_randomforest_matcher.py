# input : clf, t1, t2, feat_table, features.
# output:
# Summary : Num Trees: 10; Mean Probability of False match = 1/10; Mean Probability of True match = 9/10 ;
#           Match status : T/F
# Tree 1
#   --
#   --
#   -- Probability for non-match : ; Prob. for match :
import logging

import pandas as pd

from magellan.debugmatcher.debug_decisiontree_matcher import _debug_decisiontree_matcher, get_prob
from magellan.matcher.rfmatcher import RFMatcher
from magellan.utils.catalog_helper import check_attrs_present

logger = logging.getLogger(__name__)


def debug_randomforest_matcher(rf, t1, t2, feat_table, fv_columns, exclude_attrs=None):
    if not isinstance(feat_table, pd.DataFrame):
        logger.error('The input feature table is not of type dataframe')
        raise AssertionError('The input feature table is not of type dataframe')

    i = 1
    if isinstance(rf, RFMatcher):
        clf = rf.clf
    else:
        clf = rf

    if exclude_attrs is None:
        feature_names = fv_columns
    else:
        cols = [c not in exclude_attrs for c in fv_columns]
        feature_names = fv_columns[cols]

    prob = get_prob(clf, t1, t2, feat_table, feature_names)
    prediction = False
    if prob[1] > prob[0]:
        prediction = True
    print(
        "Summary: Num trees = {0}; Mean Prob. for non-match = {1}; Mean Prob for match = {2}; "
        "Match status =  {3}".format(
            str(len(clf.estimators_)), str(
                prob[0]), str(prob[1]), str(prediction)))

    print("")
    for e in clf.estimators_:
        print("Tree " + str(i))
        i += 1
        p = _debug_decisiontree_matcher(e, t1, t2, feat_table, fv_columns, exclude_attrs,
                                        ensemble_flag=True)
        print("")
