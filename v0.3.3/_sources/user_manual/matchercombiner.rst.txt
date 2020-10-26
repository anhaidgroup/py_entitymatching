============================================
Combining Predictions from Multiple Matchers
============================================
In the matching step, if you use multiple matchers then you will have to combine the
predictions from them to get a consolidated prediction. There are many different ways
to combine these predictions such as weighted vote, majority vote, stacking, etc.
Currently, py_entitymatching supports majority and weighted voting-based combining.
These combiners are experimental and not tested.

An example of using majority voting-based combining is shown below.

    >>> dt = DTMatcher()
    >>> rf = RFMatcher()
    >>> nb = NBMatcher()
    >>> dt.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label') # H is training set containing feature vectors
    >>> dt.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='dt_predictions') # L is the test set for which we should get predictions.
    >>> rf.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
    >>> rf.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='rf_predictions')
    >>> nb.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
    >>> nb.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='nb_predictions')
    >>> mv_combiner = MajorityVote()
    >>> L['consol_predictions'] = mv_combiner.combine(L[['dt_predictions', 'rf_predictions', 'nb_predictions']])

Conceptually, given a list of predictions (from different matchers) the prediction that
occurs most is returned as the consolidated prediction. If there is no clear winning
prediction (for example, 0 and 1 occuring equal number of times) then 0 is returned.

An example of using weighted voting-based combining is shown below.


    >>> dt = DTMatcher()
    >>> rf = RFMatcher()
    >>> nb = NBMatcher()
    >>> dt.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label') # H is training set containing feature vectors
    >>> dt.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='dt_predictions') # L is the test set for which we should get predictions.
    >>> rf.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
    >>> rf.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='rf_predictions')
    >>> nb.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
    >>> nb.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='nb_predictions')
    >>> wv_combiner = WeightedVote(weights=[0.3, 0.2, 0.1], threshold=0.4)
    >>> L['consol_predictions'] = wv_combiner.combine(L[['dt_predictions',
    'rf_predictions', 'nb_predictions']])

Conceptually, given a list of predictions, each prediction is given a
weight, we compute a weighted sum of these predictions and compare the result to a
threshold. If the result is greater than or equal to the threshold then the
consolidated prediction is returned as 1 (i.e., a match) else returned as 0 (no-match).
