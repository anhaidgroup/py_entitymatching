======================
Selecting a ML-Matcher
======================
Once you have created different concrete ML matchers, then you have to choose one of
them for matching purposes. There are many different criteria by which one can
decide to choose a matcher such as `akaike information criterion`, `bayesian information
criterion`, `k-fold cross validation`, etc. Currently py_entitymatching supports
k-fold cross validation and other approaches are left for future work.

Conceptually, the command to select a matcher would take in the following inputs:

* List of ML matchers.
* Training data (feature vector).
* A column of labels that correspond to the feature vectors in the training data.
* Number of folds.

And it would produce the following output:

* Selected matcher.
* Statistics such as mean accuracy of all input matchers.

In py_entitymatching, `select_matcher` command addresses the above needs. An
example of using `select_matcher` is shown below:

    >>> dt = em.DTMatcher()
    >>> rf = em.RFMatcher()
    >>> result = em.select_matcher(matchers=[dt, rf], table=train, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels', k=5)

In the above the output, `result` is a dictionary containing three keys: (1) selected_matcher,
(2) cv_stats, and (3) drill_down_cv_stats. `selected_matcher` is the selected ML-based matcher,
`cv_stats` is a Dataframe which includes the average cross validation scores for each matcher
and for each metric, and 'drill_down_cv_stats' is a dictionary where each key is a metric that
includes the cross validation statistics for each fold.

Please refer to the API reference of :py:meth:`~py_entitymatching.select_matcher` for
more details.
