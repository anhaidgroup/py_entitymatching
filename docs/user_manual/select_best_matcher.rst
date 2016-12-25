======================
Selecting a ML Matcher
======================
Once the user has created different concrete ML matchers, he/she would
like to choose one of them.
There are many different criteria by which one can decide to choose a
matcher such as `akaike information criterion`, `bayesian information
criterion`, `k-fold cross validation`, etc. Currently *py_entitymatching* supports
k-fold cross validation and other approaches are left for future work.

Conceptually, the command to select a matcher would take in the following inputs:

* List of ML matchers.
* Training data (feature vector).
* A column of labels that correspond to the feature vectors in the training data.
* Number of folds.

And it would produce the following output:

* Selected matcher.
* Statistics such as mean accuracy of all input matchers.

In *py_entitymatching*, `select_matcher` command addresses the above needs. An
example of using `select_matcher` is shown below:

    >>> dt = em.DTMatcher()
    >>> rf = em.RFMatcher()
    >>> result = em.select_matcher(matchers=[dt, rf], table=train, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels', k=5)

In the above the output, `result` is a dictionary containing two keys: (1) selected_matcher,
and (2) cv_stats. `selected_matcher` is the selected ML-based matcher, `cv_stats` includes
cross validation statistics such as false positives, false negatives for each fold.

Please refer to the API reference of :py:meth:`~py_entitymatching.select_matcher` for
more details.