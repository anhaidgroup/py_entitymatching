==============================================
Specifying ML-Matchers and Performing Matching
==============================================
Once yor convert the labeled sample  into a table of feature vectors (and their
labels), the we can  can create and apply matchers to the feature vectors.
Currently py_entitymatching supports only ML-based matchers. Implementation wise,
a Matcher is defined as a Python class with certain methods (and some common
utility functions) and all concrete blockers inherit from this Matcher class and
override the methods. Specifically, each concrete matcher will implement at least
the following methods:

* fit (for training)
* predict (for prediction)

Creating Learning-Based Matchers
--------------------------------
In py_entitymatching, there are seven concrete ML-matchers implemented: (1) naive bayes,
(2) logistic regression, (3) linear regression, (4) support vector machine, (5) decision
trees, (6) random forest, and (7) xgboost matcher.

These concrete matchers are just wrappers of scikit-learn matchers or that supports
scikit-learn wrappers (for eg., xgboost) and this is because
the fit/predict methods in scikit-learn are not metadata aware. The concrete matchers
make the scikit-learn matchers metadata aware.


Each matcher can be created by calling its constructor. Since these matchers are
just the wrappers of scikit-learn matchers, the parameters that can be given to
scikit-learn matchers can be to given to the matchers in py_entitymatching.
For example, a user can create a Decision Tree matcher like this:

    >>> dt = em.DTMatcher(max_depth=5)

Please refer to :py:meth:`~py_entitymatching.DTMatcher`, :py:meth:`~py_entitymatching.RFMatcher`,
:py:meth:`~py_entitymatching.NBMatcher`, :py:meth:`~py_entitymatching.LogisticRegressionMatcher`,
:py:meth:`~py_entitymatching.LinearRegressionMatcher`, :py:meth:`~py_entitymatching.SVMMatcher`, and
:py:meth:`~py_entitymatching.XGBoostMatcher`
for more details.

Training Learning-Based Matchers
--------------------------------
Once the ML-matcher is instantiated, you can train the matcher using the
`fit` command. An example of using the `fit` command for Decision Tree matcher
is shown below:

    >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels')

There are other variants of `fit` method. As an example, Please refer to
:py:meth:`~py_entitymatching.DTMatcher.fit` for more details.

Applying Learning-Based Matchers
--------------------------------
Once the ML-matcher is trained, you can predict the matches using the
`predict` command. An example of using the `predict` command for Decision Tree matcher
is shown below:

    >>> dt.predict(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='predicted_labels', return_probs=True, probs_attr='proba', append=True,
    inplace=True)

There are other variants of `predict` method. As an example, Please refer to
:py:meth:`~py_entitymatching.DTMatcher.predict` for more details.
