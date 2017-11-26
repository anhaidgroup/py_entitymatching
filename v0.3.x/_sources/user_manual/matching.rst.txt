===========================================
Specifying Matchers and Performing Matching
===========================================

ML-Matchers
===========

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


Rule-Based Matchers
===================
You can write a few domain specific rules (for matching purposes) using the rule-based
matcher. If you want to write rules, then you must start by defining a set of features.
Each `feature` is a function that when applied to a tuple pair will return a
numeric value. We will discuss how to create a set of features in the section
:ref:`label-create-features-matching`.

Once the features are created, py_entitymatching stores this set of features in a
feature table. We refer to this feature table as `match_f`. Then you will be able
to instantiate a rule-based matcher and add rules.

Adding and Deleting Rules
-------------------------
Once you have created the features for matching, you can create rules like this:

    >>> brm = em.BooleanRuleMatcher()
    >>> brm.add_rule(rule1, match_f)
    >>> brm.add_rule(rule2, match_f)

In the above, `match_f` is a set of features stored as a Dataframe (see section
:ref:`label-create-features-matching`).

Each rule is a list of strings. Each string specifies a conjunction of predicates. Each
predicate has three parts: (1) an expression, (2) a comparison operator, and (3) a
value. The expression is evaluated over a tuple pair, producing a numeric value.
Currently, in py_entitymatching an expression is limited to contain a single feature
(being applied to a tuple pair). So an example predicate will look like this:
::

    name_name_lev(ltuple, rtuple) > 3

In the above `name_name_lev` is feature. Concretely, this feature computes
Levenshtein distance between the `name` values in the input tuple pair.

As an example, the rules `rule1` and `rule2` can look like this:
::

    rule1 = ['name_name_lev(ltuple, rtuple) > 3', 'age_age_exact_match(ltuple, rtuple) !=0']
    rule2 = ['address_address_lev(ltuple, rtuple) > 6']

In the above, `rule1` contains two predicates and `rule2` contains just a single
predicate. Each rule is a conjunction of predicates. That is, each rule will return True
only if all the predicates return True. The matcher is then a disjunction of rules.
That is, even if one of the rules return True, then the tuple pair will be a match.

Rules can also be deleted once they have been added to the matcher:

    >>> rule_name = brm.add_rule(rule_1, match_f)
    >>> brm.delete_rule(rule_name)

The command delete_rule must be given the name of the rule to be deleted. Rule names
and information on rules in a matcher can be found using the following commands:

    >>> # get a list of rule names
    >>> rule_names = brm.get_rule_names()
    >>> # view rule source
    >>> brm.view_rule('rule_name')
    >>> # get rule fn
    >>> brm.get_rule('rule_name')

Applying Rule-Based Matcher
---------------------------

Once the rules are specified, you can predict the matches using the
`predict` command. An example of using the `predict` command is shown below:

    >>> brm.predict(table=H, target_attr='predicted_labels', inplace=True)

For more information on the `predict` method, please refer to
:py:meth:`~py_entitymatching.BooleanRuleMatcher.predict` for more details.
