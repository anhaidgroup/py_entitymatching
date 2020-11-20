=========================================
Using Triggers to Update Matching Results
=========================================

Match Triggers
==============
Once you have used a matcher to predict results on a table, you might find
that there is some pattern of false positives or false negatives. Often, it
is useful to be able to create a set of rules to reevaluate tuple pair
predictions to correct these patterns of mistakes.

Creating the Trigger
--------------------
Each trigger can be created by calling its constructor. For example, a user can
create a trigger like this:

    >>> mt = em.MatchTrigger()

Please refer to :py:meth:`~py_entitymatching.MatchTrigger` for more details.

If you have already used a matcher, you should have already created a set of features
for matching. More information on this can be found in the section
:ref:`label-create-features-matching`.

Once the features are created, py_entitymatching stores this set of features in a
feature table. We refer to this feature table as `match_f`. Then you will be able
to instantiate a match trigger and add rules.

Adding and Deleting Rules
-------------------------
Once you have created the features, you can create rules like this:

    >>> mt = em.MatchTrigger()
    >>> mt.add_cond_rule(rule1, match_f)
    >>> mt.add_cond_rule(rule2, match_f)

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
That is, even if one of the rules return True, then the result for the tuple pair will
be true.

You also need to add a condition status and action when using match triggers. If the
result is the same value as the condition status, then the action will be carried out.
For example, the action and condition status can be declared like so:

    >>> mt.add_cond_status(False)
    >>> mt.add_action(0)

The condition status and action in the above example mean that if the rules in the
trigger return the value False, then the prediction will be changed to a 0.

Rules can also be deleted once they have been added:

    >>> rule_name = mt.add_cond_rule(rule_1, match_f)
    >>> mt.delete_rule(rule_name)

The command delete_rule must be given the name of the rule to be deleted. Rule names
and information on rules can be found using the following commands:

    >>> # get a list of rule names
    >>> rule_names = mt.get_rule_names()
    >>> # view rule source
    >>> mt.view_rule('rule_name')
    >>> # get rule fn
    >>> mt.get_rule('rule_name')

Executing the Triggers
----------------------
Once the rules, condition status, and action have been specified, the trigger can be
used to refine the predictions. An example of using the execute command is shown below:

    >>> mt.execute(input_table=H, label_column='prediction_labels', inplace=False)

For more information on the `execute` method, please refer to
:py:meth:`~py_entitymatching.MatchTrigger.execute` for more details.
