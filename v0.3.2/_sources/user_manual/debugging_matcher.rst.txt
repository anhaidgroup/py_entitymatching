=====================
Debugging ML-Matchers
=====================
While doing entity matching you would like to choose a matcher that produces the desired
precision, recall or F1 numbers. If a matcher does not produce the desired accuracy, then
you would like to debug the matcher. py_entitymatching supports two ways to
debug: (1) using the GUI, and (2) using the command line.

Debugging Using the GUI
-----------------------
py_entitymatching supports debugging using the GUI for a subset of ML-based matchers.
Specifically, it supports debugging Decision Tree matcher and Random Forest matcher.
You can use `vis_debug_dt` and `vis_debug_rf` to debug Decision Tree matcher
and Random Forest matcher respectively.

An example of using `vis_debug_dt` is shown below:

    >>> dt = em.DTMatcher()
    >>> train_test = em.split_train_test(devel, 0.5)
    >>> train, test = train_test['train'], train_test['test']
    >>> em.vis_debug_dt(dt, train, test, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], target_attr='gold_labels')

The command would display a GUI containing evaluation summary and an option to see tuples
flagged as `false positives` or `false negatives`. If you select `false positives`
then false positive tuple pairs would be displayed in the adjoining window. Similarly,
if `false negatives` is selected then false negative tuple pairs would be
displayed. By default, `false positives` is selected.
Each tuple pair is displayed with two buttons: `show` and `debug`. If you click on
`show`, then individual tuples (of that tuple pair) are displayed in a separate window.
If you click on `debug`, then a window with individual tuples and the
path taken by the feature vector in the Decision Tree that leads to the predicted value
is displayed.

The usage of `vis_debug_rf` is same as `vis_debug_dt`. The command would display a GUI
similar to `vis_debug_dt`, except the debug window would list a set of trees. You can
expand each tree to see the path taken by the features in that tree.

Please refer to the API reference of :py:meth:`~py_entitymatching.vis_debug_dt` and
:py:meth:`~py_entitymatching.vis_debug_rf` for more details.


If you want to debug a Decision Tree matcher or Random Forest matcher using GUI,
then we recommend the following steps:

1. In the displayed GUI, check precision and recall numbers in evaluation summary.

2. If the user wants to improve precision, then he/she should choose to see false positives.

3. If the user wants to improve recall, then he/she should choose to see false negatives.

4. In the displayed (false positive/false negative) tuple pairs,
   you can click on the `show` button to see the tuples from the left and right tables.

5. In the displayed (false positive/false negative) tuple pairs, you can choose a tuple
   and click on the `debug` button to see the detailed evaluation path of that tuple.

6. Based on the input tuples, predicates at each node and the actual feature value,
   you should decide on the next step. Some of the possible next steps are
   cleaning  the input data, adding more features, adding more training data, trying a
   different matcher, etc.


Debugging Using the Command Line
--------------------------------

Similar to debugging using the GUI, py_entitymatching supports command line debugging for two
ML matchers: Decision Tree and Random Forest. Currently, py_entitymatching  supports
command line debugging only using tuple pairs, other approaches are left for future work.

You can use `debug_decisiontree_matcher` and `debug_randomforest_matcher` to debug
Decision Tree matcher and Random Forest matcher respectively.

An example of using `debug_decisiontree_matcher` is shown below:

    >>> H = em.extract_feat_vecs(devel, feat_table=match_f, attrs_after='gold_labels')
    >>> dt = em.DTMatcher()
    >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
    >>> out = dt.predict(table=F, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
    >>> em.debug_decisiontree_matcher(dt, A.ix[1], B.ix[2], match_f, H.columns, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')

In the above, the debug command prints the path taken by the feature vector, its
evaluation status at each node and the actual feature value at each node.

The usage of `debug_randomforest_matcher` is same as `debug_decisiontree_matcher`.
Similar to `debug_decisiontree_matcher` command, it prints the path taken by the feature
vector, except that it displays the path taken in each tree of the Random Forest.

Please refer to the API reference of :py:meth:`~py_entitymatching.debug_decisiontree_matcher`
and :py:meth:`~py_entitymatching.debug_randomforest_matcher` for more details.


If you want to debug a Decision Tree matcher or Random
Forest matcher using the command line, then we recommend the following steps:

1. Evaluate the accuracy of predictions using user created labels. The evaluation can
   be done using :py:meth:`~py_entitymatching.eval_matches` command.

2. If you want to improve precision, then he/she should debug false positives.

3. If you want to improve recall, then he/she should debug false negatives.

4. You should then retrieve the tuples from the tuple id pairs listed in evaluation
   summary, and debug using the commands described above.

5. Based on the input tuples, predicates at each node and the actual feature value,
   you should decide on the next step. Some of the possible next steps are clean
   the input data, add more features, add more training data, try different matcher, etc.

Impact of Imputing Missing Values
---------------------------------
You should be aware of the following subtleties as it would
have an impact when he/she imputes values to feature vector set:

1. When you use the GUI for debugging, you would first choose to see
false positives/false negatives and then you would click the `debug` button to debug
that tuple pair. In this case, the feature vector in that row is given as input to find the path
traversed in the Decision Tree. If you had imputed the feature vector set to get
rid of NaN’s, then the imputed values would be considered to find the path traversed.

2. When you use the command line for debugging, then you would first evaluate the
predictions, select false positive or false negative tuple pairs to debug, retrieve the
tuples from the left and right tables and finally give them as input to command line
debugger commands. If you had imputed the feature vector set to get rid of NaN’s (using
a aggregate strategy), then imputed values would not be known to the debugger.

So if the input tables have NaN’s, then the output of the command line debugger would only
be partially correct (i.e., the displayed predicates would be correct, but the predicate
outcome may differ between current tuple pair and the actual feature vector used during
prediction).




