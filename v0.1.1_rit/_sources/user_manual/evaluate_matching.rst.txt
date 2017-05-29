==============================
Evaluating the Matching Output
==============================
Once you have predicted matches using ML-based matcher, then you would have to
evaluate the matches. py_entitymatching supports `eval_matches` command for that
purpose.

An example of using `eval_matches` command is shown below:

    >>> H = em.extract_feat_vecs(G, feat_table=match_f, attrs_after='gold_labels')
    >>> dt = em.DTMatcher()
    >>> dt.fit(table=H, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'], target_attr='gold_labels')
    >>> pred_table = dt.predict(table=H,  exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'gold_labels'],  append=True, target_attr='predicted_labels')
    >>> eval_summary = em.eval_matches(pred_table, 'gold_labels', 'predicted_labels')

In the above, `eval_summary` is a dictionary containing accuracy numbers (such as
precision, recall, F1, etc) and the list of false positives/negatives.

Please refer to the API reference of :py:meth:`~py_entitymatching.eval_matches` for
more details.

