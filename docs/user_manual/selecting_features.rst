==================
Selecting Features
==================
The command `select_features_univariate` can be used to select the most relevant features
 from a feature set. It requires a feature_table (see section :ref:`get_features`)
 to be generated on the given table. An example of using `select_features_univariate`
 is shown below:

    >>> feature_table_selected = select_features_univariate(feature_table, table,
    >>>     target_attr='gold', exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
    >>>     score='f_score', mode='k_best', parameter=2)

The above command will first project the given table into features by
excluding attributes specified by exclude_attrs and target by selecting attribute
specified by target_attr. The parameter `score` specifies the scoring function used
to measure the relevance between each feature and the target; `mode` specifies how to
select from the ranked features, and `parameter` specifies the number of features or
the percentile of features to return. The meaning of `parameter` depends on the chosen
`mode`. The function returns only selected features in the given feature_table.

Please refer to the API reference of :py:meth:`~py_entitymatching.select_feature_univariate`
for more details.