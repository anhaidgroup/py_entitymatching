=========================
rescaling_feature_vectors
=========================
The command `scale_features` can be used to rescale/normalize the feature
 vectors in the given table. It is often called before selecting features
(see section :ref:`select_features_univariate`) or selecting best matcher (see section
:ref:`select_matcher`). An example of using `scale_features` is shown below:

    >>> table, scaler = em.scale_features(table,
    >>>                                   exclude_attrs=['_id', 'ltable_ID', 'rtable_ID'],
    >>>                                   scaling_method='MinMax',
    >>>                                   scaler=None)

The above command will first project the given table by excluding attributes specified
in exclue_attrs. It will then apply 'MinMax' scaling method to scale each feature vector
in the projected table. In the end, the fitted scaler and the table with transformed
feature vectors will be returned.

Please refer to the API reference of :py:meth:`~py_entitymatching.scale_features`
for more details.