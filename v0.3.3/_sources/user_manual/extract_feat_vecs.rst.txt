==========================
Extracting Feature Vectors
==========================
Once you have created a set of features, you use them to convert labeled sample to feature
vectors. In py_entitymatching, you can use `extract_feature_vecs` to convert
labeled sample to feature vectors using the features created
(see section :ref:`label-create-feats-matching`).

An example of using `extract_feature_vecs` is shown below:

    >>> H = em.extract_feature_vecs(G, feature_table=match_f, attrs_before=['title'], attrs_after=['gold_labels'])

Conceptually, the command takes the labeled data (`G`), applies the feature functions (in `match_f`)
to each tuple in G to create a Dataframe, adds the `attrs_before` and `attrs_after`
columns, updates the metadata and returns the resulting Dataframe.

If there is one (or several columns) in labeled data that contains the labels, then those need
to be explicitly specified in `attrs_after`, if you want them them to copy over.

Please refer to the API reference of :py:meth:`~py_entitymatching.extract_feature_vecs`
for more details.
