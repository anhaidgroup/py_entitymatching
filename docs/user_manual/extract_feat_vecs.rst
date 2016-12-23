==========================
Extracting Feature Vectors
==========================
Once we have a set of features, we can use them to convert labeled sample (`G`) to feature
vectors (`H`). In *py_entitymatching*, the user can use `extract_feature_vecs` to convert
`G` to `H` using the features created (see section :ref:`label-create-feats-matching`).

An example of using `extract_feature_vecs` is shown below:

    >>> H = em.extract_feature_vecs(G, features=match_f, attrs_before=['title'], attrs_after=['gold_labels'])

If `match_f` contains features `f1`, `f2`, and `f3` then H will have:

* Key attribute, this is the same `id` attribute as in G
* Keys of table A and B pointed to by H (this is same as in G)
* Attributes listed in `attrs_before` (this is a optional parameter)
* Feature values based on `f1`, `f2`, and `f3`
* Attributes listed in `attrs_after` (this is a optional parameter)

If there is one (or several columns) in `G` that contains the labels, then those need
to be explicitly specified in `attrs_after`, if the user wants them to copy over.

Note that given C (i.e. the candidate set table), we could convert it to feature vectors
like this:

    >>> D = em.extract_feature_vecs(C, features=match_f, attrs_before=['ltable_title'])

.. note:: In the above, the `attrs_before` and `attrs_after` can include attributes only from table C.
