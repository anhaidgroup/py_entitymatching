.. _label-create-feats-matching:

==============================
Creating Features for Matching
==============================
If you have to use supervised learning-based matchers, then you cannot just operate on the
labeled set of tuple pairs. For each tuple in the labeled, you need to convert it
into a feature vector which consists of a list of numerical/categorical features. To do
this, first we need to create a set of features.

There are two ways to create features:

* Automatically create a set of features (then the user can remove or add some more).
* Skip the automatic process and generate features manually.


Creating the Features Manually
------------------------------
This is very similar to manual feature creation process for blocking (see section
:ref:`label-create-features-blocking`) except the features are created for
matching purposes.
In brief, you can execute the following sequence of commands in py_entitymatching
to create the features manually:

    >>> match_t = em.get_tokenizers_for_matching()
    >>> match_s = em.get_sim_funs_for_matching()
    >>> atypes1 = em.get_attr_types(A) # don't need, if atypes1 exists from blocking step
    >>> atypes2 = em.get_attr_types(B) # don't need, if atypes2 exists from blocking step
    >>> match_c = em.get_attr_corres(A, B)
    >>> match_f = em.get_features(A, B, atypes1, atype2, match_c, match_t, match_s)

Further, you can add or delete features as see saw in section
:ref:`label-add-remove-features`.

Please refer to the API reference of :py:meth:`~py_entitymatching.get_tokenizers_for_matching`
and :py:meth:`py_entitymatching.get_sim_funs_for_matching` for more details.

.. note:: Currently, py_entitymatching returns the same set of features for blocking and matching purposes.

Creating the Features Automatically
-----------------------------------
If you do not want to go through the hassle of creating the features manually, then
the user can generate the features automatically. This is very similar to automatic
feature creation process for blocking (see section :ref:`label-gen-feats-automatically`).

In py_entitymatching, you can use `get_features_for_matching` to generate features
for matching purposes automatically. An example of using `get_features_for_matching` is
shown below:

    >>> match_f = em.get_features_for_matching(A, B)

Similar to what we saw in section :ref:`label-gen-feats-automatically` for blocking, the
command will set the following variables: `_match_t`, `_match_s`, `_atypes1`, `_atypes2`, `_match_c`
and they can be accessed like this:

    >>> em._match_t
    >>> em._match_s
    >>> em._atypes1
    >>> em._atypes2
    >>> em._match_c

You can to examine these variables, modify them as appropriate, and then
perhaps regenerate a set of features.
Please refer to the API reference of :py:meth:`~py_entitymatching.get_features_for_matching`
for more details.





