===============================
Commands Implemented Using Dask
===============================

Downsampling
------------
.. autofunction:: py_entitymatching.dask.dask_down_sample.dask_down_sample


Blocking
--------
.. autoclass:: py_entitymatching.dask.dask_attr_equiv_blocker.DaskAttrEquivalenceBlocker
    :members:

.. autoclass:: py_entitymatching.dask.dask_overlap_blocker.DaskOverlapBlocker
    :members:

.. autoclass:: py_entitymatching.dask.dask_rule_based_blocker.DaskRuleBasedBlocker
    :members:

.. autoclass:: py_entitymatching.dask.dask_black_box_blocker.DaskBlackBoxBlocker
    :members:

Extracting Feature Vectors
--------------------------
.. autofunction:: py_entitymatching.dask.dask_extract_features.dask_extract_feature_vecs

ML-Matchers
-----------
.. autoclass:: py_entitymatching.dask.dask_dtmatcher.DaskDTMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.dask.dask_rfmatcher.DaskRFMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.dask.dask_svm_matcherDaskSVMMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.dask.dask_nbmatcher.DaskNBMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.dask.dask_logregmatcher.DaskLogRegMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__


.. autoclass:: py_entitymatching.dask.dask_xgboost_matcher.DaskXGBoostMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__