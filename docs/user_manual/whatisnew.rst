What is New?
============

Compared to Version 0.3.3, the followings are new:
  * Dropped support for Python 2 and 3.5.
  * To support Python 3.8, updated the function
    :code:`py_entitymatching.matcher.matcherutils.impute_table()` to use current
    scikit-learn's :code:`SimpleImputer`; see :ref:`Imputing Missing Values` for correct
    usage.
