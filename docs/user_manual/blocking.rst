===========================================
Specifying Blockers and Performing Blocking
===========================================

Types of Blockers and Blocker Hierarchy
---------------------------------------
After the tables are loaded and downsampled, the user often has to do blocking.
Note that by *blocking* we mean to block a *tuple pair* from going through to the
matching step. When applied to a tuple pair, a blocker returns *true* if the pair
should be blocked.

There are two types of blockers: (1) tuple-level, and (2) global. A tuple-level blocker
can examine a tuple pair in isolation and decide if it should be admitted to the next
stage. For example, an attribute equivalence blocker is a tuple-level blocker. A global
blocker cannot make this decision in isolation. It would need to examine a set of other
pairs as well. For example, sorted neighborhood blocker applied over union of A and B
is a global blocker. Currently, *py_entitymatching* supports only tuple-level blockers.

The blockers can be combined in complex ways, such as

* apply blocker *b1* to the two tables
* apply blocker *b2* to the two tables
* apply blocker *b3* to the output of *b2*

Further, the user may also want to apply a blocker to just a pair of tuples, to see how
the blocker works.

In *py_entitymatching*, there is a Blocker class from which a set of concrete blockers
are inherited. These concrete blockers implement the following methods:

  + block_tables (apply to input tables A and B)
  + block_candset (apply to an output from another blocker (e.g. table C))
  + block_tuples (apply to a tuple pair to check if it will survive blocking)

In *py_entitymatching*, there are four concrete blockers implemented: (1) attribute
equivalence blocker, (2) overlap blocker, (3) rule-based blocker, and (4) black box
blocker.

The class diagram of Blocker and the concrete blockers inherited from it is shown below:

Built-In Blockers
-----------------
Built-in blockers are those that have been built into *py_entitymatching* and the user
can just simply call them. *py_entitymatching* currently offers two built-in blockers.

**Attribute Equivalence Blocker**

The function signature for `block_tables` is shown below:
::

    block_tables(ltable, rtable, l_block_attr, r_block_attr, l_output_attrs=None,
    r_output_attrs=None, l_output_prefix='ltable_', r_output_prefix='rtable_',
    allow_missing=False, verbose=False, n_jobs=1)

Conceptually, this will check `l_block_attr=r_block_attr` for each tuple
pair from the Cartesian product of tables `ltable` and `rtable`. It outputs a
Pandas dataframe object with tuple pairs that satisfy the equality condition.
The dataframe will include attributes '_id', key attribute from
ltable, key attributes from rtable, followed by lists `l_output_attrs` and
`r_output_attrs` if they are specified. Each of these output and key attributes will be
prefixed with given `l_output_prefix` and `r_output_prefix`. If `allow_missing` is set
to `True` then all tuple pairs with missing value in at least one of the tuples will be
included in the output dataframe.
Further, this will update the following metadata in the catalog for the output table:
(1) key, (2) ltable, (3) rtable, (4) fk_ltable, and (5) fk_rtable.

An example of using the above function is shown below:

.. code-block:: python

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata(em.get_install_path()+'/datasets/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata(em.get_install_path()+'/datasets/table_B.csv', key='ID')
    >>> ab = em.AttrEquivalenceBlocker()
    >>> C = ab.block_tables(A, B, 'zipcode', 'zipcode', l_output_attrs=['name','age'],r_output_attrs=['name', 'age'])

The package is bundled with a few sample datasets (e.g. table_A.csv, table_B.csv). They
can be accessed using the `em.get_install_path` as shown above.

Please look at :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_tables` for
more details.

The function `block_candset` is similar to `block_tables` except `block_candset` is
applied on the output from `block_tables`. An example of using `block_candset` is shown
below:
::
    >>> D = ab.block_candset(C, 'age', 'age')

Please look at :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_candset` for
more details.

Further, `block_tuples` can be used to check if a tuple pair would get blocked. An
example of using `block_candset` is shown below:
::
    >>> status = ab.block_tuples(A.ix[0], B.ix[0])
    >>> status
        True

Please look at :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_tuples` for
more details.
