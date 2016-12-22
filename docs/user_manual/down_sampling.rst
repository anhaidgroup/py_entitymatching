=============
Down Sampling
=============
Recall that we have loaded two tables in the memory. If the these tables are large (e.g.
each having 100K+ tuples), we may have to sample smaller tables A' and B' from A and B
and use them to come up with an EM workflow. This is because working with large tables can
be very time consuming as any operation performed would have to process these large
tables.

In *py_entitymatching*, the user can use `down_sample` to sample smaller tables from
the input tables. An example of using `down_sample` is shown below:

    >>> sample_A, sample_B = em.down_sample(A, B, 500, 1)

.. note:: Currently, the input tables must be loaded in memory before the user can down
 sample.

.. note:: For the purposes of this manual, we assume that we use the input tables A and
 B for the matching purposes.


Please look at the API reference of py:meth:`~py_entitymatching.down_sample` for more
details.