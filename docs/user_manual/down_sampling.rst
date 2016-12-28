=============
Down Sampling
=============
Recall that the two tables, A and B (to be matched) are loaded in the memory.
If these tables are large (e.g. each having 100K+ tuples), then they have to sampled to
to produce smaller tables A' and B' (from A and B) and use them to come up with an EM
workflow. This is because working with large tables can be very time consuming
(as any operation performed would have to process these large tables).

Random sampling however does not work, because tables A' and B' may end up sharing very
few matches, i.e., matching tuples (especially if the number of matches between A
and B is small to begin with). *py_entitymatching* provides a command, `down_sample`
that samples more intelligently, to ensure a reasonable number of matches between
A' and B'.

An example of using `down_sample` is shown below:

    >>> sample_A, sample_B = em.down_sample(A, B, 500, 1)

Conceptually, the command takes in two original input tables, `A`, `B` (and some parameters),
and produces two sampled tables, `sample_A` and `sample_B`. The command internally uses a
heuristic to ensure a reasonable number of matches between `sample_A` and `sample_B`.

Please look at the API reference of :py:meth:`~py_entitymatching.down_sample` for more
details.

.. note:: Currently, the input tables must be loaded in memory before the user can down
 sample.

.. note:: For the purposes of this manual, we assume that the user uses the input tables
    A and B for the matching purposes.


