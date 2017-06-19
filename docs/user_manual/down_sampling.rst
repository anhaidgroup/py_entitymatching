=============
Down Sampling
=============
Once the tables to be matched are read, they must be down sampled if the number of
tuples in them are large (for example, 100K+ tuples). This is because working with
large tables can be very time consuming (as any operation performed would have
to process these large tables).

Random sampling however does not work, because the sampled may end up sharing very
few matches, especially if the number of matches between the
input tables are small to begin with.

In py_entitymatching, you can use sample the input tables using `down_sample` command.
This command samples the input tables intelligently that ensures a reasonable number of
matches between them.

If `A` and `B` are the input tables, then you can use `down_sample` command as shown
below:

>>> sample_A, sample_B = em.down_sample(A, B, size=500, y_param=1)

Conceptually, the command takes in two original input tables, `A`, `B` (and some parameters),
and produces two sampled tables, `sample_A` and `sample_B`.
Specifically, you must set the `size` to be the number of tuples that
should be sampled from `B` (this will be the size of `sample_B` table) and set the
`y_param` to be the number of tuples to be selected from `A` (for each tuple in
`sample_B` table). The command internally uses a
heuristic to ensure a reasonable number of matches between `sample_A` and `sample_B`.

Please look at the API reference of :py:meth:`~py_entitymatching.down_sample` for more
details.

.. note:: Currently, the input tables must be loaded in memory before the user can down
 sample.


