.. _label-sampling:

========
Sampling
========
If we are to use supervised learning-based matchers or evaluate matchers, we need
labeled data. To get labeled data, we can sample of candidate set pairs and let the
user label them. In *py_stringmatching*, the user can use `sample_table` to get a sample.
The command does uniform random sampling without replacement. An example of using
`sample_table` is shown below:

    >>> S = em.sample_table(C, 100)

The command will first create a copy of the input table, sample the specified number of
tuple pairs from the copy, update the metadata and return the sampled table.


For more details, please look into the API reference of :py:meth:`~py_entitymatching.sample_table`