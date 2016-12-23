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

The output table S, will be updated with the metadata as in table C.

For more details, please look into the API reference of :py:meth:`~py_entitymatching.sample_table`