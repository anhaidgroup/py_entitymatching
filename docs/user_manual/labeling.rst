========
Labeling
========
The command `label_table` can be used to label the samples (see section
:ref:`label-sampling`). An example of using `label_table` is shown below:

    >>> G = em.label_table(S, label_col_name='gold_labels')

The above command will first create a copy of the input table `S`, update
the metadata, add a column with the
specified column name (in `label_col_name` parameter) fill it with 0 (i.e non-matches)
and open a GUI for the user to update the labels. The user must specify 0 for non-matches and
1 for matches. Once the user closes the GUI, the updated table will be returned.

Please refer to the API reference of :py:meth:`~py_entitymatching.label_table`
for more details.