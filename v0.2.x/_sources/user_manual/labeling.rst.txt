========
Labeling
========
The command `label_table` can be used to label the samples (see section
:ref:`label-sampling`). An example of using `label_table` is shown below:

    >>> G = em.label_table(S, label_column_name='gold_labels')

The above command will first create a copy of the input table `S`, update
the metadata, add a column with the
specified column name (in `label_col_name` parameter) fill it with 0 (i.e non-matches)
and open a GUI for you to update the labels. You must specify 0 for non-matches and
1 for matches. Once you close the GUI, the updated table will be returned.

Please refer to the API reference of :py:meth:`~py_entitymatching.label_table`
for more details.


New tool for labeling (experimental)
------------------------------------

WARNING: This labeler is still in pre-alpha stage! Use at your own risk!

A new command `new_label_table` has been added to label the samples. This new
labeler is currently in pre-alpha stage and is still incomplete. Use at your
own risk. An example use is shown below:

    >>> G = em.new_label_table(S, label_column_name='gold_labels')

The new labeler completes the same task as `label_table` in that it will take
an input table `S` with pairs of tuples and create a copy table `G` with
additional label, comment, and tags columns. The command will open a GUI that
allows the user to label each pair of tuples with with either 'Yes', 'No', or
'Not-Sure'.

Please refer to the API reference of :py:meth:`~py_entitymatching.new_label_table`
for more details
