=====================================================
Splitting Labeled Data into Training and Testing Sets
=====================================================
While doing entity matching you will have to split data for
multiple purposes. Some examples are:

1. Split labeled data into development and test. Th development
set is used to come up with right features for learning-based matcher, and
`test` set is used to evaluate the matcher.

2. Split feature vectors into a train and test set. The train
set is used to train the learning-based matcher and test set is used
for evaluation.


py_entitymatching provides `split_train_test` command for the above need.
An example of using `split_train_test` is shown below:

    >>> train_test = em.split_train_test(G, train_proportion=0.5)

In the above, `split_train_test` returns a dictionary with two keys: train, and test.
The value for the key `train` is a Dataframe containing tuples
allocated from the input table based on train_proportion.
Similarly, the value for the key `test` is a Dataframe containing
tuples for evaluation. An example of getting train and test Dataframes from the output
of `split_train_test` command is shown below:


    >>> devel_set = train_test['train']
    >>> eval_set = train_test['test']

Setting the value for train proportion would depend on the
context of its use. For instance, if the data is split for machine learning
purposes then train proportion is typically larger than the
test.
The most commonly used values of train_proportion are between
0.5 and 0.8.

Please refer to the API reference of :py:meth:`~py_entitymatching.split_train_test` for
more details.

