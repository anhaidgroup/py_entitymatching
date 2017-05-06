=======================
Imputing Missing Values
=======================
While doing supoervised learning-based matching, you would need to create labeled sample,
convert the sample into table of feature vectors, fill in the missing values, select
a machine learning (ML) model and use it to produce matches.

The step of filling in the missing values (also called imputing
missing values) is important and necessary. If there are missing values in the input
tables A and B, then they would be passed on to candidate set and most
likely to the feature vectors. In py_entitymatching, if the feature vectors
contain missing values, then most of the ML algorithms would not work
as they rely on scikit-learn package to provide ML-algorithm
implementations (and their implementations would not work if the
feature vectors contain NaN’s).

To avoid missing value problem in the feature vectors, you must impute the values
of the NaN's. There are many different ways to impute missing values such as
filling the NaN's (in the whole table or just some columns) with a constant value,
or fill the NaN's with an aggregate value (mean, median, etc.).

Since the table is represented as a pandas Dataframe, there are two common ways to impute
missing values: (1) use `fillna` method from pandas Dataframe, and (2) impute missing
values using `Imputer` from Scikit-learn package.

But there are two problems that we have to tackle if we have to using the above commands
or objects directly:

* They are not metadata aware, so the user has to explicitly take care of it.

* The Dataframe type that gets imputed typically contains attributes such as key, foreign
  keys to A and B. The user must have to rightly project them out to impute missing
  values using aggregates.

In py_entitymatching, we propose a hybrid method to impute missing values. To fill NaN's
with a constant value use `fillna` command from pandas Dataframe. Please look at the
`API reference of fillna <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.fillna.html>`_
for more details. An example of using `fillna` to the whole table is shown below:

    >>> H.fillna(value=0, inplace=True)


In the above, `H` is a Dataframe containing feature vectors, 0 is the constant value that
to be filled in, and `inplace=True` means that the updation should be done in place
(i.e., without creating a copy). It is important to set `inplace=True` as we do not want
the metadata for H in Catalog to be corrupted.

Another example of using `fillna` on a column is shown below:

    >>> H['name_name_lev'] = H['name_name_lev'].fillna(value=0, inplace=False)

Note that, in the above `inplace` should be specified as False, this is because
the output is getting assigned to a column in the old Dataframe `H` and the metadata
of `H` does not get affected.

To fill NaN's with an aggregate value, in py_entitymatching you can use `impute_table`
command. It is a wrapper around scikit-learn's `Imputer` object (to make it metadata aware).
An example of using `impute_table` is shown below:

    >>> H = em.impute_table(H, exclude_attrs=['_id', 'ltable_id', 'rtable_id'], strategy='mean')

.. note:: If all the values in a column or a row are NaN's, then the above aggregation
    strategy will not work (i.e. we cannot compute the mean and use it to fill the
    missing values). In such cases, you need to specify a value in `val_all_nans`
    parameter and the command will use this value to fill in all the missing values.

Please refer to the API reference of :py:meth:`~py_entitymatching.impute_table` for
more details.

