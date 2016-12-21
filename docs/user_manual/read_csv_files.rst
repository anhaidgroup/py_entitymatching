===============================
Reading the CSV Files from Disk
===============================
Currently, *py_entitymatching* supports reading CSV files from disk.

**The Minimal That You Should Do:** The original tables A an B (to be matched) must be
stored as CSV files on disk.

*Example 1. Suppose we have stored the two tables to be matched as table_A.csv and
table_B.csv on disk, then table_A.csv may look like this:*
::

    ID, name, birth_year, hourly_wage, zipcode
    a1, Kevin Smith, 1989, 40, 94107
    a2, Michael Franklin, 1988, 27.5, 94122
    a3, William Bridge, 1988, 32, 94121

In the above CSV file, `ID` is the key attribute. To read these files into memory as
Pandas dataframes and specify `ID` is the key attribute, use the
:py:meth:`~py_entitymatching.read_csv_metadata` command.

.. NOTE::
   Each table in *py_entitymatching* is expected to have a key. We assume the key is
   just a single attribute (i.e composite keys are not allowed).

*Example 2. The two tables mentioned in the above example can be read into
py_entitymathcing as follows:*
::
    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')

**If You Want to Understand and Play Around More:** In general,
:py:meth:`~py_entitymatching.read_csv_metadata` looks for file with `.csv` extension
replaced with `.metadata` in the same directory containing the metadata. For example,
if you read `table_A.csv` then :py:meth:`~py_entitymatching.read_csv_metadata` looks for
`table_A.metadata` file. The contents of `table_A.metadata` may look like this:
::
    #key=ID

In the above, you can specify the key attribute of `table_A` and call
:py:meth:`~py_entitymatching.read_csv_metadata` with out providing any metadata as
parameters. The method will automatically read the metadata for the table from the file
and update appropriately.

*Example 2. The two tables mentioned in the above example along with metadata file
stored in the same directory can be read into *py_entitymatching* as follows:*

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv')

See :py:meth:`~py_entitymatching.read_csv_metadata` for more details.

Once, the table is read into *py_entitymatching*, the user can check to see which
attribute of the table is a key using :py:meth:`~py_entitymatching.get_key` command as
shown below:


    >>> em.get_key(A)
       'ID'


