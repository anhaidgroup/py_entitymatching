===============================
Reading the CSV Files from Disk
===============================
Currently, *py_entitymatching* supports reading CSV files from disk.

**The Minimal That You Should Do:** The original tables A an B (to be matched) must be
stored as CSV files on disk.

Suppose we have stored the two tables to be matched as table_A.csv and
table_B.csv on disk, then table_A.csv may look like this:
::

    ID, name, birth_year, hourly_wage, zipcode
    a1, Kevin Smith, 1989, 40, 94107
    a2, Michael Franklin, 1988, 27.5, 94122
    a3, William Bridge, 1988, 32, 94121

In the above CSV file, `ID` is the key attribute. To read these files into memory as
pandas Dataframes and specify `ID` is the key attribute, use the
:py:meth:`~py_entitymatching.read_csv_metadata` command.

.. NOTE::
   Each table in *py_entitymatching* is expected to have a key. We assume the key is
   just a single attribute (i.e composite keys are not allowed).

The two tables mentioned in the above example can be read as follows:

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')

**If You Want to Understand and Play Around More:** In general, the command
:py:meth:`~py_entitymatching.read_csv_metadata` looks for a file (with the same file name
as the `CSV` file) with `.metadata` extension in the same directory containing the metadata.
For example, if you read `table_A.csv` then :py:meth:`~py_entitymatching.read_csv_metadata`
looks for `table_A.metadata` file. The contents of `table_A.metadata` may look like this:
::

    #key=ID


Each line in the file starts with `#`. The metadata is written as `key=value` pairs,
one in each line. The contents of the above file says that `ID` is the key attribute
(for the table in the file `table_A.csv`).

The user can manually create this file and specify the metadata for a table. Then the
user can call :py:meth:`~py_entitymatching.read_csv_metadata` with out providing
any metadata as parameters. The command will automatically read the metadata from the
file and update the Catalog.

The two tables mentioned in the above example along with the metadata files
stored in the same directory can be read as follows:

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv')

Once, the table is read, the user can check to see which
attribute of the table is a key using :py:meth:`~py_entitymatching.get_key` command as
shown below:


    >>> em.get_key(A)
       'ID'

See :py:meth:`~py_entitymatching.read_csv_metadata` for more details.
