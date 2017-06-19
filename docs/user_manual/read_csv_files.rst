===============================
Reading the CSV Files from Disk
===============================
Currently, py_entitymatching only asupports reading CSV files from disk.

**The Minimal That You Should Do:** First, you must store the input tables as CSV files
in disk. Please look at section :ref:`label-csv-format` to learn more
about CSV format. An example of a CSV file will look like this:

::

    ID, name, birth_year, hourly_wage, zipcode
    a1, Kevin Smith, 1989, 40, 94107
    a2, Michael Franklin, 1988, 27.5, 94122
    a3, William Bridge, 1988, 32, 94121

Next, each table in py_entitymatching must have a key column. If the table already
has a key column, then you can read the CSV file and set the key column as like this:

::

    # ID is the key column in table.csv
    >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')

If the table does not have a key column, then you can read the CSV file, add a
key column and set the added key column like this:

::

    # Read the CSV file
    >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv')
    # Add a key column with name 'ID'
    >>> A['ID'] = range(0, len(A))
    # Set 'ID' as the key column
    >>> em.set_key(A, 'ID')

**If You Want to Read and Play Around More:** In general, the command
:py:meth:`~py_entitymatching.read_csv_metadata` looks for a file (with the same file name
as the `CSV` file) with `.metadata` extension in the same directory containing the
metadata. If the file containing metadata information is not present, then
:py:meth:`~py_entitymatching.read_csv_metadata` will proceed just reading the CSV file
as mentioned in the command.

To update the metadata for a table, using a metadata file, first, you must manually create
this file and specify the metadata for a table and then call
:py:meth:`~py_entitymatching.read_csv_metadata`. The command will automatically read the metadata from the
file and update the Catalog.

For example, if you read `table.csv` then :py:meth:`~py_entitymatching.read_csv_metadata`
looks for `table.metadata` file. The contents of `table.metadata` may look like this:
::

    #key=ID

Each line in the file starts with `#`. The metadata is written as `key=value` pairs,
one in each line. The contents of the above file says that `ID` is the key attribute
(for the table in the file `table.csv`).


The table mentioned in the above example along with the metadata file
stored in the same directory can be read as follows:

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv')

Once, the table is read, you can check to see which
attribute of the table is a key using :py:meth:`~py_entitymatching.get_key` command as
shown below:


    >>> em.get_key(A)
       'ID'

As you see, the key for the table is updated correctly as 'ID'.

See :py:meth:`~py_entitymatching.read_csv_metadata` for more details.
