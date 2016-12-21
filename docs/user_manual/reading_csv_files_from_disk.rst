===============================
Reading the CSV Files from Disk
===============================

Currently, *py_entitymatching* supports reading CSV files from disk. We assume that the
original tables A and B (to be matched) are stored as CSV files on disk.

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

*Example 2. The two tables mentioned in the above example can be read into
py_entitymathcing as follows:*
::
    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')

