=============
Miscellaneous
=============
This section covers some miscellaneous things in py_entitymatching.

.. _label-csv-format:

CSV Format
----------
The CSV format is selected because it’s well known and can be read by numerous external
programs. Further, it can be easily inspected and edited by the users.
You can read more about CSV formats `here <https://en.wikipedia.org/wiki/Comma-separated_values>`_.

There are two common CSV formats that are used to store CSV files: one with attribute
names in the first line, and one without. Both these formats are supported by py_entitymatching.

An example of a CSV file with attribute names is shown below:
::

    ID, name, birth_year, hourly_wage, zipcode
    a1, Kevin Smith, 1989, 30, 94107
    a2, Michael Franklin, 1988, 27.5, 94122
    a3, William Bridge, 1988, 32, 94321

An example of a CSV file with out attribute names is shown below:

::

    a1, Kevin Smith, 1989, 30, 94107
    a2, Michael Franklin, 1988, 27.5, 94122
    a3, William Bridge, 1988, 32, 94321

Metadata File Format
--------------------
The CSV file can be accompanied with a metadata file containing the metadata information
of the table. Typically, it contains information such as key, foreign key, etc.
The metadata file is expected to be of the same name as the CSV file but with `.metadata`
extension. For example, if the CSV file `table_A.csv` contains table A's data, then
`table_A.metadata` will contain table A's metadata. So, the metadata is
associated based on the names of the files. The metadata file contains key-value pairs
one per line and each line starts with '#'.

An example of metadata file is shown below:

::

    #key=ID

In the above, the pair key=ID states that ID is the key attribute.

Writing a Dataframe to Disk Along With Its Metadata
---------------------------------------------------
To write a Dataframe to disk along with its metadata, you can use `to_csv_metadata`
command in py_entitymatching. An example of using `to_csv_metadata` is shown below:

    >>> em.to_csv_metadata(A, './table_A.csv')

The above command will first write Dataframe pointed by `A` to `table_A.csv` file in the
disk (in CSV format), next it will write the metadata of `table A` stored in the Catalog
to `table_A.metadata` file in the disk.

Please refer to the API reference of :py:meth:`~py_entitymatching.to_csv_metadata` for
more details.

.. note:: Once the Dataframe is written to disk along with metadata, it can read using :py:meth:`~py_entitymatching.read_csv_metadata` command.


Writing/Reading Other Types of py_entitymatching Objects
----------------------------------------------------------
After creating a blocker or feature table, it is desirable to have a
way to persist the objects to disk for future use. py_entitymatching provides
two commands for that purpose: `save_object` and `load_object`.

An example of using `save_object` is shown below:

    >>> block_f = em.get_features_for_blocking(A, B)
    >>> rb = em.RuleBasedBlocker()
    >>> rb.add_rule([name_name_lev(ltuple, rtuple) < 0.4], block_f)
    >>> em.save_object(rb, './rule_based_blocker.pkl')

`load_object` loads the stored object from disk. An example of using `load_object` is
shown below:

    >>> rb = em.load_object('./rule_based_blocker.pkl')

Please refer to the API reference of :py:meth:`~py_entitymatching.save_object` and
:py:meth:`~py_entitymatching.save_object` for more details.
