===============
Data Structures
===============
In py_entitymatching, we will need to store many tables and metadata associated
with it. It is important for you to know the data structures that are used to store
the tables and the metadata, so that you can manipulate them based on your need.

As a convention, we will use:

* A and B to refer to the original two tables to be matched,
* C to refer to the candidate set table obtained from A and B after the blocking step,
* S to refer to a sample taken from C, and
* G to refer to a table that contains the tuple pairs in S and a golden label for each
  pair (indicating the pair as matches or non-matches).


Storing Tables Using Pandas Dataframes
--------------------------------------
We will need to store a lot of data as tables in py_entitymatching. We use pandas Dataframes to
represent tables (you can read more about pandas and pandas Dataframes `here
<http://pandas.pydata.org/>`_).

**Tuple:** We often refer to a row of a table as tuple. Each tuple is just a row
in a Dataframe and this is of type pandas Series (you can read more about pandas Series
`here <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.html>`_).


Storing Metadata Using a Catalog
--------------------------------

**Bare Minimum that You Should Know:**
In py_entitymatching, we need to store a lot of metadata with a table such as
key and foreign key. We use a
new data structure, Catalog, to store metadata. You need not worry
about instantiating this object (it gets automatically instantiated when py_entitymatching
gets loaded in Python memory) or manipulating this object directly.

All the py_entitymatching commands correctly handle the metadata in the Catalog,
and for you, there are commands to manipulate the Catalog (please see
:ref:`label-handling-metadata` section for the supported commands).


**If You Want to Read More:**
As we mentioned earlier,  we need to store a lot of metadata with a table. Here are a few examples:

* Each table in py_entitymatching should have a key, so that we can easily identify the tuples.
  Keys are also critical later for debugging, provenance, etc. Key is a metadata that we
  want to store for a table.

* The blocking step will create tuple pairs from two tables A and B. For example,
  suppose we have table A(aid, a, b) and table B(bid, x, y), then the tuple pairs can be
  stored in a candidate set table C(cid, aid, bid, a, b, x, y). This table could be very
  big, taking up a lot of space in memory. To save space, we may want to just store C as
  C(cid, aid, bid) and then have pointers back to tables A and B. The two pointers back
  to A and B are metadata that we may want to store for table C. Specifically, the
  metadata for C include key (`cid`) and foreign keys (`aid`, `bid`) to the base tables
  (`A`, `B`).

There are many other examples of metadata that we may want to store for a table. Though
pandas Dataframes is a good choice for storing data as tables, it does not provide a
robust way to store metadata (for more discussion on this topic, please look at `this thread <https://github.com/pandas-dev/pandas/issues/2485>`_).
To tackle this, we have a new data structure, `Catalog` to store the metadata for tables.

Conceptually, Catalog is a dictionary, where the keys are unique identifiers for
each Dataframe and the values are dictionaries containing metadata.
This dictionary can have different kinds of keys that point to metadata.
Examples of such keys are:

* key: the name of the key attribute of the table.
* ltable: pointer to the left table (see below).
* rtable: pointer to the right table (see below).

The kind of metadata stored for a table would depend on the table itself. For example,
the input tables must have a key and this can be the only metadata.

But, if we consider table C (which is obtained by performing blocking on input tables A
and B), this table can be very large, so we typically represent it using a view over
two tables A and B. Such a table C will have the following attributes:

*  _id (key attribute of table C).
* ltable_aid (aid is the key attribute in table A).
* rtable_bid (bid is the key attribute in table B).
* some attributes from A and B.

The metadata dictionary for table C will have at least these fields:

* key: _id.
* ltable: points to table A.
* rtable: points to table B.
* fk_ltable: ltable_aid (that is, ltable.aid is a foreign key of table A).
* fk_rtable: rtable_bid.


Summary
-------
* Tables in py_entitymatching are represented as pandas Dataframes.
* The metadata for tables are stored in a separate data structure called Catalog.
* The kind of metadata stored will depend on the table (for example input table will have key,
  and the table from blocking will have key, ltable, rtable, fk_table, fk_rtable).
* So there are five reserved keywords for metadata: key, ltable, rtable, fk_ltable,
  fk_rtable. You should not use these names to store metadata for other application
  specific purposes.





