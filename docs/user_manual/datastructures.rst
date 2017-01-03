===============
Data Structures
===============
In *py_entitymatching*, we will need to store a lot of tables and metadata associated
with it. It is important for the user to know the data structures that are used to store
the tables and the metadata, so that the user can manipulate them based on his/her need.



As a convention, we will use:

* A and B to refer to the original two tables to be matched,
* C to refer to the candidate set table obtained from A and B after the blocking step,
* S to refer to a sample taken from C, and
* G to refer to a table that contains the tuple pairs in S and a golden label for each
  pair (indicating the pair as matches or non-matches).


Store Tables Using Pandas Dataframes
------------------------------------
We will need to store a lot of data as tables in *py_entitymatching*. We use pandas Dataframes to
represent tables (you can know more about pandas and pandas Dataframes `here
<http://pandas.pydata.org/>`_).

**Tuple:** Each row in a Dataframe is a tuple and this is of type pandas Series.

Store Metadata Using a Catalog
------------------------------

**Bare Minimum that the User Should Know:**
In *py_entitymatching*, we need to store a lot of metadata with a table. We use a
new data structure, Catalog, to store metadata. The user need not worry
about intantiating this object (it gets automatically intantiated when *py_entitymatching*
gets loaded in Python memory) or manipulating this object directly.

All the *py_entitymatching* commands correctly handle the metadata in the Catalog,
and for the user, there are commands to manipulate the Catalog (please see
:ref:`label-handling-metadata` section for the supported commands).


**If You Want to Understand More:**
As we mentioned earlier,  we need to store a lot of metadata with a table. Here are a few examples:

* Each table in *py_entitymatching* should have a key, so that we can easily identify the tuples.
  Keys are also critical later for debugging, provenance, etc. Key is a metadata that we
  want to store for a table.

* The blocking step will create tuple pairs from two tables A and B. For example,
  suppose we have table A(aid, a, b) and table B(bid, x, y), then the tuple pairs can be
  stored in a candidate set table C(cid, aid, bid, a, b, x, y). This table could be very
  big, taking up a lot of space in memory. To save space, we may want to just store C as
  C(cid, aid, bid) and then have pointers back to tables A and B. The two pointers back
  to A and B are metadata that we may want to store for table C. Specifically, the
  metadata for C would be aid and bid in table C are foreign keys to tables A and B.

There are many other examples of metadata that we may want to store for a table. Though
pandas Dataframes is a good choice for storing data as tables, it does not provide a
robust way to store metadata (for more discussion on this topic, please look at `this thread <https://github.com/pandas-dev/pandas/issues/2485>`_).
To tackle this, we have a new data structure, `Catalog` to store the metadata for tables.
As mentioned earlier, the user need not worry
about intantiating this object (it gets automatically intantiated when *py_entitymatching*
gets loaded in Python memory) or manipulating this object directly. All the *py_entitymatching*
commands correctly handle the metadata in the Catalog, and for the user, there
are commands to manipulate the Catalog.

Conceptually, Catalog is a dictionary, where the key is the unique identifier of a Dataframe and the
value is a dictionary containing metadata.
The metadata dictionary can have different kinds of keys that point to metadata.
Examples of such keys are:

* key: the name of the key attribute of the table.
* ltable: pointer to the left table (see below).
* rtable: pointer to the right table (see below).

The kind of metadata stored for a table would depend on the table itself. For example,
the input tables may typically have key as the only metadata (of course, the user can
add more metadata for a table depending on the application need).



But, if we consider table C (which is obtained by performing blocking on input tables A
and B), this table can be very large, so we typically represent it using a view over
two tables A and B. Such a table C will have the following attributes:

*  _id (key attribute of table C).
* ltable_aid (aid is the key attribute in table A).
* rtable_bid (bid is the key attribute in table B).
* some attributes from A and B.

The metadata dictionary for table C in the Catalog, will have at least these fields:

* key: _id.
* ltable: points to table A.
* rtable: points to table B.
* fk_ltable: ltable_aid (that is, ltable.aid is a foreign key of table A).
* fk_rtable: rtable_bid.


Summary
-------
* Tables in *py_entitymatching* are represented as pandas Dataframes.
* The metadata for tables are stored in a separate data structure called Catalog.
* The kind of metadata stored will depend on the table (input table: key, table from
  blocking (say): key, ltable, rtable, fk_table, fk_rtable).
* So there are five reserved keywords for metadata: key, ltable, rtable, fk_ltable,
  fk_rtable.





