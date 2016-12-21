===============
Data Structures
===============

A note on conventions: We will often use

* A and B to refer to the original two tables to be matched,
* C to refer to the candidate set table obtained from A and B after the blocking step,
* S to refer to a sample taken from C, and
* G to refer to a table that contains the tuple pairs in S and a golden label for each
  pair (indicating the pair as matches).


Pandas Dataframes as Tables
---------------------------
We will need to store a lot of data as tables in Magellan. We use Pandas dataframes to
represent tables.

Catalog to Store Metadata
-------------------------
In Magellan, we need to store a lot of metadata with a table.  Here
are a few examples:

* Each table in Magellan should have a key, so that we can easily identify the tuples.
Keys are also critical later for debugging, provenance, etc. Key is a metadata that we
want to store for a table.

* The blocking step will create tuple pairs from two tables A and B. For example,
suppose we have table A(aid, a, b) and table B(bid, x, y), then the tuple pairs can be
stored in a candidate set table C(cid, aid, bid, a, b, x, y). This table could be very
big, taking up a lot of space in memory. To save space, we may want to just store C as
C(cid, aid, bid) and then have pointers back to tables A and B. The two pointers back
to A and B are metadata that we may want to store for table C. Specifically, the
metadata for C would be 'aid and bid in table C are foreign keys to tables A and B'.

There are many other examples of metadata that we may want to store for a table. Though
Pandas dataframes is a good choice for storing data as tables, it does not provide a
consistent way to store metadata (for more discussion on this, look at this thread).
To tackle this, we have a new data structure: catalog to store the metadata for tables.
Catalog is a dictionary, where the key is the id of a dataframe (know more id here) and
the value is a dictionary containing metadata. The metadata dictionary can have
different kinds of keys that point to metadata. Examples of such keys are:

* key: the name of the key attribute of the table.
* ltable: pointer to the left table (see below).
* rtable: pointer to the right table (see below).

The kind of metadata stored for a table would depend on the table itself. For example,
the input tables may typically have 'key' as the only metadata (of course, the user can
add more metadata for a table depending on the application need).

.. note:: Each table in Magellan is expected to have a key. We assume the key is just a
single attribute (i.e composite keys are not allowed).

But, if we consider table C (which is obtained by performing blocking on input tables A
and B), this table can be very large, so we typically represent it using a view over
two tables A and B. Such a table C will have the following attributes:
*  _id (key attribute of table C)
* ltable.aid (aid is the key attribute in table A)
* rtable.bid (bid is the key attribute in table B)
* some attributes from A and B

The metadata dictionary for table C in the catalog, will have at least these fields:

* key: _id
* ltable: points to table A.
* rtable: points to table B.
* fk_ltable: ltable.aid (that is, ltable.aid is a foreign key of table A)
* fk_rtable: rtable.bid.

Blockers
--------
Given two table A and B, we have to perform blocking to reduce the number of tuple
pairs considered for matching. In Magellan, Blockers are the objects that help user
perform blocking. Implementation wise, a Blocker is defined as a Python
class with certain methods (and some common utility functions) and all the concrete
blockers inherit from this Blocker class and override the methods. Specifically, each
concrete blocker will implement at least the following methods:

+ block_tables (apply to input tables A and B)
+ block_candset (apply to an output from another blocker (e.g. table C))
+ block_tuples (apply to a tuple pair to check if it will survive blocking)

In Magellan, there are four concrete blockers implemented: (1) attribute equivalence
blocker, (2) overlap blocker, (3) rule-based blocker, and (4) black box blocker.

The class diagram of Blocker and the concrete blockers inherited from it is shown below:

[fig]



Matchers
--------
After performing blocking, we have to perform matching to predict each tuple pair as a
match or not a match. In Magellan, Matchers are the objects that help user perform
matching. Currently only machine learning (ML) matchers are supported in
Magellan. Implementation wise, a Matcher is defined as a Python class
with certain methods (and some common utility functions) and all concrete blockers
inherit from this Matcher class and override the methods. Specifically, each concrete
matcher will implement at lease the following methods:

+ fit (for training)
+ predict (for prediction)

In Magellan, there are six concrete ML-matchers implemented: (1) naive bayes, (2)
logistic regression, (3) linear regression, (4) support vector machine, (5) decision
trees, and (6) random forest.

These concrete matchers are just wrappers of scikit-learn matchers and this is because
the fit/predict methods in scikit-learn are not metadata aware. The concrete matchers
make the scikit-learn matchers metadata aware.

The class diagram of Matchers and the concrete matchers inherited from it is shown below:

[fig]




Summary
-------
* Tables in Magellan are represented as Pandas dataframes.
* The metadata for tables are stored in a separate data strucuture: catalog
* The kind of metadata stored will depend on the table (input table: key, table from
  blocking (say): key, ltable, rtable, fk_table, fk_rtable).
* So there are five reserved keywords for metadata: key, ltable, rtable, fk_ltable,
  fk_rtable.
* There are objects for blockers and matchers.





