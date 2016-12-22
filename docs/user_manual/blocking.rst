===========================================
Specifying Blockers and Performing Blocking
===========================================

Types of Blockers and Blocker Hierarchy
---------------------------------------
After the tables are loaded and downsampled, the user often has to do blocking.
Note that by *blocking* we mean to block a *tuple pair* from going through to the
matching step. When applied to a tuple pair, a blocker returns *true* if the pair
should be blocked.

There are two types of blockers: (1) tuple-level, and (2) global. A tuple-level blocker
can examine a tuple pair in isolation and decide if it should be admitted to the next
stage. For example, an attribute equivalence blocker is a tuple-level blocker. A global
blocker cannot make this decision in isolation. It would need to examine a set of other
pairs as well. For example, sorted neighborhood blocker applied over union of A and B
is a global blocker. Currently, *py_entitymatching* supports only tuple-level blockers.

The blockers can be combined in complex ways, such as

* apply blocker *b1* to the two tables
* apply blocker *b2* to the two tables
* apply blocker *b3* to the output of *b1*

Further, the user may also want to apply a blocker to just a pair of tuples, to see how
the blocker works.

In *py_entitymatching*, there is a Blocker class from which a set of concrete blockers
are inherited. These concrete blockers implement the following methods:

  + block_tables (apply to input tables A and B)
  + block_candset (apply to an output from another blocker (e.g. table C))
  + block_tuples (apply to a tuple pair to check if it will survive blocking)

In *py_entitymatching*, there are four concrete blockers implemented: (1) attribute
equivalence blocker, (2) overlap blocker, (3) rule-based blocker, and (4) black box
blocker.

The class diagram of Blocker and the concrete blockers inherited from it is shown below:

Built-In Blockers
-----------------
Built-in blockers are those that have been built into *py_entitymatching* and the user
can just simply call them. *py_entitymatching* currently offers two built-in blockers.

**Attribute Equivalence Blocker**

Given two tables A and B, conceptually, `block_tables` in attribute equivalence blocker
takes an attribute `x` of table A, an attribute `y` of table B, and returns true (that
is, drop the tuple pair) if `x` and `y` are not of the same value.

An example of using the above function is shown below:

    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata('path_to_csv_dir/datasets/table_B.csv', key='ID')
    >>> ab = em.AttrEquivalenceBlocker()
    >>> C = ab.block_tables(A, B, 'zipcode', 'zipcode', l_output_attrs=['name'], r_output_attrs=['name'])

Please look at the API reference of :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_tables`
for more details.

The function `block_candset` is similar to `block_tables` except `block_candset` is
applied to the candidate set, i.e. the output from `block_tables`. An example of using
`block_candset` is shown below:

    >>> D = ab.block_candset(C, 'age', 'age')

Please look at the API reference of :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_candset`
for more details.

Further, `block_tuples` can be used to check if a tuple pair would get blocked. An
example of using `block_candset` is shown below:

    >>> status = ab.block_tuples(A.ix[0], B.ix[0])
    >>> status
        True

Please look at the API reference of :py:meth:`~py_entitymatching.AttrEquivalenceBlocker.block_tuples`
for more details.

**Overlap Blocker**

Given two tables A and B, conceptually, `block_tables` in overlap blocker takes an
attribute `x` of table A, an attribute `y` of table B, and returns true (that is, drop
the tuple pair) if `x` and `y` do not share any token (where the token can be a word or
a q-gram).

Please look at the API reference of :py:meth:`~py_entitymatching.OverlapBlocker.block_tables`
for more details.

An example of using `block_tables` is shown below:
    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
    >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
    >>> ob = em.OverlapBlocker()
    >>> C = ob.block_tables(A, B, 'zipcode', 'zipcode', num_overlap=1, l_output_attrs=['name'], r_output_attrs=['name'] )

The function `block_candset` is similar to `block_tables` except `block_candset` is
applied to the candidate set, i.e. the output from `block_tables`.

An example of using `block_candset` is shown below:
::
    >>> D = ab.block_candset(C, 'age', 'age')

Please look at the API reference of :py:meth:`~py_entitymatching.OverlapBlocker.block_candset`
for more details.


Further, `block_tuples` can be used to check if a tuple pair would get blocked. An
example of using `block_tuples` is shown below:
::
    >>> status = ob.block_tuples(A.ix[0], B.ix[0], 'name', 'name', num_overlap=1)
    >>> status
        True

Please look at the API reference of :py:meth:`~py_entitymatching.OverlapBlocker.block_tuples`
for more details.

Blackbox Blockers
-----------------
By `blackbox blockers` we mean that the user supplies a Python function which
encodes blocking for a tuple pair. Specifically, the Python function will take
in two tuples and returns True if the tuple pair needs to be blocked else
returns False. To use a blackbox blocker, the user should first write a
blackbox blocker function.
An example of blackbox blocker function is shown below:
::

    def match_last_name(ltuple, rtuple):
        # assume that there is a 'name' attribute in the input tables
        # and each value in it has two words
        l_last_name = ltuple['name'].split()[1]
        r_last_name = rtuple['name'].split()[1]
        if l_last_name != r_last_name:
            return True
        else:
            return False

Then instantiate a `blackbox blocker` and set the blocking function function as follows:
::
    >>> import py_entitymatching as em
    >>> bb = em.BlackBoxBlocker()
    >>> bb.set_black_box_function(match_last_name)

Now, the user can call `block_tables` on the input tables. Conceptually, `block_tables` would
apply the blackbox blocker function on the Cartesian product of the input tables A and B and
return a candidate set of tuple pairs.

An example of using `block_tables` is shown below:
::
    >>> C = bb.block_tables(A, B, l_output_attrs=['name'], r_output_attrs=['name'] )

Please look at the API reference of :py:meth:`~py_entitymatching.BlackBoxBlocker.block_tables`
for more details.

The function `block_candset` is similar to `block_tables` except `block_candset` is
applied to the candidate set, i.e. the output from `block_tables`.

An example of using `block_candset` is shown below:

    >>> D = bb.block_candset(C)

Please look at the API reference of :py:meth:`~py_entitymatching.BlackBoxBlocker.block_candset`
for more details.

Further, `block_tuples` can be used to check if a tuple pair would get blocked. An
example of using `block_tuples` is shown below:

    >>> status = bb.block_tuples(A.ix[0], B.ix[0])
    >>> status
        True

Please look at the API reference of :py:meth:`~py_entitymatching.BlackBoxBlocker.block_tuples`
for more details.

Rule-Based Blockers
-------------------
A user can write a few domain specific rules (for blocking purposes) using `rule-based blocker`.
If a user wants to write rules, then he/she must start by defining a set of features.
Each `feature` will be a function that when applied to a tuple pair will return a
numeric value. We will discuss how to define a set of features in **section []**.
Once defined, *py_entitymatching* stores this set of features in a feature table. We
refer to this feature table as `block_f`. Then the user may be able to instantiate a
rule-based blocker and add rules like this:
::
    >>> rb = em.RuleBasedBlocker()
    >>> rb.add_rule(rule1, block_f)
    >>> rb.add_rule(rule2, block_f)

In the above, `block_f` is a set of features stored as a dataframe **(see section)**.
Each rule is a list of strings. Each string specifies a conjunction of predictes. Each
`predicate` has three parts: (1) an expression, (2) a comparison operator, and (3) a
value. The expression can be evaluated over a tuple pair, producing a numerical value.
Currently, in *py_entitymatching* an expression is limited to contain a single feature
(being applied to a tuple pair). So a `predicate` may look like this:
::

    name_name_lev(ltuple, rtuple) > 3

In the above `name_name_lev` is feature. Concretely, this feature may compute
Levenshtein distance between the `names` in the input tuple pair.

Now, the rules `rule1` and `rule2` may look like this:
::
    rule1 = ['name_name_lev(ltuple, rtuple) > 3', 'age_age_exact_match(ltuple, rtuple) !=0']
    rule2 = ['address_address_lev(ltuple, rtuple) > 6']

The blocker is then a disjunction of rules. That is, even if one of the rules return
True, then the tuple pair will be blocked.


Now, the user can call `block_tables` on the input tables. Conceptually, `block_tables` would
apply the rule-based blocker function on the Cartesian product of the input tables A and B and
return a candidate set of tuple pairs.

An example of using `block_tables` is shown below:
::
    >>> C = rb.block_tables(A, B, l_output_attrs=['name'], r_output_attrs=['name'] )

Please look at the API reference of :py:meth:`~py_entitymatching.RuleBasedBlocker.block_tables`
for more details.

The function `block_candset` is similar to `block_tables` except `block_candset` is
applied to the candidate set, i.e. the output from `block_tables`.

An example of using `block_candset` is shown below:

    >>> D = rb.block_candset(C)

Please look at the API reference of :py:meth:`~py_entitymatching.RuleBasedBlocker.block_candset`
for more details.

Further, `block_tuples` can be used to check if a tuple pair would get blocked. An
example of using `block_tuples` is shown below:

    >>> status = rb.block_tuples(A.ix[0], B.ix[0])
    >>> status
        True

Please look at the API reference of :py:meth:`~py_entitymatching.RuleBasedBlocker.block_tuples`
for more details.

Combining Multiple Blockers
---------------------------
If the user uses multiple blockers, he/she often has to combine them to get a
consolidated candidate set. There are many different ways to combine the candidate sets
such as doing union, majority vote, weighted vote, etc. Currently, *py_entitymatching*
supports union-based combining.
In *py_entitymatching*, `combine_blocker_outputs_via_union` can be used to do union-based
combining. An example of using `combine_blocker_outputs_via_union` is shown below:

    >>> import py_entitymatching as em
    >>> ab = em.AttrEquivalenceBlocker()
    >>> C = ab.block_tables(A, B, 'zipcode', 'zipcode')
    >>> ob = em.OverlapBlocker()
    >>> D = ob.block_candset(C, 'address', 'address')
    >>> block_f = em.get_features_for_blocking(A, B)
    >>> rb = em.RuleBasedBlocker()
    >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
    >>> rb.add_rule(rule, block_f)
    >>> E = rb.block_tables(A, B)
    >>> F = em.combine_blocker_outputs_via_union([C, E])

Please look at the API reference of :py:meth:`~py_entitymatching.RuleBasedBlocker.block_tuples`
for more details.




