==================
Debugging Blocking
==================
In a typical entity matching workflow, you will load in the two tables to
match, sample them (if required) and use a blocker to remove obvious non-matches.
But it is often not clear whether the blocker drops only non-matches or it
also removes a lot of potential matches.

In such cases, it is important to debug the output of blocker. In
py_entitymatching, `debug_blocker` command can be used for that purpose.

The `debug_blocker` command takes in two input tables A, B, blocker output C
and returns a table D containing a set of tuple pairs that are
potential matches and yet are not present in the blocker output
C. Table D also contains similarity measure computed for each reported
tuple pair (as its second column).

You can examine these potential matches in table D. If you
find that many of them are indeed true matches, then that means the
blocker may have removed too many true matches. In this case you
may want to `relax` the blocker by modifying its parameters, or
choose a different blocker. On the other hand, if you do not
find many true matches in table D, then it could be the case that the
blocker has done a good job and preserve all the matches (or most of
the matches) in the blocker output C.

In the `debug_blocker`, you can optionally specify attribute correspondences between
the input tables A and B. If it is not specified, then attribute correspondences
will be a list of attribute pairs with the exact same names in A and B.

The debugger will use only the attributes mentioned in these attribute
correspondences to try to find potentially matching pairs and place
those pairs into D. Thus, our recommendation is that (a) if the tables
have idential schemas or share a lot of attributes with the same
names, then do not specify the attribute correspondences, in this
case the debugger will use all the attributes with the same name between the two
schemas, (b) otherwise think about what attribute pairs you want to see the
debugger use, then specify those as attribute correspondences.

An example of using `debug_blocker` is shown below:

    >>> import py_entitymatching as em
    >>> ob = em.OverlapBlocker()
    >>> C = ob.block_tables(A, B, l_overlap_attr='title', r_overlap_attr='title', overlap_size=3)
    >>> corres = [('ID','ssn'), ('name', 'ename'), ('address', 'location'),('zipcode', 'zipcode')]
    >>> D = em.debug_blocker(C, A, B, attr_corres=corres)

Please refer to the API reference of :py:meth:`~py_entitymatching.debug_blocker`
for more details.



