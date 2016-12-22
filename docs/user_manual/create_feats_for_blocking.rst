==============================
Creating Features for Blocking
==============================
Recall that when doing blocking, the user can use built-in blockers,
blackbox blockers, or rule-based blockers. For rule-based blockers,
the user often has to create a set of features.

In creating features, user will have to refer to tokenizers, similarity functions, and
attributes of the tables. Currently, in *py_entitymatching, there are two ways
to create features:
+ *py_entitytmatching* can automatically generate a set of features,
then the user can remove or add some more.
+ user can skip the automatic process and generate features of their own.

Note that features will also be used in the matching process, as we
will discuss later. The set of features for blocking and the set of
features for matching can be quite different however. For example,
for blocking we may only want to have features that are inexpensive
to compute.
Lay users may just want to ask the system to automatically
generate a set of features. To do this, please see **Section ...** For
users who want to generate features of their own, please read below.

Available Tokenizers and Similarity Functions
---------------------------------------------
A tokenizer is a function that takes a string and optionally a number
of other arguments, then tokenize the string and return an output
value which is often a set of tokens. Currently, the following tokenizers
are provided along with *py_entitytmatching*:

* Alphabetic
* Alphanumeric
* White space
* Delimiter based
* Qgram based


A similarity function takes two arguments, which are typically two attribute values such
as two book titles, then return an output value which is typically a similarity score
between the two attribute values. Currently, the following similarity functions
are provided along with *py_entitytmatching*:

* Affine
* Hamming distance
* Jaro
* Jaro-Winkler
* Levenshtein
* Monge-Elkan
* Needleman-Wunsch
* Smith-Waterman
* Jaccard
* Cosine
* Dice
* Overlap coefficient
* Exact match
* Absolute norm


Obtaining Tokenizers and Similarity Functions
---------------------------------------------
In *py_entitymatching*, the user can use
`get_tokenizers_for_blocking` to get all the tokenizers available for blocking purposes.

    >>> block_t = get_tokenizers_for_blocking()

In the above, `block_t` will be a dictionary where keys are tokenizer names
and values are tokenizer functions in Python. User can inspect `block_t` and delete/add
tokenizers as appropriate. The above command will return single-argument tokenizers,
i.e., those that take a string then produce a set of tokens.

The reason we want to obtain single-argument tokenizers is because eventually we want
to write expressions such as:
::

    jacccard(qgram3(x.title), qgram3(y.title)).

Here the tokenizer will have just a single argument. So if we have tokenizers
that take multiple arguments, then we want to generate all templates
from them that have just a single argument, to facilitate writing
expressions such as above.

Please look at the API reference of py:meth:`~py_entitymatching.get_tokenizers_for_blocking`
for more details.

Similarly, the user can use `get_sim_funs_for_blocking` to get all the similarity
functions available for blocking purposes.

    >>> block_s = get_sim_funs_for_blocking()

In the above, `block_t` will be a dictionary where keys are similarity function names
and values are similarity functions in Python. Similar to `block_t`, the user can
inspect `block_s` and delete/add similarity functions as appropriate.

Please look at the API reference of py:meth:`~py_entitymatching.get_sim_funs_for_blocking`
for more details.


Obtaining Attribute Types and Correspondences
---------------------------------------------
In the next step, we need to obtain some information about A and B
so that we can generate features.

To start, we want to obtain the types of attributes in A and B,
so that we can apply the right tokenizers/similarity functions to each of them.
In *py_entitymatching*, the user can use `get_attr_types` to get the attribute types.
An example of using `get_attr_types` is shown below:

    >>> atypes1 = em.get_attr_types(A)
    >>> atypes2 = em.get_attr_types(B)

In the above, `atypes1` and `atypes2` are dictionaries. They contain, the type of
attribute in each of the tables. Note that this `type` is different from basic
Python types. Please look at the API reference of
py:meth:`~py_entitymatching.get_attr_types` for more details.

Next, we need to obtain correspondences between the attributes of A and B,
so that we can generate features based on those. In *py_entitymatching*, the user can
use `get_attr_types` to get the attribute types. An example of using `get_attr_types`
is shown below:

    >>> block_c = em.get_attr_corres(A, B)

In the above, `block_c` is a dictionary containing attribute correspondences.
Currently, *py_entitymatching* returns attribute correspondences based on the exact
match of attribute names. The user can inspect `block_c` and modify the attribute
correspondences. Please look at the API reference of
py:meth:`~py_entitymatching.get_attr_corres` for more details.

Getting a Set of Features
-------------------------
Recall that so far we have obtained:

+ block_t, the set of tokenizers,
+ block_s, the set of sim functions
+ atypes1 and atypes2: types of attributes in A and B
+ block_c: correspondences of attributes in A and B

To obtain a set of features, we can use `get_features` command.

    >>> block_f = get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)

Briefly, this function will go through the correspondences. For each
correspondence `m`, it examines the types of the involved attributes,
then apply the appropriate tokenizers and similarity functions to generate
all appropriate features for this correspondence. The features are returned as
a dataframe. Please look at the API reference of
py:meth:`~py_entitymatching.get_features` for more details.

Adding/Removing Features
------------------------
Given the set of features block_f, user can delete certain features,
add new features.

One way to add features is to write blackbox features **(see Section ...)** for more
details.

Another way to add features is to write a feature expression in
a `declarative` way. *py_entitymatching* will then compile it into a feature. For
example, user can do something like this:

    >>> r = get_feature_fn('jaccard(qgm_3(ltuple.name), qgm_3(rtuple.name)', block_t, block_s)
    >>> em.add_feature(block_f, 'name_name_jac_qgm3_qgm3', r)

Here `block_t` and `block_s` refer to the set of tokenizers and similarity functions
for blocking, respectively.

The first command creates a feature which is a function that will take
two tuples `ltuple` and `rtuple`, get the attribute publisher from `ltuple`,
issuer from `rtuple`, tokenize them, then compute jaccard score.

.. note:: The feature must refer the tuple from the left table (say A) as **ltuple** and
 the tuple from the right table (say B) as **rtuple**.

The second command creates a feature with a particular name,
supplying the above function as the feature code.
As described, the feature that was just created is *independent* of any table
(eg A and B). Instead, it expects as the input two tuples ltuple and rtuple.

User can create more complex features. For example,

    >>> r = get_feature_fn('jaccard(qgm_3(ltuple.address + ltuple.zipcode), qgm_3(rtuple.address + rtuple.zipcode)',block_t,block_s)
    >>> em.add_feature(block_f, 'full_address_address_jac_qgm3_qgm3', r)

The user is allow to define arbitrary complex expression involving function names from
`block_t` and `block_s`, and attribute names from ltuple and rtuple.

Please look at the API reference of
py:meth:`~py_entitymatching.get_feature_fn` and py:meth:`~py_entitymatching.add_feature`
for more details.

Generating Features Automatically
---------------------------------
Recall that to get the features for blocking, eventually user
must execute the following:

    >>> block_f = get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)

where atypes1/atypes2 are the attribute types of A and B, block_c is
the correspondences between their attributes, block_t is the set of tokenizers,
and block_s is the set of similarity functions.

If a user doesn’t want to go through the hassle of creating these intermediate
variables, then the user can execute the following:

    >>> block_f = get_features_for_blocking(A,B)

The system will automatically generate a set of features (stored in block_f) that user
can then use for blocking purposes.

The command `get_features_for_blocking` will set the following variables: _block_t,
_block_s, _atypes1, _atypes2, and _block_c. The user can access these variables like this:

    >>> em._block_t
    >>> em._block_s
    >>> em._atypes1
    >>> em._atypes2
    >>> em._block_c

The user can examine these variables, modify them as appropriate, and
then perhaps re-generate the set of features.

Please look at the API reference of
py:meth:`~py_entitymatching.get_features_for_blocking` for more details.
