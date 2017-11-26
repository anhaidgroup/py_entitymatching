.. _label-create-features-blocking:

==============================
Creating Features for Blocking
==============================
Recall that when doing blocking, you can use built-in blockers,
blackbox blockers, or rule-based blockers. For rule-based blockers,
you have to create a set of features. While creating features, you will have to
refer to tokenizers, similarity functions, and attributes of the tables.
Currently, in py_entitymatching, there are two ways to create features:

* Automatically generate a set of features (then you can remove or add some more).
* Skip the automatic process and generate features manually.


Note that features will also be used in the matching process, as we
will discuss later.

.. The set of features for blocking and the set of features for matching can be quite different however. For example, for blocking we may only want to have features that are inexpensive to compute.

If you are interested in just letting the system to automatically
generate a set of features, then please see :ref:`label-gen-feats-automatically`.

If you want to generate features on your own, please read below.

Available Tokenizers and Similarity Functions
---------------------------------------------
A tokenizer is a function that takes a string and optionally a number
of other arguments, then tokenizes the string and returns a set of tokens.
Currently, the following tokenizers are provided along with *py_entitytmatching*:

* Alphabetic
* Alphanumeric
* White space
* Delimiter based
* Qgram based


A similarity function takes two arguments (can be strings, numeric values, etc.),
which are typically two attribute values such
as two book titles, then returns an output value which is typically a similarity score
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
First you need to get tokenizers and similarity functions to refer them in features.
In py_entitymatching, you can use
`get_tokenizers_for_blocking` to get all the tokenizers available for blocking purposes.

    >>> block_t = em.get_tokenizers_for_blocking()

In the above, `block_t` is a dictionary where keys are tokenizer names
and values are tokenizer functions in Python. You can inspect `block_t` and delete/add
tokenizers as appropriate. The above command will return single-argument tokenizers,
i.e., those that take a string then produce a set of tokens.

Each of the keys of the default dictionary returned to 'block_t' by
'get_tokenizers_for_blocking' represent a tokenizer that can be used by similarity
functions. The keys and the respective tokenizer they represent are shown below:

* alphabetic: Alphabetic tokenizer
* alphanumeric: Alphanumeric tokenizer
* dlm_dc0: Delimiter tokenizer using spaces as the delimiter
* qgm_2: Two Gram tokenizer
* qgm_3: Three Gram tokenizer
* wspace: Whitespace tokenizer


Please look at the API reference of :py:meth:`~py_entitymatching.get_tokenizers_for_blocking`
for more details.

Similarly, the user can use `get_sim_funs_for_blocking` to get all the similarity
functions available for blocking purposes.

    >>> block_s = em.get_sim_funs_for_blocking()

In the above, `block_s` is a dictionary where keys are similarity function names
and values are similarity functions in Python. Similar to `block_t`, you can
inspect `block_s` and delete/add similarity functions as appropriate.

Each of the keys of the default dictionary returned to 'block_s' by
'get_sim_funs_for_blocking' represent a similarity function. The keys and the
respective similarity function they represent are shown below:

* abs_norm: Absolute Norm
* affine: Affine Transformation
* cosine: Cosine Similarity
* dice: Dice similarity Coefficient
* exact_match: Exact Match
* hamming_dist: Hamming Distance
* hamming_sim: Hamming Similarity
* jaccard: Jaccard Similarity
* jaro: Jaro Distance
* jaro_winkler: Jaro-Winkler Distance
* lev_dist: Levenshtein Distance
* lev_sim: Levenshtein Similarity
* monge_elkan: Monge-Elkan Algorithm
* needleman_wunsch: Needleman-Wunsch Algorithm
* overlap_coeff: Overlap Coefficient
* rel_diff: Relative Difference
* smith_waterman: Smith-Waterman Algorithm



Please look at the API reference of :py:meth:`~py_entitymatching.get_sim_funs_for_blocking`
for more details.


Obtaining Attribute Types and Correspondences
---------------------------------------------
In the next step, you need to obtain type and correspondence information about A and B
so that the features can be generated.

First, you need to obtain the types of attributes in A and B,
so that the right tokenizers/similarity functions can be applied to each of them.
In py_entitymatching, you can use `get_attr_types` to get the attribute types.
An example of using `get_attr_types` is shown below:

    >>> atypes1 = em.get_attr_types(A)
    >>> atypes2 = em.get_attr_types(B)

In the above, `atypes1` and `atypes2` are dictionaries. They contain, the type of
attribute in each of the tables. Note that this `type` is different from basic
Python types. Please look at the API reference of
:py:meth:`~py_entitymatching.get_attr_types` for more details.

Next, we need to obtain correspondences between the attributes of A and B,
so that the features can be generated based on those correspondences.
In py_entitymatching, you can use `get_attr_corres` to get the attribute
correspondences.

An example of using `get_attr_corres` is shown below:

    >>> block_c = em.get_attr_corres(A, B)

In the above, `block_c` is a dictionary containing attribute correspondences.
Currently, py_entitymatching returns attribute correspondences only based on the exact
match of attribute names. You can inspect `block_c` and modify the attribute
correspondences. Please look at the API reference of
:py:meth:`~py_entitymatching.get_attr_corres` for more details.

.. _label-get-a-set-of-features-manual:

Getting a Set of Features
-------------------------
Recall that so far we have obtained:

+ block_t, the set of tokenizers,
+ block_s, the set of sim functions
+ atypes1 and atypes2, the types of attributes in A and B
+ block_c, the correspondences of attributes in A and B

Next, to obtain a set of features, you can use `get_features` command.
An example of using `get_features` command is shown below:

    >>> block_f = em.get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)

Briefly, this function will go through the correspondences. For each
correspondence `m`, it examines the types of the involved attributes,
then apply the appropriate tokenizers and similarity functions to generate
all appropriate features for this correspondence. The features are returned as
a Dataframe. Please look at the API reference of
:py:meth:`~py_entitymatching.get_features` for more details.


.. _label-add-remove-features:

Adding/Removing Features
------------------------
Given the set of features `block_f` as a pandas Dataframe, you can delete certain features,
add new features.

Deletion of a feature is straightforward, all that you have to do is delete the row
from the feature table corresponding to the feature. You can use `drop` command
from pandas Dataframe for this purpose. Please look at this
`API reference link <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop.html>`_
for more details.


There are two ways to create and add a feature: (1) write a blackbox function and
add it to feature table, and (2) define a feature declartively and add it to
feature table.


**Adding a Blackbox Function as Feature**

To create and add a blackbox function as a feature, first you must define it. Specifically,
the function must take in two tuples as input and return a numeric value. An example of
a blackbox function is shown below:

::

    def age_diff(ltuple, rtuple):
        # assume that the tuples have age attribute and values are valid numbers.
        return ltuple['age'] - rtuple['age']

Then add it to the feature table `block_f` using `add_blackbox_feature` like this:

    >>> status = em.add_blackbox_feature(block_f, 'age_difference', age_diff)

Please look at the API reference of
:py:meth:`~py_entitymatching.add_blackbox_feature` for more details.

**Adding a Feature Declaratively**

Another way to add features is to write a feature expression in
a `declarative` way. py_entitymatching will then compile it into a feature. For
example, you can declaratively create and add a feature like this:

    >>> r = em.get_feature_fn('jaccard(qgm_3(ltuple["name"]), qgm_3(rtuple["name"]))', block_t, block_s)
    >>> em.add_feature(block_f, 'name_name_jac_qgm3_qgm3', r)

Here `block_t` and `block_s` refer to the dictionaries containing a set of
tokenizers and similarity functions for blocking. Additionally, 'jaccard' refers
to the key in 'block_s' that represents the Jaccard Similarity function and
'qgm_3' refers to the key in 'block_t' that represents a three gram tokenizer.
The keys in 'block_t' and 'block_s' and which function or tokenizer they
represent are explained above in the Obtaining Tokenizers and Similarity Functions
section.

Conceptually, the first command, `get_feature_fn`, creates a feature which is a Python function
that will take two tuples `ltuple` and `rtuple`, get the attribute publisher from `ltuple`,
issuer from `rtuple`, tokenize them, then compute jaccard score.

.. note:: The feature must refer the tuple from the left table (say A) as **ltuple** and
 the tuple from the right table (say B) as **rtuple**.

The second command, `add_feature` tags the feature with the specified name,
and adds it to the feature table.

As described, the feature that was just created is *independent* of any table
(eg A and B). Instead, it expects as the input two tuples: ltuple and rtuple.


You can also create more complex features. Specifically,
you are allowed to define arbitrary complex expression involving function names from
`block_t` and `block_s`, and attribute names from ltuple and rtuple.

    >>> r = em.get_feature_fn('jaccard(qgm_3(ltuple.address + ltuple.zipcode), qgm_3(rtuple.address + rtuple.zipcode)',block_t,block_s)
    >>> em.add_feature(block_f, 'full_address_address_jac_qgm3_qgm3', r)


You can also create your own similarity functions and tokenizers for your custom features.
For example, you can create a similarity function that changes all strings to lowercase
before checking if they are equivalent.

    >>> # This similarity function converts the two strings to lowercase before checking if they are an exact match
    >>> def match_lowercase(l_attr, r_attr):
    >>>     l_attr = l_attr.lower()
    >>>     r_attr = r_attr.lower()
    >>>     if l_attr == r_attr:
    >>>         return 1
    >>>     else:
    >>>         return 0

You can then add a feature declarativly with your new similarity function.

    >>> # The new similarity function is added to block_s and then a new feature is created
    >>> block_t = em.get_tokenizers_for_blocking()
    >>> block_s = em.get_sim_funs_for_blocking()
    >>> block_s['match_lowercase'] = match_lowercase
    >>> r = em.get_feature_fn('match_lowercase(ltuple["name"], rtuple["name"])', block_t, block_s)
    >>> em.add_feature(block_f, 'name_name_match_lowercase', r)

It is also possible to create features with your own similarity functions that require
tokenizers. The next example shows how to create a custom tokenizer that returns only
the first and last words of a string.

    >>> # This custom tokenizer returns the first and last words of a string
    >>> def first_last_tok(attr):
    >>>     all_toks = attr.split(" ")
    >>>     toks = [all_toks[0], all_toks[len(all_toks) - 1]]
    >>>     return toks

Next, a similarity function that can utilize the new tokenizer is created. This example
shows how to create a similarity function that raises the score if the first words match
and raises the score by one if the second words match.

    >>> # This similarity function compares two tokens from each set.
    >>> # Greater weight is placed on the equality of the first token.
    >>> def first_last_sim(l_attr, r_attr):
    >>>     score = 0
    >>>     if l_attr[0] == r_attr[0]:
    >>>         score += 2
    >>>     if l_attr[1] == r_attr[1]:
    >>>         score +=1
    >>>     return score

Finally, with the tokenizer and similarity functions defined, the new feature can be
created and added.

    >>> # The new tokenizer is added to block_t and the new similarity function is added to block_s
    >>> # then a new feature is created
    >>> block_t = em.get_tokenizers_for_blocking()
    >>> block_t['first_last_tok'] = first_last_tok
    >>> block_s = em.get_sim_funs_for_blocking()
    >>> block_s['first_last_sim'] = first_last_sim
    >>> r = em.get_feature_fn('first_last_sim(first_last_tok(ltuple["name"]), first_last_tok(rtuple["name"]))',
    >>>                  block_t, block_s)
    >>> em.add_feature(block_f, 'name_name_fls_flt_flt', r)

Please look at the API reference of
:py:meth:`~py_entitymatching.get_feature_fn` and :py:meth:`~py_entitymatching.add_feature`
for more details.

Summary of the Manual Feature Generation Process
------------------------------------------------
Here is the summary of commands for the entire manual feature generation process.

To generate features, you must execute the following commands:

    >>> block_t = em.get_tokenizers_for_blocking()
    >>> block_s = em.get_sim_funs_for_blocking()
    >>> atypes1 = em.get_attr_types(A)
    >>> atypes2 = em.get_attr_types(B)
    >>> block_c = em.get_attr_corres(A, B)
    >>> block_f = em.get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)

The variable `block_f` points to a Dataframe containing features as rows.


Ways to Edit the Manual Feature Generation Process
--------------------------------------------------
Here is the summary of ways to edit the variables used in feature generation process.

* The `block_t`, `block_s`, `atypes1`, `atypes2`, `block_c` are dictionaries. You
  can modify these variables based on your need, to add/remove tokenizers,
  similarity functions, attribute correspondences, etc.

* `block_f` is a Dataframe. You can remove a feature by just deleting the corresponding
  tuple from the Dataframe.

* There are two ways to create and add a feature: (1) write a blackbox function and
  add it to feature table, and (2) define the feature declartively and add it to
  feature table.
  To add a blackbox feature, first write a blackbox function like this:
  ::

    def age_diff(ltuple, rtuple):
        # assume that the tuples have age attribute and values are valid numbers.
        return ltuple['age'] - rtuple['age']

  Then add it to the table `block_f` using `add_blackbox_feature` like this:

        >>> status = em.add_blackbox_feature(block_f, 'age_difference', age_diff)

  To add a feature declaratively, first write a feature expression and compile it to feature
  using `get_feature_fn` like this:

        >>> r = em.get_feature_fn('jaccard(qgm_3(ltuple.address + ltuple.zipcode), qgm_3(rtuple.address + rtuple.zipcode)',block_t,block_s)

  Then add it to the table `block_f` using `add_feature` like this:

        >>> em.add_feature(block_f, 'full_address_address_jac_qgm3_qgm3', r)

.. _label-gen-feats-automatically:

Generating Features Automatically
---------------------------------
Recall that to get the features for blocking, eventually you
must execute the following:

    >>> block_f = em.get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)

where `atypes1`/`atypes2` are the attribute types of A and B, `block_c` is
the correspondences between their attributes, `block_t` is the set of tokenizers,
and `block_s` is the set of similarity functions.

If you don't want to go through the hassle of creating these intermediate
variables, then you can execute the following:

    >>> block_f = em.get_features_for_blocking(A,B)

The system will automatically generate a set of features and return it as
as a Dataframe which you can then use for blocking purposes. This Dataframe
contains a few attributes that require further explanation, specifically
'left_attr_tokenizer', 'right_attr_tokenizer', and 'simfunction'. There are
two types of similarity functions, those that use tokenizers and those that
do not. Some similarity functions use tokenizers and all such features must
designate a tokenizer for both the left table attribute in
'left_attr_tokenizer' and for the right table attribute in
'right_attr_tokenizer'. The 'simfunction' attribute refers to the name of
the function and comes from the keys in 'block_s'. The various keys and the
actual functions they correspond to are explained in the Obtaining
Tokenizers and Similarity Functions section above.

The command `get_features_for_blocking` will set the following variables: `_block_t`,
`_block_s`, `_atypes1`, `_atypes2`, and `_block_c`. You can access these variables like this:

    >>> em._block_t
    >>> em._block_s
    >>> em._atypes1
    >>> em._atypes2
    >>> em._block_c

You can examine these variables, modify them as appropriate, and
then perhaps re-generate the set of features using `get_features` command.

Please look at the API reference of
:py:meth:`~py_entitymatching.get_features_for_blocking` for more details.
