==================================
Overview of Supported EM Processes
==================================

In this section we provide a high-level overview of the EM processes supported by py_entitymatching. For more details, please read the document "How-To Guide to Entity Matching" (will soon be available from the package website). 

Supported EM Problem Scenarios
------------------------------

Entity matching (EM) has many problem variations: matching two tables, matching within a single table, matching from a table into a knowledge base, etc. The package currently only support matching two tables. Specifically, given two tables A and B of relational tuples, find all tuple pairs (a in A, b in B) such that a and b refer to the same real-world entity. The following figure shows an example of matching persons: Given the two tables in (a) and (b), find all tuple pairs across the two tables that refer to the same real-world person; these pairs are called *matches* and are shown in (c). 

.. image:: match-two-tables-example.png
	:scale: 100
    :alt: 'An example of matching two tables'
    

Of course, if you want to match tuples within a single table X, you can also use the package, by matching X with X (you do not have to create another copy of X, just provide X twice as the input if a command in the package requires two tables A and B as the input). 

Two Fundamental Steps in the EM Process: Blocking and Matching
--------------------------------------------------------------

In practice, tables A and B can be quite large, such as having 100K tuples each, resulting in 10 billions tuple pairs across A and B. Trying to match all of these pairs is clearly very expensive. Thus, in such cases the user often employs domain heuristics to quickly remove obviously non-matched pairs, in a step called *blocking*, before matching the remaining pairs, in a step called *matching*. 

The following figure illustrates the above two fundamental steps. Suppose that we are matching the two tables A and B in (a), where each tuple describes a person. The blocking step can use a heuristic such as "if two tuples do not agree on state, then they cannot refer to the same person" to quickly remove all such tuple pairs. In other words, the blocking step retains only the four tuple pairs that agree on state, as shown in (b). The matching step in (c) then considers only these tuple pairs and predicts for each of them a label "match" or "not-match" (shown as "+" and "-" in the figure). 

.. image:: blocking-matching-example.png
	:scale: 100
    :alt: 'An example of blocking and matching'
    
Supported EM Workflows
----------------------

The current package supports EM workflows that consist of a blocking step followed by a matching step. Specifically, the package provides a set of blockers and a set of matchers (and the user can easily write his or her own blocker/matcher). Given two tables A and B to be matched, the user applies a blocker to the two tables 

    

