==================================
Overview of Supported EM Processes
==================================

In this section we provide a high-level overview of the EM processes supported by py_entitymatching. For more details, please read the document "How-To Guide to Entity Matching" (will soon be available from the package website). 

Entity matching (EM) has many problem variations: matching two tables, matching within a single table, matching from a table into a knowledge base, etc. The package currently only support matching two tables. Specifically, given two tables A and B of relational tuples, find all tuple pairs (a in A, b in B) such that a and b refer to the same real-world entity. The following figure shows an example of matching persons: Given the two tables in (a) and (b), find all tuple pairs across the two tables that refer to the same real-world person; these pairs are called *matches* and are shown in (c). 

.. image:: match-two-tables-example.png
	:scale: 100
    :alt: 'An example of matching two tables'
    

