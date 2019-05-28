================
Data Exploration
================

Data exploration is an important part of the entity matching workflow because it
gives the user a chance to look at the actual data closely. Data exploration
allows the user to inspect the individual records and features present in the
table so that he or she can understand the important trends and relationships
present in the data. A complete understanding of the data gives the user an
advantage later on in the entity matching workflow.


OpenRefine
----------

OpenRefine is a data exploration tool that is compatible with Python >= 2.7 or
Python >= 3.4. More information about OpenRefine can be found at its github page
at https://github.com/OpenRefine/OpenRefine


.. note::
    OpenRefine is not included with py_entitymatching and must be downloaded and
    installed separately. The installation instructions can be found at
    https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions

Using OpenRefine
~~~~~~~~~~~~~~~~

Before using OpenRefine, you must start the application to start an OpenRefine
server. The explanations for doing so are explained after the installation
instructions at https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions

Once the application has created a server, copy the URL from the address bar of
the OpenRefine browser (default is http://127.0.0.1:3333 ). Then the data can
be explored as in the example below:


    >>> import py_entitymatching as em
    >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')
    >>> p = em.data_explore_openrefine(A, name='Table')
    >>> # Save the project back to our dataframe
    >>> # Calling export_pandas_frame will automatically delete the OpenRefine project
    >>> df = p.export_pandas_frame()


Pandastable
-----------
Pandastable is a data exploration tool available for python >=3.4 that allows users
to view and manipulate data. More information about pandastable can be found at
https://github.com/dmnfarrell/pandastable

.. note::
    pandastable is not packaged along with py_entitymatching. You can install
    pandastable using pip as show below:

        $ pip install pandastable

    or conda as shown below:

        $ conda install -c dmnfarrell pandastable=0.7.1



Using pandastable
~~~~~~~~~~~~~~~~~


Pandastable can be easily be used with the wrappers included with py_entitymatching.
The following example shows how:

    >>> # import py_entitymatching
    >>> import py_entitymatching as em
    >>> # Explore the data using pandastable
    >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')
    >>> em.data_explore_pandastable(A)
