============
Installation
============

Requirements
------------
* Python 2.7 or Python 3.4+

Platforms
---------
py_entitymatching has been tested on Linux (Redhat enterprise Linux with 2.6
.32 kernel), OS X (Sierra), and Windows 10.

Dependencies
------------
* pandas (provides data structures to store and manage tables)
* scikit-learn (provides implementations for common machine learning algorithms)
* joblib (provides multiprocessing capabilities)
* pyqt5 (provides tools to build GUIs)
* py_stringsimjoin (provides implementations for string similarity joins)
* py_stringmatching (provides a set of string tokenizers and string similarity functions)
* cloudpickle (provides functions to serialize Python constructs)
* pyprind (library to display progress indicators)
* pyparsing (library to parse strings)
* six (provides functions to write compatible code across Python 2 and 3)
* xgboost (provides an implementation for xgboost classifier)
* pandas-profiling (provides implementation for profiling pandas dataframe)
* pandas-table (provides data exploration tool for pandas dataframe)
* openrefine (provides data exploration tool for tables)



Installing Using conda
----------------------
The easiest and recommended way to install the package is to use the command conda,
which will retrieve py_entitymatching from Anaconda repository then install it::

    conda install -c uwmagellan py_entitymatching

The above command will install py_entitymatching and all of its dependencies except
XGBoost, pandastable and openrefine. As these packages are either not available from
anaconda cloud (for some Python versions or operating systems) or they are not Python
packages (openrefine).

* To install XGBoost manually from the source follow the instructions on `this page <https://XGBoost.readthedocs.io/en/latest/build.html>`_.

* To install pandastable follow the instructions at `this page <https://github.com/dmnfarrell/pandastable>`_.

* To install openrefine follow the instructions at `this page <https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions>`_.


.. note::
    To use the command conda, first you must install Miniconda or Anaconda. For
    more details refer to this `conda page <http://conda.pydata.org/docs/using/index.html>`_.


Installing Using pip
--------------------
To install the package using pip, execute the following
command::

    pip install -U numpy scipy py_entitymatching


The above command will install py_entitymatching and all of its dependencies except
XGBoost, pandastable, openrefine, and PyQt5. This is because pip can only install the
dependency packages that are available in PyPI and PyQt5, XGBoost, pandastable are not
in PyPI for Python 2.


* To install PyQt5, follow the instructions at `this page <http://pyqt.sourceforge.net/Docs/PyQt5/installation.html>`_.

* To install XGBoost, follow the instructions at `this page <https://XGBoost.readthedocs.io/en/latest/build.html>`_.

* To install pandastable follow the instructions at `this page <https://github.com/dmnfarrell/pandastable>`_.

* To install openrefine follow the instructions at `this page <https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions>`_.



Installing from Source Distribution
-----------------------------------
Clone the py_entitymatching package from GitHub

    git clone  https://github.com/anhaidgroup/py_entitymatching.git

Then,  execute the following commands from the package root::

    pip install -U numpy scipy
    python setup.py install

which installs py_stringmatching into the default Python directory on your machine. If you do not have installation permission for that directory then you can install the package in your
home directory as follows::

        python setup.py install --user

For more information see this StackOverflow `link <http://stackoverflow.com/questions/14179941/how-to-install-python-packages-without-root-privileges>`_.

The above commands will install py_entitymatching and all of its
dependencies, except PyQt5 and XGBoost.

This is  because, similar to pip, setup.py can only install the dependency packages 
that are available in PyPI and PyQt5, pandastable, XGBoost are not in PyPI for Python 2.

* To install PyQt5, follow the instructions at `this page <http://pyqt.sourceforge.net/Docs/PyQt5/installation.html>`_.

* To install XGBoost, follow the instructions at `this page <https://XGBoost.readthedocs.io/en/latest/build.html>`_.

* To install pandastable follow the instructions at `this page <https://github.com/dmnfarrell/pandastable>`_.

* To install openrefine follow the instructions at `this page <https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions>`_.

