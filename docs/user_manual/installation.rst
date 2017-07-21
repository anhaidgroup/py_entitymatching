============
Installation
============

Requirements
------------
* Python 2.7 or Python 3.5+

Platforms
---------
py_entitymatching has been tested on Linux (Redhat enterprise Linux with 2.6
.32 kernel), OS X (Sierra), and Windows 10.

Dependencies
------------
* pandas (provides data structures to store and manage tables)
* scikit-learn (provides implementations for common machine learning algorithms)
* xgboost (provides an implementation for xgboost classifier)
* joblib (provides multiprocessing capabilities)
* pyqt4 (provides tools to build GUIs)
* py_stringsimjoin (provides implementations for string similarity joins)
* py_stringmatching (provides a set of string tokenizers and string similarity functions)
* cloudpickle (provides functions to serialize Python constructs)
* pyprind (library to display progress indicators)
* pyparsing (library to parse strings)
* six (provides functions to write compatible code across Python 2 and 3)

py_entitymatching can be installed using source distribution, as described below.
 


Installing from Source Distribution
-----------------------------------
Clone the py_entitymatching package from GitHub and check out the *RIT_features* branch
like this::

    git clone -b RIT_features https://github.com/anhaidgroup/py_entitymatching.git

Then,  execute the following commands from the package root::

    pip install -U numpy scipy
    python setup.py install

which installs py_stringmatching into the default Python directory on your machine. If you do not have installation permission for that directory then you can install the package in your
home directory as follows::

        python setup.py install --user

For more information see this StackOverflow `link <http://stackoverflow.com/questions/14179941/how-to-install-python-packages-without-root-privileges>`_.

The above commands will install py_entitymatching and all of its
dependencies, except PyQt5 and xgboost.

This is  because, similar to pip,
setup.py can only install the dependency packages that are available in PyPI and PyQt5
xgboost are not in PyPI for Python 2.

* You can install PyQt5, using the instructions on `this page <http://pyqt.sourceforge.net/Docs/PyQt5/installation.html>`_.

* You can install xgboost using the instructions on `this page <https://xgboost.readthedocs.io/en/latest/build.html>`_.

