======
Guides
======

The goal of this page  is to give you some concrete examples for using py_entitymatching.
These are examples with sample data that is already bundled along with the package. The
examples are in the form of Jupyter notebooks.

A Quick Tour of Jupyter Notebook
--------------------------------
`This tutorial <https://jupyter-notebook-beginner-guide.readthedocs.io/en/latest/index.html>`_
gives a quick tour on installing and using Jupyter notebook.

End-to-End EM Workflows
-----------------------
* EM workflow with blocking using a overlap blocker and matching using Random Forest
  matcher: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/end_to_end_em_guides/Basic%20EM%20Workflow%20Restaurants%20-%201.ipynb>`_
* EM workflow with blocking using a overlap blocker, selecting among multiple matchers,
  using the selected matcher to predict matches, and evaluating the predicted matches: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/end_to_end_em_guides/Basic%20EM%20Workflow%20Restaurants%20-%202.ipynb>`_

* EM workflow with blocking using multiple blockers (overlap and attribute equivalence
  blocker), debugging the blocker output, selecting among multiple matchers, debugging the
  matcher output, using the selected matcher to predict matches, and evaluating the
  predicted matches: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/end_to_end_em_guides/Basic%20EM%20Workflow%20Restaurants%20-%203.ipynb>`_

Stepwise Guides
---------------
* Reading CSV files from disk: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Reading%20CSV%20Files%20from%20Disk.ipynb>`_

* Down sampling: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Down%20Sampling.ipynb>`_

* Data profiling: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Data%20Profiling.ipynb>`_

* Data exploration: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Data%20Exploration.ipynb>`_


* Blocking:

    * Using overlap blocker: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Performing%20Blocking%20Using%20Built-In%20Blockers%20%28Overlap%20Blocker%29.ipynb>`_

    * Using attribute equivalence blocker: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Performing%20Blocking%20Using%20Built-In%20Blockers%20%28Attr.%20Equivalence%20Blocker%29.ipynb>`_

    * Using rule-based blocker: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Performing%20Blocking%20Using%20Rule-Based%20Blocking.ipynb>`_

    * Using blackbox blocker: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Performing%20Blocking%20Using%20Blackbox%20Blocker.ipynb>`_

    * Combining multiple blockers: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Combining%20Multiple%20Blockers.ipynb>`_

    * Debugging blocker output: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Debugging%20Blocker%20Output.ipynb>`_

* Handling features:

    * Generating features manually: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.1.x/notebooks/guides/step_wise_em_guides/Generating%20Features%20Manually.ipynb>`_

    * Editing attribute types and generating features manually: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Editing%20and%20Generating%20Features%20Manually.ipynb>`_

    * Adding features to feature table: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Adding%20Features%20to%20Feature%20Table.ipynb>`_

    * Removing features from feature table: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.1.x/notebooks/guides/step_wise_em_guides/Removing%20Features%20From%20Feature%20Table.ipynb>`_

* Sampling and labeling: `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Sampling%20and%20Labeling.ipynb>`_

* Matching:

    * Selecting the best learning-based matcher (involves splitting the labeled data, generating features,
      instantiating multiple matchers, debugging the matcher output): `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Selecting%20the%20Best%20Learning%20Matcher.ipynb>`_

    * Evaluating the predictions from a matcher:  `Jupyter notebook <https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.2.x/notebooks/guides/step_wise_em_guides/Evaluating%20the%20Selected%20Matcher.ipynb>`_




