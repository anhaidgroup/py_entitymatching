#!/bin/bash

# Test the package from source install.

source activate py_entitymatching_test_env

# Unit tests
pip install nosetests
python setup.py build_ext --inplace
nosetests -s

# Test install
cd ~
pwd
python -c "import py_entitymatching; print(py_entitymatching.__version__)"
