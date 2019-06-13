#!/bin/bash

# Install this package and its dependencies using pip.

source activate py_entitymatching_test_env

# Package dependencies (TODO: factor this out)
pip install -r requirements.txt

# Install from TestPyPI
pip install -i https://test.pypi.org/simple/ py-entitymatching
