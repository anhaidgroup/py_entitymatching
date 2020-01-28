#!/bin/bash

# Install this package and its dependencies using pip.

set -e

source activate py_entitymatching_test_env

# Packages required at dependency build time (patch)
pip install numpy==1.16.2
pip install scikit-learn==0.20

# Package dependencies (TODO: factor this out)
pip install -r requirements.txt

# Install from TestPyPI
pip install -i https://test.pypi.org/simple/ py-entitymatching
