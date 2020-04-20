#!/bin/bash

# Install this package and its dependencies from source.

set -e

source activate py_entitymatching_test_env

# System dependencies
which gcc

# Packages required at dependency build time (patch)
pip install numpy==1.16.2
pip install scikit-learn==0.20

# Package dependencies
pip install -r requirements.txt

# Build dependencies
pip install Cython

# Install package
python setup.py install
