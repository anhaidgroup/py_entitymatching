#!/bin/bash

# Install this package and its dependencies from source.

source activate py_entitymatching_test_env

# System dependencies
if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then conda install --yes gcc; fi
which gcc

# Package dependencies
pip install -r requirements.txt

# Build dependencies
pip install Cython

# Install package
python setup.py install
