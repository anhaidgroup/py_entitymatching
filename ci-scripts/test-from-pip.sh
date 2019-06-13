#!/bin/bash

# Test the package from pip install.

set -e

source activate py_entitymatching_test_env

# Test install (no unit tests here)
cd ~
pwd
python -c "import py_entitymatching; print(py_entitymatching.__version__)"
