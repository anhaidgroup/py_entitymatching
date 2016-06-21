#!/bin/bash
for i in `seq 1 100`;
do
  nosetests -w magellan/tests/multicore
done
