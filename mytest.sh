#!/bin/bash
for i in `seq 1 1000`;
do
  nosetests -w magellan/tests/multicore
done
