#!/bin/bash
for i in `seq 1 10`;
do
  nosetests -w magellan/tests/multicore
done
