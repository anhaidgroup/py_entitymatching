# coding=utf-8

# A = mg.load_dataset('table_A', key='ID')
# B = mg.load_dataset('table_B', key='ID')
#
# ab = mg.AttrEquivalenceBlocker()
# C = ab.block_tables(A, B,  'zipcode', 'zipcode', l_output_attrs=['name', 'zipcode', 'birth_year'],
#                     r_output_attrs=['name', 'zipcode', 'birth_year'])
#
#
# mg.to_csv_metadata(C, './C.csv')
# print 'Hi'
import logging
import os
import pandas as pd

from magellan.io.parsers import read_csv_metadata
from magellan.utils.generic_helper import get_install_path
import magellan.catalog.catalog_manager as cm

logging.basicConfig()

io_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'io'])
# path_a = os.sep.join([io_datasets_path, 'A.csv'])
path_b = os.sep.join([io_datasets_path, 'B.csv'])
# path_c = os.sep.join([io_datasets_path, 'C.csv'])
#
# A = read_csv_metadata(path_a)
# B = read_csv_metadata(path_b)
#
# C = mg.read_csv_metadata(path_c, ltable=A, rtable=B)
#
# cm.show_properties(C)

p_C = os.sep.join([io_datasets_path, 'C.csv'])
p_A = os.sep.join([io_datasets_path, 'A_fk1.csv'])
A = read_csv_metadata(p_A)
cm.set_property(A, 'key', 'ID')
B = read_csv_metadata(path_b, key='ID')
C = read_csv_metadata(p_C, ltable=A, rtable=B)
cm.show_properties(C)


