import magellan as mg
import six
import os
from magellan.utils.generic_helper import get_install_path
import magellan.catalog.catalog_manager as cm
from magellan.io.parsers import read_csv_metadata
from magellan.blockercombiner.blockercombiner import combine_blocker_outputs_via_union

datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets'])
bc_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'blockercombiner'])
path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([bc_datasets_path, 'C.csv'])
path_c1 = os.sep.join([bc_datasets_path, 'C1.csv'])
path_c2 = os.sep.join([bc_datasets_path, 'C2.csv'])
path_c3 = os.sep.join([bc_datasets_path, 'C3.csv'])

# A = read_csv_metadata(path_a)
# B = read_csv_metadata(path_b, key='ID')
# C1 = read_csv_metadata(path_c1, ltable=A, rtable=B)
# C2 = read_csv_metadata(path_c2, ltable=A, rtable=B)
# C3 = read_csv_metadata(path_c3, ltable=A, rtable=B)
# C = combine_blocker_outputs_via_union([C1, C2, C3])
# # print(C.head(5))
# C1 = read_csv_metadata(path_c, ltable=A, rtable=B)
# C1.sort_values(['ltable_ID', 'rtable_ID'], inplace=True)
# C1['_id'] = six.moves.range(0, len(C1))
# C1.reset_index(inplace=True, drop=True)
# ne = (C == C1)
# # print(ne)
# print(C['_id'])
# print(C1['_id'])
# # print(C1.head(5))


A = read_csv_metadata(path_a)
B = read_csv_metadata(path_b, key='ID')
C1 = read_csv_metadata(os.sep.join([bc_datasets_path, 'C3_ex_2.csv']), ltable=A, rtable=B)
C = combine_blocker_outputs_via_union([C1, C1])
print(C.head(5))
print(C1.head(5))

