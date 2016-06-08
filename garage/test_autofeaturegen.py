# coding=utf-8
import magellan as mg

A = mg.load_dataset('table_A', key='ID')
B = mg.load_dataset('table_B', key='ID')
F = mg.get_features_for_blocking(A, B)
print(F)

