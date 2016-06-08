import magellan as mg

A = mg.load_dataset('table_A', key='ID')
A1= A[['ID']]
d = mg.get_attr_types(A1)
print(d)