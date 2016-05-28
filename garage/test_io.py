# coding=utf-8
import magellan as mg

A = mg.load_dataset('table_A', key='ID')
B = mg.load_dataset('table_B', key='ID')

ab = mg.AttrEquivalenceBlocker()
C = ab.block_tables(A, B,  'zipcode', 'zipcode', l_output_attrs=['name', 'zipcode', 'birth_year'],
                    r_output_attrs=['name', 'zipcode', 'birth_year'])


mg.to_csv_metadata(C, './C.csv')
print 'Hi'