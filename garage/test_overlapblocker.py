import magellan as mg
A = mg.load_dataset('table_A')
B = mg.load_dataset('table_B')

ob = mg.OverlapBlocker()
C = ob.block_tables(A, B, 'name', 'name')

print C



# define structures for each type


