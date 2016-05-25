import magellan as mg

A = mg.load_dataset('table_A')
B = mg.load_dataset('table_B')

ab = mg.AttrEquivalenceBlocker()
C1 = ab.block_tables(A, B, 'name', 'name', ['name', 'address'], ['name', 'address'])

D = mg.combine_blocker_outputs_via_union([C1, C1])

print D.columns