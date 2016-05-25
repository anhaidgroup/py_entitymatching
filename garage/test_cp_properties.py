import magellan as mg
import pandas as pd
import magellan.core.catalog_manager as cm

A = mg.load_dataset('table_A')
B = pd.read_csv('../magellan/datasets/B.csv')
cm.init_properties(B)
cm.copy_properties(A, B)

print 'hi'