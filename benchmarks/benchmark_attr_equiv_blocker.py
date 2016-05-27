# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os                                                                       
                                                                                
import magellan as mg                                                           
                                                                                
class TimeBlockTables:                                                               
    def setup(self):
	p = mg.get_install_path()                                                       
	path_for_A = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'bowker.csv'])                        
	path_for_B = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'walmart.csv'])                       
	#l_key = 'ID'                                                                    
	#r_key = 'ID'                                                                    
	self.l_block_attr = 'pubYear'                                                       
	self.r_block_attr = 'pubYear'                                                       
	#l_output_attrs = ['pubYear']                                                   
	#r_output_attrs = ['pubYear']                                                   
	#l_output_prefix = 'ltable_'                                                     
	#r_output_prefix = 'rtable_' 
	self.A = mg.read_csv_metadata(path_for_A)                                        
    	mg.set_key(A, l_key)                                                        
    	self.B = mg.read_csv_metadata(path_for_B)                                        
    	mg.set_key(B, r_key)                                                        
    	self.ab = mg.AttrEquivalenceBlocker()                                            
    
    def time_books(self):                                                 
    	self.ab.block_tables(self.A, self.B, self.l_block_attr, self.r_block_attr)
