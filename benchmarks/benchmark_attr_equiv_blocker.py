# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os                                                                       
import sys
                                                                                
import magellan  as mg                                                           
                                                                                
class TimeBlockTablesBooks:                                                               
    def setup(self):
	p = mg.get_install_path()                                                       
	path_for_A = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'bowker.csv'])                        
	path_for_B = os.sep.join([p, 'datasets', 'test_datasets', 'blocker', 'walmart.csv'])                       
	l_key = 'ID'                                                                    
	r_key = 'ID'                                                                    
	self.l_block_attr = 'pubYear'                                                       
	self.r_block_attr = 'pubYear'                                                       
	#l_output_attrs = ['pubYear']                                                   
	#r_output_attrs = ['pubYear']                                                   
	#l_output_prefix = 'ltable_'                                                     
	#r_output_prefix = 'rtable_' 
	self.A = mg.read_csv_metadata(path_for_A)
	#print sys.stderr, "Size of A: ", len(self.A)                                        
    	mg.set_key(self.A, l_key)                                                        
    	self.B = mg.read_csv_metadata(path_for_B)                                        
	#print sys.stderr, "Size of B: ", len(self.B)                                        
    	mg.set_key(self.B, r_key)                                                        
    	self.ab = mg.AttrEquivalenceBlocker()                                            
    
    def time_block_tables(self):                                                 
    	self.ab.block_tables(self.A, self.B, self.l_block_attr,
			     self.r_block_attr, verbose=False)
    def teardown(self):
	del self.A
	del self.B
	del self.ab

class TimeBlockCandsetBikes:
    timeout = 3600.0                                                               
    def setup(self):
	p = mg.get_install_path()                                                       
	path_for_A = os.sep.join([p, 'datasets', 'benchmark_datasets', 'bikes', 'A.csv'])                        
	path_for_B = os.sep.join([p, 'datasets', 'benchmark_datasets', 'bikes', 'B.csv'])                        
	l_key = 'ID'                                                                    
	r_key = 'ID'                                                                    
	self.A = mg.read_csv_metadata(path_for_A)
    	mg.set_key(self.A, l_key)                                                        
    	self.B = mg.read_csv_metadata(path_for_B)                                        
    	mg.set_key(self.B, r_key)                                                        
	l_block_attr_1 = 'city_posted'                                                       
	r_block_attr_1 = 'city_posted'
	l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
			  'color', 'model_year']                                                       
	r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
			  'color', 'model_year']                                                       
    	self.ab = mg.AttrEquivalenceBlocker()
	self.C = self.ab.block_tables(self.A, self.B, l_block_attr_1,
				      r_block_attr_1, l_output_attrs,
				      r_output_attrs, verbose=False)                                            
    	self.l_block_attr = 'model_year'
	self.r_block_attr = 'model_year'

    def time_block_candset(self):                                                 
    	self.ab.block_candset(self.C, self.l_block_attr, self.r_block_attr,
			      verbose=False, show_progress=False)

    def teardown(self):
	del self.A
	del self.B
	del self.C
	del self.ab
