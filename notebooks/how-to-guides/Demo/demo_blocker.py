# coding=utf-8
import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.blocker.attr_equiv_blocker import AttrEquivalenceBlocker

class MyBlocker(AttrEquivalenceBlocker):
    def __init__(self, *args, **kwargs):
        super(AttrEquivalenceBlocker, self).__init__(*args, **kwargs)
        self.black_box_function = None
    def set_blackbox_function(self, function):
        self.black_box_function = function
    def block_tables(self, A, B, l_block_attr, r_block_attr):
        A1, B1 = A.copy(), B.copy()
        A1['l_tmp_attr'] = A[l_block_attr].map(self.black_box_function)
        B1['r_tmp_attr'] = B[r_block_attr].map(self.black_box_function)
        cm.copy_properties(A, A1)
        cm.copy_properties(B, B1)
        return super(MyBlocker, self).block_tables(A1, B1,'l_tmp_attr', 'r_tmp_attr',
                                                   l_output_attrs=[l_block_attr],
                                                   r_output_attrs=[r_block_attr])
