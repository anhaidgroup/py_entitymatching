import os
from nose.tools import *
import pandas as pd
import unittest

import py_entitymatching as em

p = em.get_install_path()
path_a = os.sep.join([p, 'tests', 'test_datasets', 'A.csv'])
path_b = os.sep.join([p, 'tests', 'test_datasets', 'B.csv'])
l_block_attr = 'lbkv'
r_block_attr = 'rbkv'
l_output_attrs = ['zipcode', 'birth_year']
r_output_attrs = ['zipcode', 'birth_year']
l_output_prefix = 'l_'
r_output_prefix = 'r_'
window_size = 3

# sn_blocking on [l|r]_block_fn_1
expected_ids_1 = sorted([('a3', 'b2'), ('a3', 'b1'), ('a3', 'b6'),
                  ('a1', 'b1'), ('a1', 'b6'), ('a1', 'b4'),
                  ('a1', 'b5'), ('a5', 'b4'), ('a5', 'b5'),
                  ('a4', 'b5'), ('a4', 'b3'), ('a2', 'b3')])

# attribute equivalence on [l|r]_block_attr_1 \intersection [l|r]_block_attr_2
#expected_ids_2 = [('a2', 'b3'), ('a3', 'b2'), ('a5', 'b5')]

expected_ids_3= sorted([ ('a4', 'b5'), ('a5', 'b5'), ('a5', 'b4'),
                 ('a5', 'b2'), ('a4', 'b2'), ('a1', 'b2'), ('a1', 'b4'),
                 ('a1', 'b1'), ('a4', 'b1'), ('a1', 'b6'), ('a4', 'b6'),
                 ('a2', 'b1'), ('a2', 'b6'), ('a2', 'b4'),
                 ('a2', 'b3'), ('a4', 'b3'), ('a3', 'b3'), ('a3', 'b4')])

# attr equiv on [l|r]_block_attr_1 in tables with missing vals, allow_missing = False
#expected_ids_4ick = [('a2', 'b3'), ('a2', 'b5'), ('a3', 'b1'), ('a3', 'b6'),
#                  ('a5', 'b3'), ('a5', 'b5')]
expected_ids_4= sorted([ ('a5', 'b5'), ('a5', 'b2'), ('a1', 'b2'), ('a1', 'b1'),
                 ('a1', 'b6'), ('a2', 'b1'), ('a2', 'b6'), ('a2', 'b3'),
                 ('a3', 'b3'), ('a3', 'b4'), ('a4', 'b3'), ('a4', 'b4') ])


# expected_ids_4 \intersection attr equiv on [l|r]_block_attr_2 in tables with
# missing vals, allow_missing = True
expected_ids_5 = [('a2', 'b3'), ('a2', 'b5'), ('a5', 'b5')]




class SortedNeighborhoodBlockerTestCases(unittest.TestCase):

    def setUp(self):
        # The blocking functions are something that a user will typically do in the step before running sn_blocker
        # To reduce the number of datasets needed, I will create different variants using different blocking key functions, on the fly
        l_block_fn_1 = lambda x:str(x['zipcode'])+":"+str(x['birth_year'])+":"+str(x['address'])+":"+str(x['name'])
        l_block_fn_2 = lambda x:str(x['name'])+":"+str(x['birth_year'])+":"+str(x['zipcode'])+":"+str(x['address'])
        l_block_fn_3 = lambda x:str(x['birth_year'])+":"+str(x['name'])+":"+str(x['address'])+":"+str(x['zipcode'])
        r_block_fn_1 = lambda x:str(x['zipcode'])+":"+str(x['birth_year'])+":"+str(x['address'])+":"+str(x['name'])
        r_block_fn_2 = lambda x:str(x['name'])+":"+str(x['birth_year'])+":"+str(x['zipcode'])+":"+str(x['address'])
        r_block_fn_3 = lambda x:str(x['birth_year'])+":"+str(x['name'])+":"+str(x['address'])+":"+str(x['zipcode'])

        # Read the file in multiple times.  I could have chosen to read it once and copy it and update the corresponding catalog, which would have been equivilant
        self.A1 = em.read_csv_metadata(path_a)
        em.set_key(self.A1, 'ID')
        self.B1 = em.read_csv_metadata(path_b)
        em.set_key(self.B1, 'ID')
        self.A2 = em.read_csv_metadata(path_a)
        em.set_key(self.A2, 'ID')
        self.B2 = em.read_csv_metadata(path_b)
        em.set_key(self.B2, 'ID')
        self.A3 = em.read_csv_metadata(path_a)
        em.set_key(self.A3, 'ID')
        self.B3 = em.read_csv_metadata(path_b)
        em.set_key(self.B3, 'ID')

        self.A1['lbkv']=self.A1[['zipcode','birth_year','address','name']].apply(l_block_fn_1, axis=1)
        self.B1['rbkv']=self.B1[['zipcode','birth_year','address','name']].apply(r_block_fn_1, axis=1)
        em.set_property(self.A1,"lbkv", "key")
        em.set_property(self.B1,"rbkv", "key")
        self.A2['lbkv']=self.A2[['zipcode','birth_year','address','name']].apply(l_block_fn_2, axis=1)
        self.B2['rbkv']=self.B2[['zipcode','birth_year','address','name']].apply(r_block_fn_2, axis=1)
        em.set_property(self.A2,"lbkv", "key")
        em.set_property(self.B2,"rbkv", "key")
        self.A3['lbkv']=self.A3[['zipcode','birth_year','address','name']].apply(l_block_fn_3, axis=1)
        self.B3['rbkv']=self.B3[['zipcode','birth_year','address','name']].apply(r_block_fn_3, axis=1)
        em.set_property(self.A3,"lbkv", "key")
        em.set_property(self.B3,"rbkv", "key")

        self.sn = em.SortedNeighborhoodBlocker()

        
    def tearDown(self):
        del self.A1
        del self.B1
        del self.A2
        del self.B2
        del self.A3
        del self.B3
        del self.sn

    @raises(AssertionError)
    def test_sn_block_tables_invalid_ltable_1(self):
        self.sn.block_tables(None, self.B1, l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_ltable_2(self):
        self.sn.block_tables([10, 10], self.B1, l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_ltable_3(self):
        self.sn.block_tables(pd.DataFrame(), self.B1,
                             l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_rtable_1(self):
        self.sn.block_tables(self.A1, None, l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_rtable_2(self):
        self.sn.block_tables(self.A1, [10, 10], l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_rtable_3(self):
        self.sn.block_tables(self.A1, pd.DataFrame(),
                             l_block_attr, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_block_attr(self):
        self.sn.block_tables(self.A1, self.B1, None, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_block_fn_2(self):
        self.sn.block_tables(self.A1, self.B1, 10, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_block_fn_3(self):
        self.sn.block_tables(self.A1, self.B1, True, r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_bogus_l_block_fn(self):
        self.sn.block_tables(self.A1, self.B1, 'bogus_attr', r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_multi_l_block_fn(self):
        self.sn.block_tables(self.A1, self.B1, ['zipcode', 'birth_year'],
                             r_block_attr, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_block_attr(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, None, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_block_fn_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, 10, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_block_fn_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, True, window_size)

    @raises(AssertionError)
    def test_sn_block_tables_bogus_r_block_fn(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, 'bogus_attr', window_size)

    @raises(AssertionError)
    def test_sn_block_tables_multi_r_block_fn(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, window_size,
                             ['zipcode', 'birth_year'])

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_attrs_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size, 1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_attrs_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             'name')

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_attrs_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             [1, 2])

    @raises(AssertionError)
    def test_sn_block_tables_bogus_l_output_attrs(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             ['bogus_attr'])

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_attrs_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_attrs=1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_attrs_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_attrs='name')

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_attrs_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_attrs=[1, 2])

    @raises(AssertionError)
    def test_sn_block_tables_bogus_r_output_attrs(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_attrs=['bogus_attr'])

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_prefix_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             l_output_prefix=None)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_prefix_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             l_output_prefix=1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_l_output_prefix_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             l_output_prefix=True)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_prefix_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_prefix=None)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_prefix_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_prefix=1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_r_output_prefix_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             r_output_prefix=True)
    @raises(AssertionError)
    def test_sn_block_tables_invalid_allow_missing_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             allow_missing=None)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_allow_misisng_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             allow_missing=1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_allow_missing_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             allow_missing='yes')

    @raises(AssertionError)
    def test_sn_block_tables_invalid_verbose_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             verbose=None)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_verbose_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             verbose=1)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_verbose_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             verbose='yes')

    @raises(AssertionError)
    def test_sn_block_tables_invalid_njobs_1(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             n_jobs=None)

    @raises(AssertionError)
    def test_sn_block_tables_invalid_njobs_2(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             n_jobs='1')

    @raises(AssertionError)
    def test_sn_block_tables_invalid_njobs_3(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             n_jobs=1.5)

    @raises(AssertionError)
    def test_sn_block_tables_window_size_zero(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, window_size,
                             n_jobs=1.5)

    @raises(AssertionError)
    def test_sn_block_tables_window_size_zero(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, 0,
                             n_jobs=2)

    @raises(AssertionError)
    def test_sn_block_tables_window_size_one(self):
        self.sn.block_tables(self.A1, self.B1, l_block_attr, r_block_attr, 1,
                             n_jobs=2)

    def test_sn_block_tables(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

#find replacement?    def test_sn_block_tables_wi_no_output_tuples(self):
#find replacement?        C = self.sn.block_tables(self.A, self.B,
#find replacement?                                 l_block_fn_3, r_block_fn_3, window_size)
#find replacement?        validate_metadata(C)
#find replacement?        validate_data(C)

    def test_sn_block_tables_wi_null_l_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 None, r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_null_r_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, None)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_l_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 [], r_output_attrs)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_r_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, [])
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size, [], [])
        validate_metadata(C)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_block_attr_not_in_output_attrs(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 ['birth_year'], ['birth_year'])
        validate_metadata(C, ['birth_year'], ['birth_year'])
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_missing_values_allow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.sn.block_tables(A, B, 'name', 'name', window_size,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix, True)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        print (C)
        validate_data(C, expected_ids_3)

    def test_sn_block_tables_wi_missing_values_disallow_missing(self):
        path_a = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_A_wi_missing_vals.csv'])
        path_b = os.sep.join([p, 'tests', 'test_datasets', 'blocker',
                              'table_B_wi_missing_vals.csv'])
        A = em.read_csv_metadata(path_a)
        em.set_key(A, 'ID')
        B = em.read_csv_metadata(path_b)
        em.set_key(B, 'ID')
        C = self.sn.block_tables(A, B, 'name', 'name', window_size,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_4)


    @raises(AssertionError)
    def test_sn_block_tuples(self):
        A = self.sn.block_tuples(self.A1.ix[1], self.B1.ix[2], l_block_attr, r_block_attr, window_size)


class SortedNeighborhoodBlockerMulticoreTestCases(unittest.TestCase):

    def setUp(self):
        # The blocking functions are something that a user will typically do in the step before running sn_blocker
        # To reduce the number of datasets needed, I will create different variants using different blocking key functions, on the fly
        l_block_fn_1 = lambda x:str(x['zipcode'])+":"+str(x['birth_year'])+":"+str(x['address'])+":"+str(x['name'])
        l_block_fn_2 = lambda x:str(x['name'])+":"+str(x['birth_year'])+":"+str(x['zipcode'])+":"+str(x['address'])
        l_block_fn_3 = lambda x:str(x['birth_year'])+":"+str(x['name'])+":"+str(x['address'])+":"+str(x['zipcode'])
        r_block_fn_1 = lambda x:str(x['zipcode'])+":"+str(x['birth_year'])+":"+str(x['address'])+":"+str(x['name'])
        r_block_fn_2 = lambda x:str(x['name'])+":"+str(x['birth_year'])+":"+str(x['zipcode'])+":"+str(x['address'])
        r_block_fn_3 = lambda x:str(x['birth_year'])+":"+str(x['name'])+":"+str(x['address'])+":"+str(x['zipcode'])

        # Read the file in multiple times.  I could have chosen to read it once and copy it and update the corresponding catalog, which would have been equivilant
        self.A1 = em.read_csv_metadata(path_a)
        em.set_key(self.A1, 'ID')
        self.B1 = em.read_csv_metadata(path_b)
        em.set_key(self.B1, 'ID')
        self.A2 = em.read_csv_metadata(path_a)
        em.set_key(self.A2, 'ID')
        self.B2 = em.read_csv_metadata(path_b)
        em.set_key(self.B2, 'ID')
        self.A3 = em.read_csv_metadata(path_a)
        em.set_key(self.A3, 'ID')
        self.B3 = em.read_csv_metadata(path_b)
        em.set_key(self.B3, 'ID')

        self.A1['lbkv']=self.A1[['zipcode','birth_year','address','name']].apply(l_block_fn_1, axis=1)
        self.B1['rbkv']=self.B1[['zipcode','birth_year','address','name']].apply(r_block_fn_1, axis=1)
        em.set_property(self.A1,"lbkv", "key")
        em.set_property(self.B1,"rbkv", "key")
        self.A2['lbkv']=self.A2[['zipcode','birth_year','address','name']].apply(l_block_fn_2, axis=1)
        self.B2['rbkv']=self.B2[['zipcode','birth_year','address','name']].apply(r_block_fn_2, axis=1)
        em.set_property(self.A2,"lbkv", "key")
        em.set_property(self.B2,"rbkv", "key")
        self.A3['lbkv']=self.A3[['zipcode','birth_year','address','name']].apply(l_block_fn_3, axis=1)
        self.B3['rbkv']=self.B3[['zipcode','birth_year','address','name']].apply(r_block_fn_3, axis=1)
        em.set_property(self.A3,"lbkv", "key")
        em.set_property(self.B3,"rbkv", "key")

        self.sn = em.SortedNeighborhoodBlocker()

    def tearDown(self):
        del self.A1
        del self.B1
        del self.A2
        del self.B2
        del self.A3
        del self.B3
        del self.sn

    def test_sn_block_tables_njobs_2(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix, n_jobs=2)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)
    
    def test_sn_block_tables_wi_null_l_output_attrs_njobs_2(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 None, r_output_attrs, n_jobs=2)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_null_r_output_attrs_njobs_2(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, None, n_jobs=2)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_l_output_attrs_njobs_2(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 [], r_output_attrs, n_jobs=2)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_r_output_attrs_njobs_2(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, [], n_jobs=2)
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_njobs_all(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, r_output_attrs, 
                                 l_output_prefix, r_output_prefix, n_jobs=-1)
        validate_metadata(C, l_output_attrs, r_output_attrs,
                          l_output_prefix, r_output_prefix)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_null_l_output_attrs_njobs_all(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 None, r_output_attrs, n_jobs=-1)
        validate_metadata(C, r_output_attrs=r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_null_r_output_attrs_njobs_all(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 l_output_attrs, None, n_jobs=-1)
        validate_metadata(C, l_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_l_output_attrs_njobs_all(self):
        C = self.sn.block_tables(self.A1, self.B1,
                                 l_block_attr, r_block_attr, window_size,
                                 [], r_output_attrs, n_jobs=-1)
        validate_metadata(C, [], r_output_attrs)
        validate_data(C, expected_ids_1)

    def test_sn_block_tables_wi_empty_r_output_attrs_njobs_all(self):
        C = self.sn.block_tables(self.A1, self.B1, l_block_attr,
                                 r_block_attr, window_size, l_output_attrs, [], n_jobs=-1)
        validate_metadata(C, l_output_attrs, [])
        validate_data(C, expected_ids_1)

    
def validate_metadata(C, l_output_attrs=None, r_output_attrs=None,
                      l_output_prefix='ltable_', r_output_prefix='rtable_',
                      l_key='ID', r_key='ID'):
    s1 = ['_id', l_output_prefix + l_key, r_output_prefix + r_key]
    if l_output_attrs:
        s1 += [l_output_prefix + x for x in l_output_attrs if x != l_key]
    if r_output_attrs:
        s1 += [r_output_prefix + x for x in r_output_attrs if x != r_key]
    s1 = sorted(s1)
    assert_equal(s1, sorted(C.columns))
    assert_equal(em.get_key(C), '_id')
    assert_equal(em.get_property(C, 'fk_ltable'), l_output_prefix + l_key)
    assert_equal(em.get_property(C, 'fk_rtable'), r_output_prefix + r_key)
    
def validate_data(C, expected_ids=None):
    if expected_ids:
        lid = em.get_property(C, 'fk_ltable')
        rid = em.get_property(C, 'fk_rtable')
        C_ids = C[[lid, rid]].set_index([lid, rid])
        actual_ids = sorted(C_ids.index.values.tolist())
        assert_equal(expected_ids, actual_ids)
    else:
        assert_equal(len(C), 0)
    
def validate_metadata_two_candsets(C, D): 
    assert_equal(sorted(C.columns), sorted(D.columns))
    assert_equal(em.get_key(D), em.get_key(C))
    assert_equal(em.get_property(D, 'fk_ltable'), em.get_property(C, 'fk_ltable'))
    assert_equal(em.get_property(D, 'fk_rtable'), em.get_property(C, 'fk_rtable'))
