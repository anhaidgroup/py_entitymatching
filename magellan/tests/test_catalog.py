# import os
# from nose.tools import *
#
# import magellan as mg
# p = mg.get_install_path()
# path_for_A = os.sep.join([p, 'datasets', 'test_datasets', 'table_A.csv'])
# path_for_A_dup = os.sep.join([p, 'datasets', 'test_datasets', 'table_A_dupid.csv'])
# path_for_A_mvals = os.sep.join([p, 'datasets', 'test_datasets', 'table_A_mvals.csv'])
#
#
# def test_set_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
#
# def test_get_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# @raises(AttributeError)
# def test_get_property_invalid_no_df():
#     mg.get_property(None, 'key')
#
#
# def test_reset_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_property(df, 'key', 'ID1')
#     assert_equal(mg.get_property(df, 'key'), 'ID1')
#
#     mg.set_property(df, 'key', 'ID')
#     assert_equal(mg.get_property(df, 'key'), 'ID')
#
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# def test_set_key_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(mg.get_key(df), 'ID')
#
#     mg.del_property(df, 'key')
#     assert_equal(len(mg.get_all_properties(df)), 0)
#
#
# def test_set_key_invalid_dup():
#     df = mg.read_csv_metadata(path_for_A_dup)
#     status = mg.set_key(df, 'ID')
#     assert_equal(status, False)
#
# def test_set_key_invalid_mv():
#     df = mg.read_csv_metadata(path_for_A_dup)
#     status = mg.set_key(df, 'ID')
#     assert_equal(status, False)
#
# def test_get_all_properties_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(len(mg.get_all_properties(df)), 1)
#     mg.del_catalog()
#
# @raises(KeyError)
# def test_get_all_properties_invalid():
#     df = mg.read_csv_metadata(path_for_A)
#     assert_equal(len(mg.get_all_properties(df)), 1)
#
#
# def test_get_catalog_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(len(mg.get_all_properties(df)), 1)
#     c = mg.get_catalog()
#     assert_equal(len(c), 1)
#     mg.del_catalog()
#
# def test_del_property_valid():
#     df = mg.read_csv_metadata(path_for_A)
#     mg.set_key(df, 'ID')
#     assert_equal(mg.get_key(df), 'ID')
#     mg.del_property(df, 'key')
#     assert_equal(mg.is_property_present_for_df(df, 'key'), False)
#     mg.del_catalog()
#
#
# def test_del_property_invalid():
#     pass
#
#
# def test_del_all_properties_valid():
#     pass
#
#
# def test_del_all_properties_invalid():
#     pass
#
#
# def test_del_catalog_valid():
#     pass
