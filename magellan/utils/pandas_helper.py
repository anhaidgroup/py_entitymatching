import pandas as pd
import magellan.catalog.catalog_manager as cm
import magellan.utils.catalog_helper as ch
import magellan.utils.generic_helper as gh


def filter_rows(df, condn):
    pass

def project_cols(df, col_list):
    pass

def rename_col(df, old_col_name, new_col_name, inplace=False):
    pass

def mutate_col(df, new_col_name, col_values):
    pass

def drop_cols(df, col_names):
    pass

def preserve_metadata(df, command):
    pass



def _is_table_or_candset(df):
    table_props = ['key']
    candset_props = ['key', 'fk_ltable', 'fk_rtable', 'ltable', 'rtable']
    properties = cm.get_all_properties(df)
    keys = list(properties)
    if len(gh.list_diff(keys, table_props)) == 0:
        return True
    elif len(gh.list_diff(keys, candset_props)) == 0:
        return True
    else:
        return False

def _is_table(df):
    table_props = ['key']
    properties = cm.get_all_properties(df)
    keys = list(properties)
    if len(gh.list_diff(keys, table_props)) == 0:
        return True
    else:
        return False
