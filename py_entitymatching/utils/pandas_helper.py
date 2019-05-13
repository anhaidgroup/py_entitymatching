import logging

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.utils.catalog_helper as ch
import py_entitymatching.utils.generic_helper as gh

logger = logging.getLogger(__name__)


def filter_rows(df, condn):
    new_df = df.query(condn)

    # update metadata
    if cm.is_dfinfo_present(df):
        if _is_table_or_candset(df):
            cm.init_properties(new_df)
            cm.copy_properties(df, new_df)

    return new_df


def project_cols(df, col_list):
    if not isinstance(col_list, list):
        col_list = [col_list]
    if cm.is_dfinfo_present(df):
        if _is_table_or_candset(df):
            if not _is_table(df):
                key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key\
                    = cm.get_metadata_for_candset(df, logger, False)
                updated_col_list = [key, fk_ltable, fk_rtable]
                updated_col_list.extend(col_list)
                col_list = gh.list_drop_duplicates(updated_col_list)
            else:
                key = cm.get_key(df)
                updated_col_list = [key]
                updated_col_list.extend(col_list)
                col_list = gh.list_drop_duplicates(updated_col_list)
        new_df = df[col_list]
        cm.init_properties(new_df)
        cm.copy_properties(df, new_df)
    else:
        new_df = df[col_list]

    return new_df


def rename_col(df, old_col_name, new_col_name):
    new_df = df.rename(columns={old_col_name: new_col_name})

    if cm.is_dfinfo_present(df):
        cm.init_properties(new_df)
        cm.copy_properties(df, new_df)

        if _is_table_or_candset(df):
            if not _is_table(df):
                key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = \
                    cm.get_metadata_for_candset(df, logger, False)
                if key == old_col_name:
                    cm.set_key(new_df, new_col_name)
                elif fk_ltable == old_col_name:
                    cm.set_fk_ltable(new_df, new_col_name)
                elif fk_rtable == old_col_name:
                    cm.set_fk_rtable(new_df, new_col_name)
                else:
                    pass
            else:
                key = cm.get_key(df)
                if key == old_col_name:
                    cm.set_key(new_df, new_col_name)

    return new_df


def mutate_col(df, **kwargs):
    new_df = df.assign(**kwargs)

    if cm.is_dfinfo_present(df):
        cm.init_properties(new_df)
        cm.copy_properties(df, new_df)

        # if _is_table_or_candset(df):
        #     key = cm.get_key(df)
        #     if key == new_col_name:
        #         cm.set_key(new_df, new_col_name)

    return new_df

def drop_cols(df, col_list):
    if not isinstance(col_list, list):
        col_list = [col_list]
    if cm.is_dfinfo_present(df):
        if _is_table_or_candset(df):
            if not _is_table(df):
                key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key\
                    = cm.get_metadata_for_candset(df, logger, False)
                col_list = gh.list_diff(col_list, [key, fk_ltable, fk_rtable])
                col_list = gh.list_drop_duplicates(col_list)
            else:
                key = cm.get_key(df)
                col_list = gh.list_diff(col_list, [key])
                col_list = gh.list_drop_duplicates(col_list)
        new_df = df.drop(col_list, axis=1)
        cm.init_properties(new_df)
        cm.copy_properties(df, new_df)
    else:
        new_df = df[col_list]

    return new_df


def preserve_metadata(df, new_df):
    if cm.is_dfinfo_present(df):
        if _is_table_or_candset(df):
            if not _is_table(df):
                key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key\
                    = cm.get_metadata_for_candset(df, logger, False)
                if not ch.check_attrs_present(new_df, [key, fk_ltable,
                                                     fk_rtable]):
                    logger.warning('Not setting the metadata as some attrs '
                                   'are not present')
                    return new_df
            else:
                key = cm.get_key(df)
                if not ch.check_attrs_present(new_df, [key]):
                    logger.warning('Not setting the metadata as some attrs '
                                   'are not present')
                    return new_df


        cm.init_properties(new_df)
        cm.copy_properties(df, new_df)
    return new_df


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
