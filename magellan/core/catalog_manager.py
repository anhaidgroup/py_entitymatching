# coding=utf-8
import logging

import pandas as pd
import six

from magellan.core.catalog import Catalog
import magellan.utils.catalog_helper as ch

logger = logging.getLogger(__name__)


def get_property(df, name):
    """
    Get property for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the property should be retrieved
        name (str): Name of the property that should be retrieved

    Returns:
        Property value (pandas object) for the given property name

    Raises:
        AttributeError: If the input dataframe in null
        KeyError: If the dataframe is not present in the catalog, or the requested property is not
            present in the catalog

    """

    catalog = Catalog.Instance()

    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')


    # if df is None or pd.isnull(df):
    #     logger.error('Input dataframe cannot be null')
    #     raise AttributeError('Input dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    if not catalog.is_property_present_for_df(df, name):
        logger.error('Requested metadata ( %s ) for the given dataframe is not present in the catalog' %name)
        raise KeyError('Requested metadata ( %s ) for the given dataframe is not present in the catalog' %name)

    return catalog.get_property(df, name)


def set_property(df, name, value):
    """
    Set property for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the property has to be set
        name (str): Property name
        value (pandas object): Property value

    Returns:
        status (bool). Returns True if the property was set successfully

    Raises:
        AttributeError: If the input dataframe is null

    """
    catalog = Catalog.Instance()

    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')


    # if df is None:
    #     raise AttributeError('Input dataframe cannot be null')

    if not catalog.is_dfinfo_present(df):
        catalog.init_properties(df)

    catalog.set_property(df, name, value)


def init_properties(df):
    catalog = Catalog.Instance()
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')
    catalog.init_properties(df)


def get_all_properties_for_id(obj_id):
    catalog = Catalog.Instance()
    if obj_id not in catalog.get_catalog():
        logger.error('Object not in the catalog')
        raise KeyError('Object not in the catalog')
    return catalog.get_all_properties_for_id(obj_id)


def get_all_properties(df):
    """
    Get all the properties for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the properties must be retrieved

    Returns:
        Property dictionary (dict). The keys are property names (str) and the values are property values (pandas object)

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the information about the input dataframe is not present in the catalog

    """
    catalog = Catalog.Instance()

    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not catalog.is_dfinfo_present(df):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.get_all_properties(df)


def del_property(df, name):
    """
    Delete a property from the catalog

    Args:
        df (pandas dataframe): Input dataframe for which a property must be deleted
        name (str): Property name

    Returns:
        status (bool). Returns True if the deletion was successful

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the Dataframe info. is not present or the given property is not present for that dataframe in the
            catalog
    """
    catalog = Catalog.Instance()

    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')


    if not catalog.is_dfinfo_present(df):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    if not catalog.is_property_present_for_df(df, name):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Requested metadata ( %s ) for the given dataframe is '
                       'not present in the catalog' %name)

    return catalog.del_property(df, name)


def del_all_properties(df):
    """
    Delete all properties for a dataframe

    Args:
        df (pandas dataframe): Input dataframe for which all the properties must be deleted.

    Returns:
        status (bool). Returns True if the deletion was successful

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the dataframe information is not present in the catalog
    """
    catalog = Catalog.Instance()
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not catalog.is_dfinfo_present(df):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.del_all_properties(df)


def get_catalog():
    """
    Get Catalog information.


    Returns:
        Catalog information in a dictionary format.

    """
    catalog = Catalog.Instance()
    return catalog.get_catalog()


def del_catalog():
    """
    Delete catalog information

    Returns:
        status (bool). Returns True if the deletion was successful.
    """
    catalog = Catalog.Instance()
    return catalog.del_catalog()


def is_catalog_empty():
    """
    Check if the catalog is empty

    Returns:
        result (bool). Returns True if the catalog is empty, else returns False.

    """
    catalog = Catalog.Instance()
    return catalog.is_catalog_empty()


def is_dfinfo_present(df):
    """
    Check if the dataframe information is present in the catalog

    Args:
        df (pandas dataframe): Input dataframe

    Returns:
        result (bool). Returns True if the dataframe information is present in the catalog, else returns False

    Raises:
        AttributeError: If the input dataframe is null

    """
    catalog = Catalog.Instance()
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    return catalog.is_dfinfo_present(df)


def is_property_present_for_df(df, name):
    """
    Check if the property is present for the dataframe

    Args:
        df (pandas dataframe): Input dataframe
        name (str): Property name

    Returns:
        result (bool). Returns True if the property is present for the input dataframe

    Raises:
        AttributeError: If the input dataframe is null
        KeyError: If the dataframe is not present in the catalog

    """
    catalog = Catalog.Instance()
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if catalog.is_dfinfo_present(df) is False:
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    return catalog.is_property_present_for_df(df, name)


def get_catalog_len():
    """
    Get the number of entries in the catalog

    Returns:
        length (int) of the catalog

    """
    catalog = Catalog.Instance()
    return catalog.get_catalog_len()


def set_properties(df, prop_dict, replace=True):
    """
    Set properties for a dataframe in the catalog
    Args:
        df (pandas dataframe): Input dataframe
        prop_dict (dict): Property dictionary with keys as property names and values as python objects
        replace (bool): Flag to indicate whether the input properties can replace the properties in the catalog

    Returns:
        status (bool). Returns True if the setting of properties was successful

    Notes:
        The function is intended to set all the properties in the catalog with the given property dictionary.
          The replace flag is just a check where the properties will be not be disturbed if they exist already in the
          catalog

    """
    catalog = Catalog.Instance()
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(prop_dict, dict):
        logger.error('The properties should be of type python dictionary')
        raise AssertionError('The properties should be of type python dictionary')

    if catalog.is_dfinfo_present(df) and replace is False:
        logger.warning('Properties already exists for df ( %s ). Not replacing it' %str(id(df)))
        return False

    if not catalog.is_dfinfo_present(df):
        catalog.init_properties(df)

    for k, v in prop_dict.iteritems():
        catalog.set_property(df, k, v)
    return True


def copy_properties(src, tar, replace=True):
    """
    Copy properties from one dataframe to another
    Args:
        src (pandas dataframe): Dataframe from which the properties to be copied from
        tar (pandas dataframe): Dataframe to which the properties to be copied
        replace (bool): Flag to indicate whether the source properties can replace the tart properties

    Returns:
        status (bool). Returns True if the copying was successful

    Notes:
        This function internally calls set_properties and get_all_properties


    """
    # copy catalog information from src to tar
    catalog = Catalog.Instance()
    if not isinstance(src, pd.DataFrame):
        logger.error('Input object (src) is not of type pandas data frame')
        raise AssertionError('Input object (src) is not of type pandas data frame')

    if not isinstance(src, pd.DataFrame):
        logger.error('Input object (tar) is not of type pandas data frame')
        raise AssertionError('Input object (tar) is not of type pandas data frame')

    if catalog.is_dfinfo_present(src) is False:
        logger.error('Dataframe information (src) is not present in the catalog')
        raise KeyError('Dataframe information (src) is not present in the catalog')

    metadata = catalog.get_all_properties(src)
    return set_properties(tar, metadata, replace) # this initializes tar in the catalog.


# key related methods
def get_key(df):
    """
    Get the key attribute for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the key must be retrieved

    Returns:
        key (str)

    """
    # get_property already checks for df type and the presence
    # of df in the catalog
    return get_property(df, 'key')


def set_key(df, key):
    """
    Set the key attribute for a dataframe

    Args:
        df (pandas dataframe): Dataframe for which the key must be set
        key (str): Key attribute in the dataframe

    Returns:
        status (bool). Returns True if the key attribute was set successfully, else returns False

    """

    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not key in df.columns:
        logger.error('Input key ( %s ) not in the dataframe' %key)
        raise KeyError('Input key ( %s ) not in the dataframe' %key)

    if ch.is_key_attribute(df, key) is False:
        logger.warning('Attribute (' + key + ') does not qualify to be a key; Not setting/replacing the key')
        return False
    else:
        return set_property(df, 'key', key)


def get_fk_ltable(df):
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    return get_property(df, 'fk_ltable')


def get_fk_rtable(df):
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    return get_property(df, 'fk_rtable')


def set_fk_ltable(df, fk_ltable):
    """
    Set foreign key attribute to the left table
    Args:
        df (pandas dataframe): Dataframe for which the foreign key must be set
        fk_ltable (str): Foreign key attribute in the dataframe

    Returns:
        status (bool). Returns True if the ltable foreign key attribute was set successfully, else returns False
    """
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not fk_ltable in df.columns:
        logger.error('Input attr. ( %s ) not in the dataframe' %fk_ltable)
        raise KeyError('Input attr. ( %s ) not in the dataframe' %fk_ltable)


    return set_property(df, 'fk_ltable', fk_ltable)


def set_fk_rtable(df, fk_rtable):
    """
    Set foreign key attribute to the right table
    Args:
        df (pandas dataframe): Dataframe for which the foreign key must be set
        fk_rtable (str): Foreign key attribute in the dataframe

    Returns:
        status (bool). Returns True if the rtable foreign key attribute was set successfully, else returns False
    """
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not fk_rtable in df.columns:
        logger.error('Input attr. ( %s ) not in the dataframe' %fk_rtable)
        raise KeyError('Input attr. ( %s ) not in the dataframe' %fk_rtable)

    return set_property(df, 'fk_rtable', fk_rtable)


def get_reqd_metadata_from_catalog(df, reqd_metadata):
    """
    Get a list of properties from the catalog

    Args:
        df (pandas dataframe): Dataframe for which the properties must be retrieved
        reqd_metadata (list): List of properties to be retrieved

    Returns:
        properties (dict)

    Notes:
        This is an internal helper function.


    """
    if not isinstance(df, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not isinstance(reqd_metadata, list):
        reqd_metadata = [reqd_metadata]

    metadata = {}
    d = get_all_properties(df)
    for m in reqd_metadata:
        if m in d:
            metadata[m] = d[m]
    return metadata


def update_reqd_metadata_with_kwargs(metadata, kwargs_dict, reqd_metadata):
    """
    Update the metadata with input args

    Args:
        metadata (dict): Properties dictonary
        kwargs_dict (dict): Input key-value args
        reqd_metadata (list): List of properties to be updated.

    Returns:
        updated properties (dict)

    Notes:
        This is an internal helper function.


    """
    if not isinstance(metadata, dict):
        logger.error('Input metdata is not of type dict')
        raise AssertionError('Input metdata is not of type dict')

    if not isinstance(kwargs_dict, dict):
        logger.error('Input kwargs_dict is not of type dict')
        raise AssertionError('Input kwargs_dict is not of type dict')


    if not isinstance(reqd_metadata, list):
        reqd_metadata = [reqd_metadata]

    for m in reqd_metadata:
        if m in kwargs_dict:
            metadata[m] = kwargs_dict[m]
    return metadata


def get_diff_with_reqd_metadata(metadata, reqd_metadata):
    """
    Find what metadata is missing from the required list

    Args:
        metadata (dict): Property dictionary
        reqd_metadata (list): List of properties

    Returns:
        diff list (list) of properties between the property dictionary and the properties list

    Notes:
        This is an internal helper function
    """
    if not isinstance(metadata, dict):
        logger.error('Input metdata is not of type dict')
        raise AssertionError('Input metdata is not of type dict')

    k = metadata.keys()
    if not isinstance(reqd_metadata, list):
        reqd_metadata = [reqd_metadata]
    d = set(reqd_metadata).difference(k)
    return d


def is_all_reqd_metadata_present(metadata, reqd_metadata):
    """
    Check if all the required metadata are present

    Args:
        metadata (dict): Property dictionary
        reqd_metadata (list): List of properties

    Returns:
        result (bool). Returns True if all the required metadata is present, else returns False

    Notes:
        This is an internal helper function

    """
    if not isinstance(metadata, dict):
        logger.error('Input metdata is not of type dict')
        raise AssertionError('Input metdata is not of type dict')

    d = get_diff_with_reqd_metadata(metadata, reqd_metadata)
    if len(d) == 0:
        return True
    else:
        return False


def show_properties(df):
    if not is_dfinfo_present(df):
        logger.warning('Dataframe information is not present in the catalog')
        return
    metadata = get_all_properties(df)
    print 'id: ' + str(id(df))
    for prop in metadata.iterkeys():
        value = metadata[prop]
        if isinstance(value, basestring):
            print prop + ": " + value
        else:
            print prop + "(obj.id): " + str(id(value))


def show_properties_for_id(obj_id):
    metadata = get_all_properties_for_id(obj_id)
    print 'id: ' + str(obj_id)
    for prop in metadata.iterkeys():
        value = metadata[prop]
        if isinstance(value, basestring):
            print prop + ": " + value
        else:
            print prop + "(obj.id): " + str(id(value))


def set_candset_properties(candset, key, fk_ltable, fk_rtable, ltable, rtable):
    set_property(candset, 'key', key)
    set_fk_ltable(candset, fk_ltable)
    set_fk_rtable(candset, fk_rtable)
    set_property(candset, 'ltable', ltable)
    set_property(candset, 'rtable', rtable)


def validate_metadata_for_table(table, key, out_str, lgr, verbose):
    if not isinstance(table, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    if not key in table.columns:
        logger.error('Input key ( %s ) not in the dataframe' %key)
        raise KeyError('Input key ( %s ) not in the dataframe' %key)


    ch.log_info(lgr, 'Validating ' + out_str + ' key: ' + str(key), verbose)
    assert isinstance(key, six.string_types) is True, 'Key attribute must be a string.'
    assert ch.check_attrs_present(table, key) is True, 'Key attribute is not present in the ' + out_str + ' table'
    assert ch.is_key_attribute(table, key, verbose) == True, 'Attribute ' + str(key) + \
                                                             ' in the ' + out_str + ' table ' \
                                                                                    'does not qualify to be the key'
    ch.log_info(lgr, '..... Done', verbose)


def validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                  lgr, verbose):
    if not isinstance(candset, pd.DataFrame):
        logger.error('Input cand.set is not of type pandas data frame')
        raise AssertionError('Input cand.set is not of type pandas data frame')

    if not key in candset.columns:
        logger.error('Input key ( %s ) not in the dataframe' %key)
        raise KeyError('Input key ( %s ) not in the dataframe' %key)

    if not fk_ltable in candset.columns:
        logger.error('Input fk_ltable ( %s ) not in the dataframe' %fk_ltable)
        raise KeyError('Input fk_rtable ( %s ) not in the dataframe' %fk_rtable)

    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas data frame')
        raise AssertionError('Input ltable is not of type pandas data frame')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas data frame')
        raise AssertionError('Input rtable is not of type pandas data frame')

    if not l_key in ltable:
        logger.error('ltable key ( %s ) not in ltable' %l_key)
        raise KeyError('ltable key ( %s ) not in ltable' %l_key)

    if not r_key in rtable:
        logger.error('rtable key ( %s ) not in rtable' %r_key)
        raise KeyError('rtable key ( %s ) not in rtable' %r_key)


    validate_metadata_for_table(candset, key, 'cand.set', lgr, verbose)

    ch.log_info(lgr, 'Validating foreign key constraint for left table', verbose)
    assert ch.check_fk_constraint(candset, fk_ltable, ltable, l_key) == True, 'Cand.set does not satisfy foreign key ' \
                                                                              'constraint with the left table'
    ch.log_info(lgr, '..... Done', verbose)
    ch.log_info(lgr, 'Validating foreign key constraint for right table', verbose)
    assert ch.check_fk_constraint(candset, fk_rtable, rtable, r_key) == True, 'Cand.set does not satisfy foreign key ' \
                                                                              'constraint with the right table'
    ch.log_info(lgr, '..... Done', verbose)

    return True


def get_keys_for_ltable_rtable(ltable, rtable, lgr, verbose):

    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas data frame')
        raise AssertionError('Input ltable is not of type pandas data frame')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas data frame')
        raise AssertionError('Input rtable is not of type pandas data frame')

    ch.log_info(lgr, 'Required metadata: ltable key, rtable key', verbose)
    ch.log_info(lgr, 'Getting metadata from the catalog', verbose)
    l_key = get_key(ltable)
    r_key = get_key(rtable)
    ch.log_info(lgr, '..... Done', verbose)
    return l_key, r_key


def get_metadata_for_candset(candset, lgr, verbose):
    if not isinstance(candset, pd.DataFrame):
        logger.error('Input candset is not of type pandas data frame')
        raise AssertionError('Input candset is not of type pandas data frame')

    ch.log_info(lgr, 'Getting metadata from the catalog', verbose)
    key = get_key(candset)
    fk_ltable = get_fk_ltable(candset)
    fk_rtable = get_fk_rtable(candset)
    ltable = get_ltable(candset)
    rtable = get_rtable(candset)
    l_key = get_key(ltable)
    r_key = get_key(rtable)
    ch.log_info(lgr, '..... Done', verbose)
    return key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key

def get_ltable(candset):
    return get_property(candset, 'ltable')

def get_rtable(candset):
    return get_property(candset, 'rtable')
