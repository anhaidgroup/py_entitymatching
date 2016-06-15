# coding=utf-8
"""
This module contains wrapper functions for the catalog.
"""
import logging

import pandas as pd
import six

import magellan.utils.catalog_helper as ch
from magellan.catalog.catalog import Catalog

logger = logging.getLogger(__name__)


def get_property(data_frame, property_name):
    """
    Gets a property (with the given property name) for a pandas DataFrame from
    the Catalog.

    Args:
        data_frame (DataFrame): Dataframe for which the property should be
            retrieved.
        property_name (str): Name of the property that should be retrieved.

    Returns:
        A pandas object (typically a string or a pandas DataFrame depending
        on the property name) is returned.

    Raises:
        AssertionError: If the object is not of type pandas DataFrame.
        AssertionError: If the property name is not of type string.
        KeyError: If the DataFrame information is not present in the catalog.
        KeyError: If the requested property for the DataFrame is not present
            in the catalog.
    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # # The property name should be of type string
    if not isinstance(property_name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')

    # Get the catalog instance, this is imported here because this object
    # used to validate the presence of a DataFrame in the catalog, and the
    # presence of requested metadata in the catalog.
    catalog = Catalog.Instance()

    # Check for the present of input DataFrame in the catalog.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    # Check if the requested property is present in the catalog.
    if not catalog.is_property_present_for_df(data_frame, property_name):
        logger.error(
            'Requested metadata ( %s ) for the given dataframe is not '
            'present in the catalog', property_name)
        raise KeyError(
            'Requested metadata ( %s ) for the given dataframe is not '
            'present in the catalog', property_name)

    # Return the requested property for the input DataFrame
    return catalog.get_property(data_frame, property_name)


def set_property(data_frame, property_name, property_value):
    """
    Sets a property (with the given property name) for a pandas DataFrame in
    the Catalog.

    Args:
        data_frame (DataFrame): DataFrame for which the property must  be set.
        property_name (str): Name of the property to be set.
        property_value (object): Value of the property to be set. This is
            typically a string (such as key) or pandas DataFrame (such as
            ltable, rtable).

    Returns:
        A boolean value of True is returned if the update was successful.

    Raises:
        AssertionError: If the input object is not of type pandas DataFrame.
        AssertionError: If the property name is not of type string.

    Note:
        If the input DataFrame is not present in the catalog, this function
        will create an entry in the catalog and set the given property.

    """
    # Validate input parameters

    # # The input object is expected to be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    # # The property name is expected to be of type string.
    if not isinstance(property_name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the DataFrame information is present in the catalog. If the
    # information is not present, then initialize an entry for that DataFrame
    #  in the catalog.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        catalog.init_properties(data_frame)

    # Set the property in the catalog, and relay the return value from the
    # underlying catalog object's function. The return value is typically
    # True if the update was successful.
    return catalog.set_property(data_frame, property_name, property_value)


def init_properties(data_frame):
    """
    Initializes properties for a pandas DataFrame in the catalog.

    Specifically, this function creates an entry in the catalog and sets its
    properties to empty.

    Args:
        data_frame (DataFrame): DataFrame for which the properties must be
            initialized.

    Returns:
        A boolean value of True is returned if the initialization was
        successful.

    """
    # Validate input parameters

    # # Input object is expected to be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Initialize the property in the catalog.
    # Relay the return value from the underlying catalog object's function.
    # The return value is typically True if the initialization was successful
    return catalog.init_properties(data_frame)


def get_all_properties(data_frame):
    """
    Gets all the properties for a pandas DataFrame object from the catalog.

    Args:
        data_frame (DataFrame): DataFrame for which the properties must be
            retrieved.

    Returns:
        A dictionary containing properties for the input pandas DataFrame.

    Raises:
        AttributeError: If the input object is not of type pandas DataFrame.
        KeyError: If the information about DataFrame is not present in the
            catalog.


    """
    # Validate input parameters
    # # The input object is expected to be of type DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the DataFrame information is present in the catalog. If not
    # raise an error.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    # Retrieve the properties for the DataFrame from the catalog and return
    # it back to the user.
    return catalog.get_all_properties(data_frame)


def del_property(data_frame, property_name):
    """
    Deletes a property for a pandas DataFrame from the catalog.

    Args:
        data_frame (DataFrame): Input DataFrame for which a property must be
            deleted from the catalog.
        property_name (str): Name of the property that should be deleted.

    Returns:
        A boolean value of True is returned if the deletion was successful.

    Raises:
        AssertionError: If the object is not of type pandas DataFrame.
        AssertionError: If the property name is not of type string.
        KeyError: If the DataFrame information is not present in the catalog.
        KeyError: If the requested property for the DataFrame is not present
            in the catalog.
    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # # The input property name is expected to be of type string
    if not isinstance(property_name, six.string_types):
        logger.error('Property name is not of type string')
        raise AssertionError('Property name is not of type string')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the DataFrame information is present in the catalog, if not
    # raise an error.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('DataFrame information is not present in the catalog')
        raise KeyError('DataFrame information is not present in the catalog')

    # Check if the requested property name to be deleted  is present for the
    # DataFrame in the catalog, if not raise an error.
    if not catalog.is_property_present_for_df(data_frame, property_name):
        logger.error('Requested metadata ( %s ) for the given DataFrame is '
                     'not present in the catalog', property_name)
        raise KeyError('Requested metadata ( %s ) for the given DataFrame is '
                       'not present in the catalog', property_name)

    # Delete the property using the underlying catalog object and relay the
    # return value. Typically the return value is True if the deletion was
    # successful
    return catalog.del_property(data_frame, property_name)


def del_all_properties(data_frame):
    """
    Deletes all properties for a DataFrame from the catalog.

    Args:
        data_frame (DataFrame): Input DataFrame for which all the properties
            must be deleted from the catalog.

    Returns:
        A boolean of True is returned if the deletion was successful
        from the catalog.

    Raises:
        AssertionError: If the input object is not of type pandas DataFrame.
        KeyError: If the DataFrame information is not present in the catalog.

    Note:
        This method's functionality is not as same as init_properties. Here
        the DataFrame's entry will be removed from the catalog,
        but init_properties will add (if the DataFrame is not present in the
        catalog) and initialize its properties to an empty object (
        specifically, an empty python dictionary).
    """
    # Validations of input parameters
    # # The input object is expected to be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the DataFrame is present in the catalog. If not, raise an error
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    # Call the underlying catalog object's function to delete the properties
    # and relay its return value
    return catalog.del_all_properties(data_frame)


def get_catalog():
    """
    Gets the catalog information for the current session.

    Returns:
        A python dictionary containing the caatalog information.
        Specifically, the dictionary contains id(DataFrame object) as the key
        and their properties as value.
    """
    # Get the catalog instance
    catalog = Catalog.Instance()
    # Call the underlying catalog object's function to get the catalog. Relay
    # the return value from the delegated function.
    return catalog.get_catalog()


def del_catalog():
    """
    Deletes the catalog for the current session.

    Returns:
        A boolean value of True is returned if the deletion was successful.
    """
    # Get the catalog instance
    catalog = Catalog.Instance()
    # Call the underlying catalog object's function to delete the catalog (a
    # dict).  Relay the return value from the delegated function.
    return catalog.del_catalog()


def is_catalog_empty():
    """
    Checks if the catalog is empty.

    Returns:
        A boolean value of True is returned if the catalog is empty,
        else returns False.

    """
    # Get the catalog instance
    catalog = Catalog.Instance()

    # Call the underlying catalog object's function to check if the catalog
    # is empty.  Relay the return value from the delegated function.
    return catalog.is_catalog_empty()


def is_dfinfo_present(data_frame):
    """
    Checks whether the DataFrame information is present in the catalog.

    Args:
        data_frame (DataFrame): DataFrame that should be checked for its
            presence in the catalog.

    Returns:
        A boolean value of True is returned if the DataFrame is present in
        the catalog, else False is returned.

    Raises:
        AssertionError: If the input object is not of type pandas DataFrame.

    """
    # Validate inputs
    # We expect the input object to be of type pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas data frame')
        raise AssertionError('Input object is not of type pandas data frame')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Call the underlying catalog object's function to check if the
    # DataFrame information is present in the catalog.
    # Relay the return value from the delegated function.
    return catalog.is_df_info_present_in_catalog(data_frame)


def is_property_present_for_df(data_frame, property_name):
    """
    Checks if the given property is present for the given DataFrame in the
    catalog.

    Args:
        data_frame (DataFrame): DataFrame for which the property must be
            retrieved.
        property_name (str): Name of the property that should be checked for
            its presence for the DataFrame, in the catalog.

    Returns:
        A boolean value of True is returned if the property is present for
        the given DataFrame.

    Raises:
        AssertionError: If the input object is not of type pandas DataFrame.
        AssertionError: If the input property name is not of type string.
        KeyError: If the input DataFrame is not present in the catalog.
    """
    # Input validations

    # # We expect the input object to be of type pandas DataFrame.
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # # The input property name should be of type string
    if not isinstance(property_name, six.string_types):
        logger.error('The property name is not of type string.')
        raise AssertionError('The property name is not of type string.')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the given DataFrame information is present in the catalog. If
    # not, raise an error.
    if catalog.is_df_info_present_in_catalog(data_frame) is False:
        logger.error('Dataframe information is not present in the catalog')
        raise KeyError('Dataframe information is not present in the catalog')

    # Call the underlying catalog object's function to check if the property
    # is present for the given DataFrame. Relay the return value from that
    # function.
    return catalog.is_property_present_for_df(data_frame, property_name)


def get_catalog_len():
    """
    Get the length (i.e the number of entries) in the catalog.

    Returns:
        The number of entries in the catalog as an integer.

    """
    # Get the catalog instance
    catalog = Catalog.Instance()
    # Call the underlying catalog object's function to get the catalog length.
    # Relay the return value from that function.
    return catalog.get_catalog_len()


def set_properties(data_frame, properties, replace=True):
    """
    Sets the  properties for a DataFrame in the catalog.

    Args:
        data_frame (DataFrame): DataFrame for which the properties must be set.
        properties (dict): A python dictionary with keys as property names and
            values as python objects (typically strings or DataFrames)
        replace (Optional[bool]): Flag to indicate whether the  input
            properties can replace the properties in the catalog. The default
            value for the flag is True.
            Specifically, if the DataFrame information is already present in
            the catalog then the function will check if the replace flag is
            True. If the flag is set to True, then the function will first
            delete the existing properties, set it with the given properties.
            If the flag is False, the function will just return without
            modifying the existing properties.


    Returns:
        A boolean value of True is returned if the properties were set for
        the given DataFrame, else returns False.

    Raises:
        AssertionError: If the input data_frame object is not of type pandas
            DataFrame.
        AssertionError: If the input properties object is not of type python
            dictionary.

    """
    # Validate input parameters
    # # Input object is expected to be a pandas DataFrame
    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    # # Input properties is expected to be of type python dictionary
    if not isinstance(properties, dict):
        logger.error('The properties should be of type python dictionary')
        raise AssertionError(
            'The properties should be of type python dictionary')

    # Get the catalog instance
    catalog = Catalog.Instance()
    # Check if the the DataFrame information is present in the catalog. If
    # present, we expect the replace flag to be True. If the flag was set to
    # False, then warn the user and return False.
    if catalog.is_df_info_present_in_catalog(data_frame):
        if not replace:
            logger.warning(
                'Properties already exists for df ( %s ). Not replacing it',
                str(id(data_frame)))
            return False
        else:
            # DataFrame information is present and replace flag is True. We
            # now reset the properties dictionary for this DataFrame.
            catalog.init_properties(data_frame)
    else:
        # The DataFrame information is not present in the catalog. so
        # initialize the properties
        catalog.init_properties(data_frame)

    # Now iterate through the given properties and set for the DataFrame.
    # Note: Here we dont check the correctness of the input properties (i.e
    # we do not check if a property 'key' is indeed a key)
    for property_name, property_value in six.iteritems(properties):
        catalog.set_property(data_frame, property_name, property_value)

    # Finally return True, if everything was successful
    return True


def copy_properties(source_data_frame, target_data_frame, replace=True):
    """
    Copies properties from a source DataFrame to target DataFrame in the
    catalog.

    Args:
        source_data_frame (DataFrame): DataFrame from which the properties
            to be copied from, in the catalog.
        target_data_frame (DataFrame): DataFrame to which the properties to be
            copied to, in the catalog.
        replace (Optional[bool]): Flag to indicate whether the source
            DataFrame's  properties can replace the target
            DataFrame's properties in the catalog. The default value for the
            flag is True.
            Specifically, if the target DataFrame's information is already
            present in the catalog then the function will check if the
            replace flag is True. If the flag is set to True, then the
            function will first delete the existing properties and then set
            it with the source DataFrame properties.
            If the flag is False, the function will just return without
            modifying the existing properties.

    Returns:
        A boolean value of True is returned if the copying was successful.

    Raises:
        AssertionError: If the input object (source_data_frame) is not of
            type pandas DataFrame.
        AssertionError: If the input object (target_data_frame) is not of
            type pandas DataFrame.
        KeyError: If the source DataFrame  is not present in the
            catalog.


    """
    # Validate input parameters

    # # The source_data_frame is expected to be of type pandas DataFrame
    if not isinstance(source_data_frame, pd.DataFrame):
        logger.error('Input object (source_data_frame) is not of type pandas '
                     'DataFrame')
        raise AssertionError(
            'Input object (source_data_frame) is not of type pandas DataFrame')

    # # The target_data_frame is expected to be of type pandas DataFrame
    if not isinstance(target_data_frame, pd.DataFrame):
        logger.error('Input object (target_data_frame) is not of type pandas '
                     'DataFrame')
        raise AssertionError('Input object (target_data_frame) is not  of '
                             'type pandas DataFrame')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the source DataFrame information is present in the catalog. If
    #  not raise an error.
    if catalog.is_df_info_present_in_catalog(source_data_frame) is False:
        logger.error(
            'DataFrame information (source_data_frame) is not present in the '
            'catalog')
        raise KeyError(
            'DataFrame information (source_data_frame) is not present in the '
            'catalog')

    # Get all properties for the source DataFrame
    metadata = catalog.get_all_properties(source_data_frame)

    # Set the properties to the target DataFrame. Specifically, call the set
    # properties function and relay its return value.

    # Note: There is a redundancy in validating the input parameters. This
    # might have a slight performance impact, but we don't expect that this
    # function gets called so often.
    return set_properties(target_data_frame, metadata,
                          replace)  # this initializes tar in the catalog.


# key related methods
def get_key(data_frame):
    """
    Gets the 'key' property for a DataFrame from the catalog.

    Args:
        data_frame (DataFrame): DataFrame for which the key must be retrieved
            from the catalog.

    Returns:
        A string value containing the key column name is returned (if present).

    Raises:
        This function calls get_properties internally, and get_properties
        raises the following exceptions:
        AssertionError: If the object is not of type pandas DataFrame.
        AssertionError: If the property name is not of type string.
        KeyError: If the DataFrame information is not present in the catalog.
        KeyError: If the requested property for the DataFrame is not present
            in the catalog.

    """
    # This function is just a sugar to get the 'key' property for a DataFrame
    return get_property(data_frame, 'key')


def set_key(data_frame, key_attribute):
    """
    Sets the 'key' property for a DataFrame in the catalog with the given
    attribute (i.e column name).
    Specifically, this function set the the key attribute for the DataFrame
    if the given attribute satisfies the following two properties:
        * The key attribute should have unique values.
        * The key attribute should not have missing values. A missing value
        is represented as np.NaN.

    Args:
        data_frame (DataFrame): DataFrame for which the key must be set in
            the catalog.
        key_attribute (str): Key attribute (column name) in the DataFrame.

    Returns:
        A boolean value of True was successful if the given attribute
        satisfies the conditions for a key and the update was successful.

    Raises:
        AssertionError: If the input object (data_frame) is not of type
            pandas DataFrame.
        AssertionError: If the input key_attribute is not of type string.
        KeyError: If the given key attribute is not in the DataFrame columns.


    """

    if not isinstance(data_frame, pd.DataFrame):
        logger.error('Input object is not of type pandas DataFrame')
        raise AssertionError('Input object is not of type pandas DataFrame')

    if not isinstance(key_attribute, six.string_types):
        logger.error('Input key attribute is not of type string')

    if not ch.check_attrs_present(data_frame, key_attribute):
        logger.error('Input key ( %s ) not in the dataframe' % key_attribute)
        raise KeyError('Input key ( %s ) not in the dataframe' % key_attribute)

    if ch.is_key_attribute(data_frame, key_attribute) is False:
        logger.warning(
            'Attribute (' + key_attribute + ') does not qualify to be a key; '
                                  'Not setting/replacing the key')
        return False
    else:
        return set_property(data_frame, 'key', key_attribute)


# def gentle_set_key(df, key):
#     """
#     Set the key attribute for a dataframe
#
#     Args:
#         df (pandas dataframe): Dataframe for which the key must be set
#         key (str): Key attribute in the dataframe
#
#     Returns:
#         status (bool). Returns True if the key attribute was set successfully,
# else returns False
#
#     """
#
#     if not isinstance(df, pd.DataFrame):
#         logger.error('Input object is not of type pandas data frame')
#         raise AssertionError('Input object is not of type pandas data frame')
#
#     if not key in df.columns:
#         logger.warning('Input key ( %s ) not in the dataframe' %key)
#         return False
#
#     if ch.is_key_attribute(df, key) is False:
#         logger.warning('Attribute (' + key + ') does not qualify to be a key;
# Not setting/replacing the key')
#         return False
#     else:
#         return set_property(df, 'key', key)



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
        logger.error('Input attr. ( %s ) not in the dataframe' % fk_ltable)
        raise KeyError('Input attr. ( %s ) not in the dataframe' % fk_ltable)

    return set_property(df, 'fk_ltable', fk_ltable)


def validate_and_set_fk_ltable(df_foreign, fk_ltable, ltable, l_key):
    # validations are done inside the check_fk_constraint fn.
    status = ch.check_fk_constraint(df_foreign, fk_ltable, ltable, l_key)
    if status == True:
        return set_property(df_foreign, 'fk_ltable', fk_ltable)
    else:
        logger.warning(
            'FK constraint for ltable and fk_ltable is not satisfied; Not setting the '
            'fk_ltable and ltable')
        return False


def validate_and_set_fk_rtable(df_foreign, fk_rtable, rtable, r_key):
    # validations are done inside the check_fk_constraint fn.
    status = ch.check_fk_constraint(df_foreign, fk_rtable, rtable, r_key)
    if status == True:
        return set_property(df_foreign, 'fk_rtable', fk_rtable)
    else:
        logger.warning(
            'FK constraint for rtable and fk_rtable is not satisfied; Not setting the fk_rtable and rtable')
        return False


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
        logger.error('Input attr. ( %s ) not in the dataframe' % fk_rtable)
        raise KeyError('Input attr. ( %s ) not in the dataframe' % fk_rtable)

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

    diff_elts = set(reqd_metadata).difference(d)
    if len(diff_elts) != 0:
        logger.error('All the required metadata is not present in the catalog')
        raise AssertionError(
            'All the required metadata is not present in the catalog')

    for m in reqd_metadata:
        if m in d:
            metadata[m] = d[m]
    return metadata


def _update_reqd_metadata_with_kwargs(metadata, kwargs_dict, reqd_metadata):
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

    diff_elts = set(reqd_metadata).difference(kwargs_dict.keys())
    if len(diff_elts) != 0:
        logger.error('All the required metadata is not present in the catalog')
        raise AssertionError(
            'All the required metadata is not present in the catalog')

    for m in reqd_metadata:
        if m in kwargs_dict:
            metadata[m] = kwargs_dict[m]
    return metadata


def _get_diff_with_reqd_metadata(metadata, reqd_metadata):
    """
    Find what metadata is missing from the required list

    Args:
        metadata (dict): Property dictionary
        reqd_metadata (list): List of properties

    Returns:
        diff list (list) of properties between the property dictionary and the properties
        list

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

    d = _get_diff_with_reqd_metadata(metadata, reqd_metadata)
    if len(d) == 0:
        return True
    else:
        return False


def show_properties(df):
    if not is_dfinfo_present(df):
        logger.error('Dataframe information is not present in the catalog')
        return
    metadata = get_all_properties(df)
    print('id: ' + str(id(df)))
    for prop, value in six.iteritems(metadata):
        if isinstance(value, six.string_types):
            print(prop + ": " + value)
        else:
            print(prop + "(obj.id): " + str(id(value)))


def show_properties_for_id(obj_id):
    catalog = Catalog.Instance()
    metadata = catalog.get_all_properties_for_id(obj_id)
    print('id: ' + str(obj_id))
    for prop, value in six.iteritems(metadata):
        if isinstance(value, six.string_types):
            print(prop + ": " + value)
        else:
            print(prop + "(obj.id): " + str(id(value)))


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
        logger.error('Input key ( %s ) not in the dataframe' % key)
        raise KeyError('Input key ( %s ) not in the dataframe' % key)

    ch.log_info(lgr, 'Validating ' + out_str + ' key: ' + str(key), verbose)
    assert isinstance(key,
                      six.string_types) is True, 'Key attribute must be a string.'
    assert ch.check_attrs_present(table,
                                  key) is True, 'Key attribute is not present in the ' + out_str + ' table'
    assert ch.is_key_attribute(table, key, verbose) == True, 'Attribute ' + str(
        key) + \
                                                             ' in the ' + out_str + ' table ' \
                                                                                    'does not qualify to be the key'
    ch.log_info(lgr, '..... Done', verbose)
    return True


def validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable,
                                  rtable,
                                  l_key, r_key,
                                  lgr, verbose):
    if not isinstance(candset, pd.DataFrame):
        logger.error('Input cand.set is not of type pandas data frame')
        raise AssertionError('Input cand.set is not of type pandas data frame')

    if not key in candset.columns:
        logger.error('Input key ( %s ) not in the dataframe' % key)
        raise KeyError('Input key ( %s ) not in the dataframe' % key)

    if not fk_ltable in candset.columns:
        logger.error('Input fk_ltable ( %s ) not in the dataframe' % fk_ltable)
        raise KeyError(
            'Input fk_ltable ( %s ) not in the dataframe' % fk_ltable)

    if not fk_rtable in candset.columns:
        logger.error('Input fk_rtable ( %s ) not in the dataframe' % fk_rtable)
        raise KeyError(
            'Input fk_rtable ( %s ) not in the dataframe' % fk_rtable)

    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas data frame')
        raise AssertionError('Input ltable is not of type pandas data frame')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas data frame')
        raise AssertionError('Input rtable is not of type pandas data frame')

    if not l_key in ltable:
        logger.error('ltable key ( %s ) not in ltable' % l_key)
        raise KeyError('ltable key ( %s ) not in ltable' % l_key)

    if not r_key in rtable:
        logger.error('rtable key ( %s ) not in rtable' % r_key)
        raise KeyError('rtable key ( %s ) not in rtable' % r_key)

    validate_metadata_for_table(candset, key, 'cand.set', lgr, verbose)

    ch.log_info(lgr, 'Validating foreign key constraint for left table',
                verbose)
    assert ch.check_fk_constraint(candset, fk_ltable, ltable,
                                  l_key) == True, 'Cand.set does not satisfy foreign key ' \
                                                  'constraint with the left table'
    ch.log_info(lgr, '..... Done', verbose)
    ch.log_info(lgr, 'Validating foreign key constraint for right table',
                verbose)
    assert ch.check_fk_constraint(candset, fk_rtable, rtable,
                                  r_key) == True, 'Cand.set does not satisfy foreign key ' \
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
