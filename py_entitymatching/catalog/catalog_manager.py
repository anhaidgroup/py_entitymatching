# coding=utf-8
"""
This module contains wrapper functions for the catalog.
"""
import logging

import pandas as pd
import six

import py_entitymatching.utils.catalog_helper as ch
from py_entitymatching.catalog.catalog import Catalog
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def get_property(data_frame, property_name):
    """
    Gets the value of a property (with the given property name) for a pandas
    DataFrame from the catalog.

    Args:
        data_frame (DataFrame): The DataFrame for which the property should be
            retrieved.
        property_name (string): The name of the property that should be
            retrieved.

    Returns:
        A Python object (typically a string or a pandas DataFrame depending
        on the property name) is returned.

    Raises:
        AssertionError: If `data_frame` is not of type pandas
         DataFrame.
        AssertionError: If `property_name` is not of type string.
        KeyError: If `data_frame` information is not present in the catalog.
        KeyError: If requested property for the `data_frame` is not present
            in the catalog.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.get_property(A, 'key')
         # id
    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # The property name should be of type string
    validate_object_type(property_name, six.string_types, error_prefix='Property name')

    # Get the catalog instance, this is imported here because this object
    # used to validate the presence of a DataFrame in the catalog, and the
    # presence of requested metadata in the catalog.
    catalog = Catalog.Instance()

    # Check for the present of input DataFrame in the catalog.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('DataFrame information is not present in the catalog')
        raise KeyError('DataFrame information is not present in the catalog')

    # Check if the requested property is present in the catalog.
    if not catalog.is_property_present_for_df(data_frame, property_name):
        logger.error(
            'Requested metadata ( %s ) for the given DataFrame is not '
            'present in the catalog' % property_name)
        raise KeyError(
            'Requested metadata ( %s ) for the given DataFrame is not '
            'present in the catalog' % property_name)

    # Return the requested property for the input DataFrame
    return catalog.get_property(data_frame, property_name)


def set_property(data_frame, property_name, property_value):
    """
    Sets the value of a property (with the given property name) for a pandas
    DataFrame in the catalog.

    Args:
        data_frame (DataFrame): The DataFrame for which the property must  be
            set.
        property_name (string): The name of the property to be set.
        property_value (object): The value of the property to be set. This is
            typically a string (such as key) or pandas DataFrame (such as
            ltable, rtable).

    Returns:
        A Boolean value of True is returned if the update was successful.

    Raises:
        AssertionError: If `data_frame` is not of type pandas
         DataFrame.
        AssertionError: If `property_name` is not of type string.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_property(A, 'key', 'id')
        >>> em.get_property(A, 'key')
         # id
        >>> em.get_key(A)
         # id


    Note:
        If the input DataFrame is not present in the catalog, this function
        will create an entry in the catalog and set the given property.

    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # The property name should be of type string
    validate_object_type(property_name, six.string_types, error_prefix='Property name')

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
        A Boolean value of True is returned if the initialization was
        successful.

    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

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
    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the DataFrame information is present in the catalog. If not
    # raise an error.
    if not catalog.is_df_info_present_in_catalog(data_frame):
        logger.error('DataFrame information is not present in the catalog')
        raise KeyError('DataFrame information is not present in the catalog')

    # Retrieve the properties for the DataFrame from the catalog and return
    # it back to the user.
    return catalog.get_all_properties(data_frame)


def del_property(data_frame, property_name):
    """
    Deletes a property for a pandas DataFrame from the catalog.

    Args:
        data_frame (DataFrame): The input DataFrame for which a property must be
            deleted from the catalog.

        property_name (string): The name of the property that should be deleted.

    Returns:
        A Boolean value of True is returned if the deletion was successful.

    Raises:
        AssertionError: If `data_frame` is not of type pandas DataFrame.
        AssertionError: If `property_name` is not of type string.
        KeyError: If `data_frame` information is not present in the catalog.
        KeyError: If requested property for the DataFrame is not present
            in the catalog.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_property(A, 'key', 'id')
        >>> em.get_property(A, 'key')
        # id
        >>> em.del_property(A, 'key')
        >>> em.is_property_present_for_df(A, 'key')
        # False

    """
    # Validate input parameters

    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # The property name should be of type string
    validate_object_type(property_name, six.string_types, error_prefix='Property name')

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
                     'not present in the catalog' %property_name)
        raise KeyError('Requested metadata ( %s ) for the given DataFrame is '
                       'not present in the catalog' %property_name)

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
        AssertionError: If the `data_frame` is not of type pandas DataFrame.
        KeyError: If the DataFrame information is not present in the catalog.

    Note:
        This method's functionality is not as same as init_properties. Here
        the DataFrame's entry will be removed from the catalog,
        but init_properties will add (if the DataFrame is not present in the
        catalog) and initialize its properties to an empty object (
        specifically, an empty Python dictionary).
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
        logger.error('DataFrame information is not present in the catalog')
        raise KeyError('DataFrame information is not present in the catalog')

    # Call the underlying catalog object's function to delete the properties
    # and relay its return value
    return catalog.del_all_properties(data_frame)


def get_catalog():
    """
    Gets the catalog information for the current session.

    Returns:
        A Python dictionary containing the catalog information.

        Specifically, the dictionary contains the Python identifier of a
        DataFrame (obtained by id(DataFrame object)) as the key
        and their properties as value.

    Examples:
        >>> import py_entitymatching as em
        >>> catalog = em.get_catalog()

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
        A Boolean value of True is returned if the deletion was successful.

    Examples:
        >>> import py_entitymatching as em
        >>> em.del_catalog()
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
        A Boolean value of True is returned if the catalog is empty,
        else returns False.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.is_catalog_empty()
         # False

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
        data_frame (DataFrame): The DataFrame that should be checked for its
            presence in the catalog.

    Returns:
        A Boolean value of True is returned if the DataFrame is present in
        the catalog, else False is returned.

    Raises:
        AssertionError: If `data_frame` is not of type pandas
         DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.is_dfinfo_present(A)
         # True

    """
    # Validate inputs
    # We expect the input object to be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

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
        data_frame (DataFrame): The DataFrame for which the property must be
            checked for.
        property_name (string): The name of the property that should be
        checked for its presence for the DataFrame, in the catalog.

    Returns:
        A Boolean value of True is returned if the property is present for
        the given DataFrame.

    Raises:
        AssertionError: If `data_frame` is not of type pandas
         DataFrame.
        AssertionError: If `property_name` is not of type string.
        KeyError: If `data_frame` is not present in the catalog.

    Examples:

        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.is_property_present_for_df(A, 'id')
         # True
        >>> em.is_property_present_for_df(A, 'fk_ltable')
         # False

    """
    # Input validations

    # # The input object should be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # The property name should be of type string
    validate_object_type(property_name, six.string_types, error_prefix='Property name')

    # Get the catalog instance
    catalog = Catalog.Instance()

    # Check if the given DataFrame information is present in the catalog. If
    # not, raise an error.
    if catalog.is_df_info_present_in_catalog(data_frame) is False:
        logger.error('DataFrame information is not present in the catalog')
        raise KeyError('DataFrame information is not present in the catalog')

    # Call the underlying catalog object's function to check if the property
    # is present for the given DataFrame. Relay the return value from that
    # function.
    return catalog.is_property_present_for_df(data_frame, property_name)


def get_catalog_len():
    """
    Get the length (i.e the number of entries) in the catalog.

    Returns:
        The number of entries in the catalog as an integer.

    Examples:
        >>> import py_entitymatching as em
        >>> len = em.get_catalog_len()

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
        properties (dict): A Python dictionary with keys as property names and
            values as Python objects (typically strings or DataFrames)
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
        A Boolean value of True is returned if the properties were set for
        the given DataFrame, else returns False.

    Raises:
        AssertionError: If the input data_frame object is not of type pandas
            DataFrame.
        AssertionError: If the input properties object is not of type Python
            dictionary.

    """
    # Validate input parameters
    # # Input object is expected to be a pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # Input properties is expected to be of type Python dictionary
    validate_object_type(properties, dict, error_prefix='The properties')

    # Get the catalog instance
    catalog = Catalog.Instance()
    # Check if the the DataFrame information is present in the catalog. If
    # present, we expect the replace flag to be True. If the flag was set to
    # False, then warn the user and return False.
    if catalog.is_df_info_present_in_catalog(data_frame):
        if not replace:
            logger.warning(
                'Properties already exists for df ( %s ). Not replacing it'
                %str(id(data_frame)))
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
    # Note: Here we don't check the correctness of the input properties (i.e
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
        source_data_frame (DataFrame): The DataFrame from which the properties
            to be copied from, in the catalog.
        target_data_frame (DataFrame): The DataFrame to which the properties
            to be copied to, in the catalog.
        replace (boolean): A flag to indicate whether the source
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
        A Boolean value of True is returned if the copying was successful.

    Raises:
        AssertionError: If `source_data_frame` is not of
            type pandas DataFrame.
        AssertionError: If `target_data_frame` is not of
            type pandas DataFrame.
        KeyError: If source DataFrame is not present in the
            catalog.

    Examples:

        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.copy_properties(A, B)
        >>> em.get_key(B)
        # 'id'
    """
    # Validate input parameters

    # # The source_data_frame is expected to be of type pandas DataFrame
    validate_object_type(source_data_frame, pd.DataFrame, error_prefix='Input object (source_data_frame)')

    # # The target_data_frame is expected to be of type pandas DataFrame
    validate_object_type(target_data_frame, pd.DataFrame, error_prefix='Input object (target_data_frame)')

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
    Gets the value of 'key' property for a DataFrame from the catalog.

    Args:
        data_frame (DataFrame): The DataFrame for which the key must be
            retrieved from the catalog.

    Returns:
        A string value containing the key column name is returned (if present).

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.get_key(A)
        # 'id'


    See Also:
        :meth:`~py_entitymatching.get_property`

    """
    # This function is just a sugar to get the 'key' property for a DataFrame
    return get_property(data_frame, 'key')


def set_key(data_frame, key_attribute):
    """
    Sets the value of 'key' property for a DataFrame in the catalog with the
    given attribute (i.e column name).

    Specifically, this function set the the key attribute for the DataFrame
    if the given attribute satisfies the following two properties:

        The key attribute should have unique values.

        The key attribute should not have missing values. A missing value
        is represented as np.NaN.

    Args:
        data_frame (DataFrame): The DataFrame for which the key must be set in
            the catalog.
        key_attribute (string): The key attribute (column name) in the
            DataFrame.

    Returns:
        A Boolean value of True is returned, if the given attribute
        satisfies the conditions for a key and the update was successful.

    Raises:
        AssertionError: If `data_frame` is not of type
            pandas DataFrame.
        AssertionError: If `key_attribute` is not of type string.
        KeyError: If given `key_attribute` is not in the DataFrame columns.

    Examples:
        >>> import py_entitymatching as em
        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.get_key(A)
        # 'id'


    See Also:
        :meth:`~py_entitymatching.set_property`


    """
    # Validate input parameters

    # # We expect the input object (data_frame) to be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # We expect input key attribute to be of type string
    validate_object_type(key_attribute, six.string_types, error_prefix='Input key attribute')

    # Check if the key attribute is present as one of the columns in the
    # DataFrame
    if not ch.check_attrs_present(data_frame, key_attribute):
        logger.error('Input key ( %s ) not in the DataFrame' % key_attribute)
        raise KeyError('Input key ( %s ) not in the DataFrame' % key_attribute)

    # Check if the key attribute satisfies the conditions to be a key. If
    # not, just return False.
    # Note: Currently it is not clear, whether we should return False from
    # here or raise an exception. As of now resorting to just returning
    # False, because this function is used by other computation
    # intensive commands in py_entitymatching and raising an exception might make all
    # the work done in those commands go in vain (or those commands should
    # catch the exception correctly, which may be complicated and require
    # changes to the current code). We need to revisit this
    # later.
    if ch.is_key_attribute(data_frame, key_attribute) is False:
        logger.warning('Attribute (%s ) does not qualify  to be a key; Not '
                       'setting/replacing the key' % key_attribute)
        return False
    else:
        # Set the key property for the input DataFrame
        return set_property(data_frame, 'key', key_attribute)


def get_fk_ltable(data_frame):
    """
    Gets the foreign key to left table for a DataFrame from the
    catalog.

    Specifically this function is a sugar function that will get the foreign
    key to left table using underlying :meth:`~py_entitymatching.get_property` function.
    This function is typically called on a DataFrame which contains metadata
    such as fk_ltable, fk_rtable, ltable, rtable.


    Args:
        data_frame (DataFrame): The input DataFrame for which the foreign key
            ltable property must be retrieved.

    Returns:
        A Python object, typically a string is returned.


    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_fk_ltable(C, 'ltable_id')
        >>> em.get_fk_ltable(C)
        # 'ltable_id'

    See Also:
        :meth:`~py_entitymatching.get_property`

    """
    # Call the get_property function and relay the result.
    return get_property(data_frame, 'fk_ltable')


def get_fk_rtable(data_frame):
    """
    Gets the foreign key to right table for a DataFrame from the catalog.

    Specifically this function is a sugar function that will get the foreign
    key to right table using :meth:`py_entitymatching.get_property` function. This
    function is typically called on a DataFrame which contains metadata such as
    fk_ltable, fk_rtable, ltable, rtable.

    Args:
        data_frame (DataFrame): The input DataFrame for which the foreign key
            rtable property must be retrieved.

    Returns:
        A Python object, (typically a string) is returned.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_fk_rtable(C, 'rtable_id')
        >>> em.get_fk_rtable(C)
        # 'rtable_id'


    See Also:
        :meth:`~py_entitymatching.get_property`
    """
    # Call the get_property function and relay the result.
    return get_property(data_frame, 'fk_rtable')


def set_fk_ltable(data_frame, fk_ltable):
    """
    Sets the foreign key to ltable for a DataFrame in the catalog.

    Specifically this function is a sugar function that will set the foreign
    key to the left table using :meth:`py_entitymatching.set_property` function. This
    function is typically called on a DataFrame which contains metadata such as
    fk_ltable, fk_rtable, ltable, rtable.

    Args:
        data_frame (DataFrame): The input DataFrame for which the foreign key
            ltable property must be set.
        fk_ltable (string): The attribute that must ne set as the foreign key
            to the ltable in the catalog.

    Returns:
        A Boolean value of True is returned if the foreign key to ltable was
        set successfully.

    Raises:
        AssertionError: If `data_frame` is not of type
            pandas DataFrame.
        AssertionError: If `fk_ltable` is not of type
            string.
        AssertionError: If `fk_ltable` is not in the input
            DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_fk_ltable(C, 'ltable_id')
        >>> em.get_fk_ltable(C)
        # 'ltable_id'



    See Also:
        :meth:`~py_entitymatching.set_property`

    """
    # Validate the input parameters
    # # We expect the input object to be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    # # We expect the input fk_ltable to be of type string
    validate_object_type(fk_ltable, six.string_types, error_prefix='The input (fk_ltable)')

    # # The fk_ltable attribute should be one of the columns in the input
    # DataFrame
    if not ch.check_attrs_present(data_frame, fk_ltable):
        logger.error('Input attr. ( %s ) not in the DataFrame' % fk_ltable)
        raise KeyError('Input attr. ( %s ) not in the DataFrame' % fk_ltable)

    # Call the set_property function and relay the result.
    return set_property(data_frame, 'fk_ltable', fk_ltable)


def validate_and_set_fk_ltable(foreign_data_frame, foreign_key_ltable, ltable,
                               ltable_key):
    """
    Validates and set the foreign key ltable for a DataFrame in the the catalog.

    Specifically, given a DataFrame and a foreign key attribute it checks
    for the following conditions to be satisfied for the attribute. First it
    checks that foreign key ltable attribute does not have any missing
    values. Second it checks that the subset of foreign key values,
    have unique values in the primary (base) table.

    Args:
        foreign_data_frame (DataFrame): DataFrame containing the foreign key
            (typically a candidate set, for example output from blocking two
            tables).
        foreign_key_ltable (string): An attribute in the foreign DataFrame
        ltable (DataFrame): Base DataFrame, in which the foreign key
            attribute would form the primary key.
        ltable_key (string): An attribute in the base table
            (typically a primary key attribute).

    Returns:
        A Boolean value of True will be returned if the validation was
        successful and the update was successful in the catalog.
    Raises:
        AssertionError: If the input foreign DataFrame (foreign_data_frame)
            is not of type pandas DataFrame.
        AssertionError: If the foreign key ltable (foreign_key_ltable) is not
            of type string.
        AssertionError: If the input ltable (ltable) is not of type pandas
            DataFrame.
        AssertionError: If the ltable key (ltable_key) is not of type string.


    """

    # check the foreign key constraint
    # # Note all the validations are done inside the function
    # check_fk_constraint
    status = ch.check_fk_constraint(foreign_data_frame, foreign_key_ltable,
                                    ltable, ltable_key)

    # If the validation is successful then set the property
    if status:
        return set_property(foreign_data_frame, 'fk_ltable', foreign_key_ltable)
    else:
        # else report the error and just return False.
        logger.warning(
            'FK constraint for fk_ltable is not satisfied; '
            'Not setting the fk_ltable')
        return False


def validate_and_set_fk_rtable(foreign_data_frame, foreign_key_rtable,
                               rtable, rtable_key):
    """
    Validates and set the foreign key ltable for a DataFrame in the the catalog.

    Specifically, given a DataFrame and a foreign key attribute it checks
    for the following conditions to be satisfied for the attribute. First it
    checks that foreign key rtable attribute does not have any missing
    values. Second it checks that the subset of foreign key values,
    have unique values in the primary (base) table.

    Args:
        foreign_data_frame (DataFrame): DataFrame containing the foreign key
            (typically a candidate set, for example output from blocking two
            tables).
        foreign_key_rtable (string): An attribute in the foreign DataFrame
        rtable (DataFrame): Base DataFrame, in which the foreign key
            attribute would form the primary key.
        rtable_key (string): An attribute in the base table
            (typically a primary key attribute).

    Returns:
        A Boolean value of True will be returned if the validation was
        successful and the update was successful in the catalog.
    Raises:
        AssertionError: If the input foreign DataFrame (foreign_data_frame)
            is not of type pandas DataFrame.
        AssertionError: If the foreign key ltable (foreign_key_ltable) is not
            of type string.
        AssertionError: If the input ltable (ltable) is not of type pandas
            DataFrame.
        AssertionError: If the ltable key (ltable_key) is not of type string.


    """

    # Validate the foreign key constraint
    # Note: All the basic input validations are done inside the
    # check_fk_constraint function.
    status = ch.check_fk_constraint(foreign_data_frame, foreign_key_rtable,
                                    rtable, rtable_key)

    # If the validation was successful, then set the property
    if status:
        return set_property(foreign_data_frame, 'fk_rtable', foreign_key_rtable)
    # else just warn and return False
    else:
        logger.warning(
            'FK constraint for fk_rtable is not satisfied; Not '
            'setting the fk_rtable and rtable')
        return False


def set_fk_rtable(data_frame, foreign_key_rtable):
    """
    Sets the foreign key to rtable for a DataFrame in the catalog.

    Specifically this function is a sugar function that will set the foreign
    key to right table using set_property function. This function
    is typically called on a DataFrame which contains metadata such as
    fk_ltable, fk_rtable, ltable, rtable.



    Args:
        data_frame (DataFrame): The input DataFrame for which the foreign key
            rtable property must be set.
        foreign_key_rtable (string): The attribute that must be set as
            foreign key to rtable in the catalog.

    Returns:
        A Boolean value of True is returned if the foreign key to rtable was
            set successfully.

    Raises:
        AssertionError: If `data_frame` is not of type
          pandas DataFrame.
        AssertionError: If `foreign_key_rtable` is not of
            type string.
        AssertionError: If `fk_rtable` is not in the input
            DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_fk_rtable(C, 'rtable_id')
        >>> em.get_fk_rtable(C)
        # 'rtable_id'


    See Also:
        :meth:`~py_entitymatching.set_property`

    """
    # Validate the input parameters
    # # The input object is expected to be of type pandas DataFrame
    validate_object_type(data_frame, pd.DataFrame)

    validate_object_type(foreign_key_rtable, six.string_types, error_prefix='Input (foreign key ltable)')

    # Check if the given attribute is present in the DataFrame
    if not ch.check_attrs_present(data_frame, foreign_key_rtable):
        logger.error('Input attr. ( %s ) not in the DataFrame'
                     % foreign_key_rtable)
        raise KeyError('Input attr. ( %s ) not in the DataFrame'
                       % foreign_key_rtable)

    # Finally set the property and relay the result
    return set_property(data_frame, 'fk_rtable', foreign_key_rtable)


def show_properties(data_frame):
    """
    Prints the properties for a DataFrame that is present in the catalog.

    Args:
        data_frame (DataFrame): The input pandas DataFrame for which the
            properties must be displayed.

    Examples:
        >>> A = pd.DataFrame({'key_attr' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'key_attr')
        >>> em.show_properties(A)
        # id: 4572922488  # This will change dynamically
        # key: key_attr



    """
    # Check if the DataFrame information is present in the catalog. If not
    # return
    if not is_dfinfo_present(data_frame):
        logger.error('DataFrame information is not present in the catalog')
        return

    # Delegate it to show properties for the id if an object in the catalog
    show_properties_for_id(id(data_frame))
    # # Get the properties for the DataFrame from the catalog
    # metadata = get_all_properties(data_frame)
    #
    # # First print the id for the DataFrame
    # print('id: ' + str(id(data_frame)))
    # # For each property name anf value, print the contents to the user
    # for property_name, property_value in six.iteritems(metadata):
    #     # If the property value is string print it out
    #     if isinstance(property_value, six.string_types):
    #         print(property_name + ": " + property_value)
    #     # else, print just the id.
    #     else:
    #         print(property_name + "(obj.id): " + str(id(property_value)))


def show_properties_for_id(object_id):
    """
    Shows the properties for an object id present in the catalog.

    Specifically, given an object id got from typically executing id(
    <object>), where the object could be a DataFrame, this function will
    display the properties present for that object id in the catalog.

    Args:
        object_id (int): The Python identifier of an object (typically a
         pandas DataFrame).

    Examples:
        >>> A = pd.DataFrame({'key_attr' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'key_attr')
        >>> em.show_properties_for_id(id(A))
        # id: 4572922488  # This will change dynamically
        # key: key_attr


    """
    catalog = Catalog.Instance()
    metadata = catalog.get_all_properties_for_id(object_id)
    # First print the id for the DataFrame
    print('id: ' + str(object_id))
    # For each property name anf value, print the contents to the user
    for property_name, property_value in six.iteritems(metadata):
        # If the property value is string print it out
        if isinstance(property_value, six.string_types):
            print(property_name + ": " + property_value)
        # else, print just the id.
        else:
            print(property_name + "(obj.id): " + str(id(property_value)))


def set_candset_properties(candset, key, foreign_key_ltable,
                           foreign_key_rtable, ltable, rtable):
    """
    Sets candidate set properties.

    Specifically, this is a sugar function that sets all the properties for a
    candidate set such as key, foreign key ltable, foreign key rtable,
    ltable and rtable. Further, this function does not check the integrity of
    input properties.



    Args:
        candset (DataFrame): Input DataFrame for which the properties must be
            set.
        key (string): Key attribute that must be set for the DataFrame in the
            catalog.
        foreign_key_ltable (string): Foreign key ltable attribute that must be
            set for the DataFrame in the catalog.
        foreign_key_rtable (string): Foreign key rtable attribute that must be
            set for the DataFrame in the catalog.
        ltable (DataFrame): DataFrame that must be set as ltable.
        rtable (DataFrame): DataFrame that must be set as rtable.

    Returns:
        A Boolean value of True is returned if the updates were successful.

    """
    # set the key
    set_property(candset, 'key', key)
    # set the foreign key attributes
    set_fk_ltable(candset, foreign_key_ltable)
    set_fk_rtable(candset, foreign_key_rtable)
    # set the ltable and rtables
    set_property(candset, 'ltable', ltable)
    set_property(candset, 'rtable', rtable)
    return True


def _validate_metadata_for_table(table, key, output_string, lgr, verbose):
    """
    Validates metadata for table (DataFrame)

    """
    # Validate input parameters
    # # We expect the input table to be of type pandas DataFrame
    validate_object_type(table, pd.DataFrame)

    # Check the key column is present in the table
    if not ch.check_attrs_present(table, key):
        raise KeyError('Input key ( %s ) not in the DataFrame' % key)

    # Validate the key
    ch.log_info(lgr, 'Validating ' + output_string + ' key: ' + str(key),
                verbose)
    # We expect the key to be of type string
    validate_object_type(key, six.string_types, error_prefix='Key attribute')

    if not ch.is_key_attribute(table, key, verbose):
        raise AssertionError('Attribute %s in the %s table does not '
                             'qualify to be the key' % (
                                 str(key), output_string))
    ch.log_info(lgr, '..... Done', verbose)
    return True


def _validate_metadata_for_candset(candset, key, foreign_key_ltable,
                                   foreign_key_rtable,
                                   ltable, rtable,
                                   ltable_key, rtable_key,
                                   lgr, verbose):
    """
    Validates metadata for a candidate set.

    """
    # Validate input parameters
    # # We expect candset to be of type pandas DataFrame
    validate_object_type(candset, pd.DataFrame, error_prefix='Input candset')

    # Check if the key column is present in the candset
    if not ch.check_attrs_present(candset, key):
        raise KeyError('Input key ( %s ) not in the DataFrame' % key)

    # Check if the foreign key ltable column is present in the candset
    if not ch.check_attrs_present(candset, foreign_key_ltable):
        raise KeyError(
            'Input foreign_key_ltable ( %s ) not in the DataFrame'
            % foreign_key_ltable)

    # Check if the foreign key rtable column is present in the candset
    if not ch.check_attrs_present(candset, foreign_key_rtable):
        raise KeyError(
            'Input fk_rtable ( %s ) not in the DataFrame' % foreign_key_rtable)

    # We expect the ltable to be of type pandas DataFrame
    validate_object_type(ltable, pd.DataFrame, error_prefix='Input ltable')

    # We expect the rtable to be of type pandas DataFrame
    validate_object_type(rtable, pd.DataFrame, error_prefix='Input rtable')

    # We expect the ltable key to be present in the ltable
    if not ch.check_attrs_present(ltable, ltable_key):
        raise KeyError('ltable key ( %s ) not in ltable' % ltable_key)

    # We expect the rtable key to be present in the rtable
    if not ch.check_attrs_present(rtable, rtable_key):
        raise KeyError('rtable key ( %s ) not in rtable' % rtable_key)

    # First validate metadata for the candidate set (as a table)
    _validate_metadata_for_table(candset, key, 'candset', lgr, verbose)

    ch.log_info(lgr, 'Validating foreign key constraint for left table',
                verbose)
    # Second check foreign key constraints
    if not ch.check_fk_constraint(candset, foreign_key_ltable,
                                  ltable, ltable_key):
        raise AssertionError(
            'Candset does not satisfy foreign key constraint with '
            'the left table')

    if not ch.check_fk_constraint(candset, foreign_key_rtable,
                                  rtable, rtable_key):
        raise AssertionError(
            'Candset does not satisfy foreign key constraint with '
            'the right table')

    ch.log_info(lgr, '..... Done', verbose)
    ch.log_info(lgr, 'Validating foreign key constraint for right table',
                verbose)
    ch.log_info(lgr, '..... Done', verbose)

    return True


# noinspection PyIncorrectDocstring
def get_keys_for_ltable_rtable(ltable, rtable, lgr, verbose):
    """
    Gets keys for the ltable and rtable.
    """
    # We expect the ltable to be of type pandas DataFrame
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas data frame')
        raise AssertionError('Input ltable is not of type pandas data frame')

    # We expect the rtable to be of type pandas DataFrame
    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas data frame')
        raise AssertionError('Input rtable is not of type pandas data frame')

    ch.log_info(lgr, 'Required metadata: ltable key, rtable key', verbose)
    ch.log_info(lgr, 'Getting metadata from the catalog', verbose)
    # Get the ltable key and rtable key from the catalog
    ltable_key = get_key(ltable)
    rtable_key = get_key(rtable)
    ch.log_info(lgr, '..... Done', verbose)
    # return the ltable and rtable keys
    return ltable_key, rtable_key


# noinspection PyIncorrectDocstring
def get_metadata_for_candset(candset, lgr, verbose):
    """
    Gets metadata for the candset

    """
    # Validate input parameters
    validate_object_type(candset, pd.DataFrame, error_prefix='Input candset')

    ch.log_info(lgr, 'Getting metadata from the catalog', verbose)
    # Get the key, foreign keys, ltable, rtable and their keys
    # # Get key
    key = get_key(candset)
    # # Get the foreign keys
    fk_ltable = get_fk_ltable(candset)
    fk_rtable = get_fk_rtable(candset)
    # # Get the base tables
    ltable = get_ltable(candset)
    rtable = get_rtable(candset)
    # Get the base table keys
    l_key = get_key(ltable)
    r_key = get_key(rtable)
    ch.log_info(lgr, '..... Done', verbose)
    # Return the metadata
    return key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key


# noinspection PyIncorrectDocstring
def get_ltable(candset):
    """
    Gets the ltable for a DataFrame from the catalog.

    Args:
        candset (DataFrame): The input table for which the ltable must be
            returned.

    Returns:
        A pandas DataFrame that is pointed by 'ltable' property of the input
        table.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_ltable(C, A)
        >>> id(em.get_ltable(A) == id(A)
        # True


    See Also:
        :meth:`~py_entitymatching.get_property`
    """
    # Return the ltable for a candidate set. This function is just a sugar
    return get_property(candset, 'ltable')


# noinspection PyIncorrectDocstring
def get_rtable(candset):
    """
    Gets the rtable for a DataFrame from the catalog.

    Args:
        candset (DataFrame): Input table for which the rtable must be returned.

    Returns:
        A pandas DataFrame that is pointed by 'rtable' property of the input
        table.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_rtable(C, B)
        >>> id(em.get_rtable(B) == id(B)
        # True


    See Also:
        :meth:`~py_entitymatching.get_property`
    """
    # Return the rtable for a candidate set. This function is just a sugar

    return get_property(candset, 'rtable')

def set_ltable(candset, table):
    """
    Sets the ltable for a DataFrame in the catalog.

    Args:
        candset (DataFrame): The input table for which the ltable must be set.
        table (DataFrame): The table (typically a pandas DataFrame) that must
            be set as ltable for the input DataFrame.

    Returns:
        A Boolean value of True is returned, if the update was successful.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_ltable(C, A)
        >>> id(em.get_ltable(A) == id(A)
        # True


    See Also:
        :meth:`~py_entitymatching.set_property`
    """
    # Return the ltable for a candidate set. This function is just a sugar
    return set_property(candset, 'ltable', table)


# noinspection PyIncorrectDocstring
def set_rtable(candset, table):
    """
    Sets the rtable for a DataFrame in the catalog.

    Args:
        candset (DataFrame): The input table for which the rtable must be set.
        table (DataFrame): The table that must be set as rtable for the input
            DataFrame.

    Returns:
        A Boolean value of True is returned, if the update was successful.

    Examples:
        >>> import py_entitymatching as em
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> em.set_key(A, 'id')
        >>> em.set_key(B, 'id')
        >>> C = pd.DataFrame({'id':[1, 2], 'ltable_id':[1, 2], 'rtable_id':[2, 1]})
        >>> em.set_key(C, 'id')
        >>> em.set_rtable(C, B)
        >>> id(em.get_rtable(B) == id(B)
        # True


    See Also:
        :meth:`~py_entitymatching.set_property`
    """
    # Return the rtable for a candidate set. This function is just a sugar

    return set_property(candset, 'rtable', table)
