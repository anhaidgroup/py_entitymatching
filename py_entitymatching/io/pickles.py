# coding=utf-8
""" This module defines the functions to load and save py_entitymatching objects
"""
# coding=utf-8
import collections
import logging
import os
import pickle

import cloudpickle
import pandas as pd
import six

import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.io.parsers as ps
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def save_object(object_to_save, file_path):
    """
    Saves a Python object to disk.

    This function is intended to be used to save py_entitymatching objects such as
    rule-based blocker, feature vectors, etc. A user would like to store
    py_entitymatching objects to disk, when he/she wants to save the workflow and
    resume it later. This function provides a way to save the required
    objects to disk.

    This function takes in the object to save the file path. It pickles the object and
    stores it in the file path specified.

    Args:
        object_to_save (Python object): The Python object to save. This can be
            a rule-based blocker, feature vectors, etc.

        file_path (string): The file path where the object must be saved.


    Returns:
        A Boolean value of True is returned, if the saving was successful.

    Raises:
        AssertionError: If `file_path` is not of type string.
        AssertionError: If a file cannot be written in the given `file_path`.

    Examples:

        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> B = pd.DataFrame({'id' : [1, 2], 'colA':['c', 'd'], 'colB' : [30, 40]})
        >>> rb = em.RuleBasebBlocker()
        >>> block_f = em.get_features_for_blocking(A, B)
        >>> rule1 = ['colA_colA_lev_dist(ltuple, rtuple) > 3']
        >>> rb.add_rule(rule1)
        >>> em.save_object(rb, './rule_blocker.pkl')



    See Also:
        :meth:`~load_object`

    """
    # Validate input parameters

    validate_object_type(file_path, six.string_types, 'Input file path')

    # Check whether the file path is valid and if a file is already present
    # at that path.
    # noinspection PyProtectedMember
    can_write, file_exists = ps._check_file_path(file_path)

    # Check whether we can write
    if can_write:
        # If a file already exists in that location, issue a warning and
        # overwrite the file.
        if file_exists:
            logger.warning(
                'File already exists at %s; Overwriting it', file_path)
            # we open the file in 'wb' mode as we are writing a binary file.
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(object_to_save, file_handler)
        else:
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(object_to_save, file_handler)

    # If we cannot write, then raise an error.
    else:
        logger.error('Cannot write in the file path %s; Exiting', file_path)
        raise AssertionError('Cannot write in the file path %s', file_path)

    # Return True if everything was successful.
    return True


def load_object(file_path):
    """
    Loads a Python object from disk.

    This function loads py_entitymatching objects from disk such as
    blockers, matchers, feature table, etc.

    Args:

        file_path (string): The file path to load the object from.

    Returns:
        A Python object read from the file path.

    Raises:
        AssertionError: If `file_path` is not of type string.
        AssertionError: If a file does not exist at the given `file_path`.

    Examples:

        >>> rb = em.load_object('./rule_blocker.pkl')

    See Also:
        :meth:`~save_object`


    """
    # Validate input parameters

    validate_object_type(file_path, six.string_types, error_prefix='Input file path')

    # Check if a file exists at the given file path.
    if not os.path.exists(file_path):
        logger.error('File does not exist at path %s', file_path)
        raise AssertionError('File does not exist at path', file_path)

    # Read the object from the file.

    # # Open the file with the mode set to binary.
    with open(file_path, 'rb') as file_handler:
        object_to_return = pickle.load(file_handler)

    # Return the object.
    return object_to_return


# noinspection PyProtectedMember
def save_table(data_frame, file_path, metadata_ext='.pklmetadata'):
    """
    Saves a DataFrame to disk along with its metadata in a pickle format.

    This function saves a  DataFrame to disk along with its metadata from
    the catalog.

    Specifically, this function saves the DataFrame in the given
    file path, and saves the metadata in the same directory (as the
    file path) but with a different extension. This extension can be
    optionally given by the user (defaults to '.pklmetadata').

    Args:
        data_frame (DataFrame): The DataFrame that should be saved.

        file_path (string): The file path where the DataFrame must be stored.

        metadata_ext (string): The metadata extension that should be used while
            storing the metadata information. The default value is
            '.pklmetadata'.

    Returns:
        A Boolean value of True is returned if the DataFrame is successfully
        saved.

    Raises:
        AssertionError: If `data_frame` is not of type pandas
         DataFrame.
        AssertionError: If `file_path` is not of type string.
        AssertionError: If `metadata_ext` is not of type string.
        AssertionError: If a file cannot written in the given `file_path`.

    Examples:

        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.save_table(A, './A.pkl') # will store two files ./A.pkl and ./A.pklmetadata

        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.save_table(A, './A.pkl', metadata_ext='.pklmeta') # will store two files ./A.pkl and ./A.pklmeta


    See Also:
        :meth:`~py_entitymatching.load_table`

    Note:
        This function is a bit different from to_csv_metadata, where the
        DataFrame is stored in a CSV file format. The CSV file format can be
        viewed using a text editor. But a DataFrame stored using 'save_table' is
        stored in a special format, which cannot be viewed with a text editor.
        The reason we have save_table is, for larger DataFrames it is
        efficient to pickle the DataFrame to disk than writing the DataFrame
        in CSV format.
    """
    # Validate the input parameters

    validate_object_type(data_frame, pd.DataFrame)

    validate_object_type(file_path, six.string_types, error_prefix='Input file path')

    validate_object_type(metadata_ext, six.string_types, error_prefix='Input Metadata ext')

    # Get the file_name (with out extension) and the extension from the given
    #  file path. For example if the file_path was /Users/foo/file.csv then
    # the file_name will be /Users/foo/file and the extension will be '.csv'
    file_name, _ = os.path.splitext(file_path)

    # The metadata file name is the same file name but with the extension
    # given by the user
    metadata_filename = file_name + metadata_ext

    # Check if the file exists in the file_path and whether we have
    # sufficient access privileges to write in that path
    can_write, file_exists = ps._check_file_path(file_path)

    if can_write:
        # If the file already exists then issue a warning and overwrite the
        # file
        if file_exists:
            logger.warning(
                'File already exists at %s; Overwriting it', file_path)
            # we open the file_path in binary mode, as we are writing in
            # binary format'
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(data_frame, file_handler)
        else:
            #
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(data_frame, file_handler)
    else:
        # Looks like we cannot write the file in the given path. Raise an
        # error in this case.
        logger.error('Cannot write in the file path %s; Exiting', file_path)
        raise AssertionError('Cannot write in the file path %s', file_path)

    # Once we are done with writing the DataFrame, we will write the metadata
    #  now

    # Initialize a metadata dictionary to hold the metadata of DataFrame from
    #  the catalog
    metadata_dict = collections.OrderedDict()

    # get all the properties for the input data frame
    # # Check if the DataFrame information is present in the catalog
    properties = {}
    if cm.is_dfinfo_present(data_frame) is True:
        properties = cm.get_all_properties(data_frame)

    # If the properties are present in the catalog, then write properties to
    # disk
    if len(properties) > 0:
        for property_name, property_value in six.iteritems(properties):
            if isinstance(property_value, six.string_types) is True:
                metadata_dict[property_name] = property_value

    # try to save metadata
    can_write, file_exists = ps._check_file_path(metadata_filename)
    if can_write:
        # If the file already exists, then issue a warning and overwrite the
        # file
        if file_exists:
            logger.warning(
                'Metadata file already exists at %s. Overwriting it',
                metadata_filename)
            # write metadata contents
            with open(metadata_filename, 'wb') as file_handler:
                cloudpickle.dump(metadata_dict, file_handler)
        else:
            # write metadata contents
            with open(metadata_filename, 'wb') as file_handler:
                cloudpickle.dump(metadata_dict, file_handler)
    else:
        logger.warning(
            'Cannot write metadata at the file path %s. Skip writing metadata '
            'file', metadata_filename)

    return True


# noinspection PyProtectedMember
def load_table(file_path, metadata_ext='.pklmetadata'):
    """
    Loads a pickled DataFrame from a file along with its metadata.

    This function loads a DataFrame from a file stored in pickle format.

    Further, this function looks for a metadata file with the same file name
    but with an extension given by the user (defaults to '.pklmetadata'. If the
    metadata file is present, the function will update the metadata for that
    DataFrame in the catalog.

    Args:
        file_path (string): The file path to load the file from.
        metadata_ext (string): The metadata file extension (defaults to
            '.pklmetadata') that should be used to generate metadata file name.

    Returns:
        If the loading is successful, the function will return a pandas
        DataFrame read from the file. The catalog will be updated with the
        metadata read from the metadata file (if the file was present).

    Raises:
        AssertionError: If `file_path` is not of type string.
        AssertionError: If `metadata_ext` is not of type string.


    Examples:

        >>> A = em.load_table('./A.pkl')

        >>> A = em.load_table('./A.pkl', metadata_ext='.pklmeta')


    See Also:
        :meth:`~py_entitymatching.save_table`



    Note:
        This function is different from read_csv_metadata in two aspects.
        First, this function currently does not support reading in candidate
        set tables, where there are more metadata such as ltable,
        rtable than just 'key', and conceptually the user is expected to
        provide ltable and rtable information while calling this function. (
        this support will be added shortly). Second, this function loads the
        table stored in a pickle format.


    """
    # Validate input parameters
    validate_object_type(file_path, six.string_types, error_prefix='Input file path')

    validate_object_type(metadata_ext, six.string_types)

    # Load the object from the file path. Note that we use a generic load
    # object to load in the DataFrame too.
    data_frame = load_object(file_path)

    # Load metadata from file path

    # # Check if the meta data file is present
    if ps._is_metadata_file_present(file_path, extension=metadata_ext):
        # Construct the metadata file name, and read it from the disk.

        # # Get the file name used to load the DataFrame
        file_name, _ = os.path.splitext(file_path)
        # # Construct the metadata file name
        metadata_filename = file_name + metadata_ext
        # # Load the metadata from the disk
        metadata_dict = load_object(metadata_filename)

        # Update the catalog with the properties read from the disk
        for property_name, property_value in six.iteritems(metadata_dict):
            if property_name == 'key':
                # If the property_name is key call set_key as the function
                # will check for the integrity of key before setting it in
                # the catalog
                cm.set_key(data_frame, property_value)
            else:
                cm.set_property(data_frame, property_name, property_value)
    else:
        # If the metadata file is not present then issue a warning
        logger.warning('There is no metadata file')

    # Return the DataFrame
    return data_frame
