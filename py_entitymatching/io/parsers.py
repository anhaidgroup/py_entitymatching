# coding=utf-8
"""This module defines functions to read and write CSV files"""
import collections
import logging
import os
import pandas as pd
import six

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def read_csv_metadata(file_path, **kwargs):
    """
    Reads a CSV (comma-separated values) file into a pandas DataFrame
    and update the catalog with the metadata. The CSV files typically contain
    data for the input tables or a candidate set.

    Specifically, this function first reads the CSV file from the given file
    path into a pandas DataFrame, by using pandas' in-built 'read_csv'
    method. Then, it updates the catalog with the metadata. There are three
    ways to update the metadata: (1) using a metadata file, (2) using the
    key-value parameters supplied in the function, and (3) using both
    metadata file and key-value parameters.

    To update the metadata in the catalog using the metadata file,
    the function will look for a file in the same directory with  same file name
    but with a  specific extension. This extension can be optionally given by
    the user (defaults to '.metadata'). If the metadata  file is  present,
    the function will read and update the catalog appropriately. If  the
    metadata file is not present, the function will issue a warning that the
    metadata file is not present.

    The metadata information can also be given as parameters to the function
    (see description of arguments for more details). If given, the function
    will update the catalog with the given information.

    Further, the metadata can partly reside in the metdata file and partly as
    supplied parameters. The function will take a union of the two and
    update the catalog appropriately.
    If the same metadata is given in both the metadata file
    and the function, then the metadata in the function takes precedence over
    the metadata given in the file.

    Args:
        file_path(string): The CSV file path

        kwargs(dictionary): A Python dictionary containing key-value arguments.
            There are a few key-value pairs that are specific to
            read_csv_metadata and  all the other key-value pairs are passed
            to pandas read_csv method

    Returns:
        A pandas DataFrame read from the input CSV file.
    Raises:
        AssertionError: If `file_path` is not of type string.
        AssertionError: If a file does not exist in the
            given `file_path`.

    Examples:
        *Example 1:* Read from CSV file and set metadata

        >>> A = em.read_csv_metadata('path_to_csv_file', key='id')
        >>> em.get_key(A)
         # 'id'

        *Example 2:*  Read from CSV file (with metadata file in the same directory

         Let the metadata file contain the following contents:

          #key = id

        >>> A = em.read_csv_metadata('path_to_csv_file')
        >>> em.get_key(A)
         # 'id'

    See Also:
        :meth:`~py_entitymatching.to_csv_metadata`
    """
    # Validate the input parameters.

    validate_object_type(file_path, six.string_types, error_prefix='Input file path')

    # # Check if the given path is valid.
    if not os.path.exists(file_path):
        logger.error('File does not exist at path %s' % file_path)
        raise AssertionError('File does not exist at path %s' % file_path)

    # Check if the user has specified the metadata file's extension.
    extension = kwargs.pop('metadata_extn', None)

    # If the extension is not specified then set the extension to .metadata'.
    if extension is None:
        extension = '.metadata'

    # Format the extension to include a '.' in front if the user has not
    # given one.
    if not extension.startswith('.'):
        extension = '.' + extension

    # If the file is present, then update metadata from file.
    if _is_metadata_file_present(file_path, extension=extension):
        file_name, _ = os.path.splitext(file_path)
        file_name = ''.join([file_name, extension])
        metadata, _ = _get_metadata_from_file(file_name)

    # Else issue a warning that the metadata file is not present
    else:
        logger.warning('Metadata file is not present in the given path; '
                       'proceeding to read the csv file.')
        metadata = {}

    # Update the metadata with the key-value pairs given in the command. The
    # function _update_metadata_for_read_cmd takes care of updating the
    # metadata with only the key-value pairs specific to read_csv_metadata
    # method
    metadata, kwargs = _update_metadata_for_read_cmd(metadata, **kwargs)

    # Validate the metadata.
    _check_metadata_for_read_cmd(metadata)

    # Read the csv file using pandas read_csv method.
    data_frame = pd.read_csv(file_path, **kwargs)

    # Get the value for 'key' property and update the catalog.
    key = metadata.pop('key', None)
    if key is not None:
        cm.set_key(data_frame, key)

    fk_ltable = metadata.pop('fk_ltable', None)
    if fk_ltable is not None:
        cm.set_fk_ltable(data_frame, fk_ltable)

    fk_rtable = metadata.pop('fk_rtable', None)
    if fk_ltable is not None:
        cm.set_fk_rtable(data_frame, fk_rtable)

    # Update the catalog with other properties.
    for property_name, property_value in six.iteritems(metadata):
        cm.set_property(data_frame, property_name, property_value)
    if not cm.is_dfinfo_present(data_frame):
        cm.init_properties(data_frame)

    # Return the DataFrame
    return data_frame


def to_csv_metadata(data_frame, file_path, **kwargs):
    """
    Writes the DataFrame contents to a CSV file and the DataFrame's metadata
    (to a separate text file).

    This function writes the DataFrame contents to a CSV file in
    the given file path. It uses 'to_csv' method from pandas to write
    the CSV file. The metadata contents are written to the same directory
    derived from the file path but with the different extension. This
    extension can be optionally given by the user (with the default value
    set to .metadata).

    Args:
        data_frame (DataFrame): The DataFrame that should be written to disk.
        file_path (string):  The file path to which the DataFrame contents
            should be written. Metadata is written with the same file name
            with the extension given by the user (defaults to '.metadata').
        kwargs (dictionary):  A Python dictionary containing key-value pairs.
            There is one key-value pair that is specific to
            to_csv_metadata: metadata_extn. All the other key-value pairs
            are passed to pandas to_csv function.
            Here the metadata_extn is the metadata
            extension (defaults to '.metadata'), with which
            the metadata file must be written.
    Returns:
        A Boolean value of True is returned if the files were written
        successfully.

    Raises:
        AssertionError: If `data_frame` is not of type  pandas
            DataFrame.
        AssertionError: If `file_path` is not of type string.
        AssertionError: If DataFrame cannot be written to the given
         `file_path`.

    Examples:

        >>> import pandas as pd
        >>> A = pd.DataFrame({'id' : [1, 2], 'colA':['a', 'b'], 'colB' : [10, 20]})
        >>> em.set_key(A, 'id')
        >>> em.to_csv_metadata(A, 'path_to_csv_file')


    See Also:
        :meth:`~py_entitymatching.read_csv_metadata`

    """
    # Validate input parameters

    validate_object_type(data_frame, pd.DataFrame)

    validate_object_type(file_path, six.string_types, error_prefix='Input file path')

    # Check if the user has specified the metadata file's extension.
    extension = kwargs.pop('metadata_extn', None)
    if extension is None:
        extension = '.metadata'
    if not extension.startswith('.'):
        extension = '.' + extension

    # If the user has not specified whether the index should be written,
    # we explicitly set it to be false. The reason is writing the index
    # along makes the CSV file cumbersome to view and later read back into a
    # DataFrame.
    index = kwargs.pop('index', None)
    if index is None:
        kwargs['index'] = False

    # retrieve the file name and the extension from the given file path.
    file_name, _ = os.path.splitext(file_path)
    metadata_filename = file_name + extension

    # check if we access privileges to write a file in the given file path,
    # and also check if a file already exists in the file path.
    can_write, file_exists = _check_file_path(file_path)

    if can_write:
        # check if the file already exists. If so issue a warning and
        # overwrite the file.
        if file_exists:
            logger.warning('File already exists at %s; Overwriting it',
                           file_path)
            data_frame.to_csv(file_path, **kwargs)
        else:
            data_frame.to_csv(file_path, **kwargs)
    else:
        # If we cannot write in the given file path, raise an exception.
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    # repeat the process (as writing the DataFrame) to write the metadata.

    # check for access privileges and file existence.
    can_write, file_exists = _check_file_path(metadata_filename)
    if can_write:
        if file_exists:
            logger.warning('Metadata file already exists at %s. Overwriting '
                           'it', metadata_filename)
            _write_metadata(data_frame, metadata_filename)
        else:
            _write_metadata(data_frame, metadata_filename)
    else:
        # If we cannot write in the given file path, raise an exception.
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    return True


def _write_metadata(data_frame, file_path):
    """
    Write metadata contents to disk.
    """
    # Initialize a metadata dictionary to store the metadata.
    metadata_dict = collections.OrderedDict()

    # Get all the properties for the input data frame
    if cm.is_dfinfo_present(data_frame) is True:
        properties = cm.get_all_properties(data_frame)
    else:
        # If the data_frame is not in the catalog, then return immedidately.
        return False

    # If the properties are present in the catalog, then write properties to
    # disk
    if len(properties) > 0:
        for property_name, property_value in six.iteritems(properties):
            # If the property value is not of type string, then just write it
            #  as 'POINTER'. This will be useful while writing the candidate
            # sets to disk. The candidate set will have properties such as
            # ltable and rtable which are DataFrames. We do not have a simple
            # way to write them to disk and link them back the candidate set
            # while reading back from disk. So to get around this problem we
            # will use 'POINTER' as the special value to indicate objects
            # other than strings.
            if isinstance(property_value, six.string_types) is False:
                metadata_dict[property_name] = 'POINTER'
            else:
                metadata_dict[property_name] = property_value

        # Write the properties to a file in disk. The file will one property
        # per line. We follow a special syntax to write the properties. The
        # syntax is:
        # #property_name=property_value
        with open(file_path, 'w') as file_handler:
            for property_name, property_value in six.iteritems(metadata_dict):
                file_handler.write('#%s=%s\n' % (property_name, property_value))

    return True


def _is_metadata_file_present(file_path, extension='.metadata'):
    """
    Check if the metadata file is present.
    """
    # Get the file name and the extension from the file path.
    file_name, _ = os.path.splitext(file_path)
    # Create a file name with the given extension.
    file_name = ''.join([file_name, extension])
    # Check if the file already exists.
    return os.path.exists(file_name)


def _get_metadata_from_file(file_path):
    """
    Get the metadata information from the file.
    """
    # Initialize a dictionary to store the metadata read from the file.
    metadata = dict()

    # Get the number of lines from the file
    num_lines = sum(1 for _ in open(file_path))

    # If there are some contents in the file (i.e num_lines > 0),
    # read its contents.
    if num_lines > 0:
        with open(file_path) as file_handler:
            for _ in range(num_lines):
                line = next(file_handler)
                # Consider only the lines that are starting with '#'
                if line.startswith('#'):
                    # Remove the leading '#'
                    line = line.lstrip('#')
                    # Split the line with '=' as the delimiter
                    tokens = line.split('=')
                    # Based on the special syntax we use, there should be
                    # exactly two tokens after we split using '='
                    assert len(tokens) is 2, 'Error in file, he num tokens ' \
                                             'is not 2'
                    # Retrieve the property_names and values.
                    property_name = tokens[0].strip()
                    property_value = tokens[1].strip()
                    # If the property value is not 'POINTER' then store it in
                    #  the metadata dictionary.
                    if property_value is not 'POINTER':
                        metadata[property_name] = property_value

    # Return the metadata dictionary and the number of lines in the file.
    return metadata, num_lines


def _update_metadata_for_read_cmd(metadata, **kwargs):
    """
    Update metadata for read_csv_metadata method.
    """
    # Create a copy of incoming metadata. We will update the incoming
    # metadata dict with kwargs.
    copy_metadata = metadata.copy()

    # The updation is going to happen in two steps: (1) overriding the
    # properties in metadata dict using kwargs, and (2) adding the properties
    #  to metadata dict from kwargs.

    # Step 1
    # We will override the properties in the metadata dict with the
    # properties from kwargs.

    # Get the property from metadata dict.
    for property_name in copy_metadata.keys():
        # If the same property is present in kwargs, then override it in the
        # metadata dict.
        if property_name in kwargs:
            property_value = kwargs.pop(property_name)
            if property_value is not None:
                metadata[property_name] = property_value
            else:
                # Warn the users if the metadata dict had a valid value,
                # but the kwargs sets it to None.
                logger.warning(
                    '%s key had a value (%s)in file but input arg is set to '
                    'None' % (property_name, metadata[property_name]))
                # Remove the property from the dictionary.
                metadata.pop(property_name)  # remove the key-value pair

    # Step 2
    # Add the properties from kwargs.
    # We should be careful here. The kwargs contains the key-value pairs that
    # are used by read_csv method (of pandas). We will just pick the
    # properties that we expect from the read_csv_metadata method.
    properties = ['key', 'ltable', 'rtable', 'fk_ltable', 'fk_rtable']

    # For the properties that we expect, read from kwargs and update the
    # metadata dict.
    for property_name in properties:
        if property_name in kwargs:
            property_value = kwargs.pop(property_name)
            if property_value is not None:
                metadata[property_name] = property_value
            else:
                # Warn the users if the properties in the kwargs is set to None.
                logger.warning('Metadata %s is set to None', property_name)
                # Remove the property from the metadata dict.
                metadata.pop(property_name, None)

    return metadata, kwargs


def _check_metadata_for_read_cmd(metadata):
    """
    Check the metadata for read_csv_metadata command
    """

    # Do basic validation checks for the metadata.

    # We require consistency of properties given for the canidate set. We
    # expect the user to provide all the required properties for the
    # candidate set.
    required_properties = ['ltable', 'rtable', 'fk_ltable', 'fk_rtable']

    # Check what the user has given
    given_properties = set(required_properties).intersection(metadata.keys())

    # Check if all the required properties are given
    if len(given_properties) > 0:
        # Check the lengths are same. If not, this means that the user is
        # missing something. So, raise an error.
        if len(given_properties) is not len(required_properties):
            logger.error(
                'Dataframe requires all valid ltable, rtable, fk_ltable, '
                'fk_rtable parameters set')
            raise AssertionError(
                'Dataframe requires all valid ltable, rtable, fk_ltable, '
                'fk_rtable parameters set')

        # ltable is expected to be of type pandas DataFrame. If not raise an
        # error.
        if not isinstance(metadata['ltable'], pd.DataFrame):
            logger.error('The parameter ltable must be set to valid Dataframe')
            raise AssertionError(
                'The parameter ltable must be set to valid Dataframe')

        # rtable is expected to be of type pandas DataFrame. If not raise an
        # error.
        if not isinstance(metadata['rtable'], pd.DataFrame):
            logger.error('The parameter rtable must be set to valid Dataframe')
            raise AssertionError(
                'The parameter rtable must be set to valid Dataframe')
    # If the length of comman properties is 0, it will fall out to return
    # True, which is ok.
    return True


def _check_file_path(file_path):
    """
    Check validity (access privileges and existence of a file already)of the
    given file path.
    """
    # returns a tuple can_write, file_exists
    if os.path.exists(file_path):
        # the file is there
        return True, True
    elif os.access(os.path.dirname(file_path), os.W_OK):
        return True, False
        # the file does not exists but write privileges are given
    else:
        return False, False
