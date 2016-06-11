# coding=utf-8
import collections
import logging
import os
import pickle

import cloudpickle
import pandas as pd
import six

import magellan.catalog.catalog_manager as cm
import magellan.io.parsers as ps

logger = logging.getLogger(__name__)


def save_object(object_to_save, file_path):
    """
    Save python objects to disk.

    This function is expected to be used to save magellan objects such as
    rule-based blocker, feature table, etc. An user would like to store
    magellan objects to disk, when he/she wants to save the workflow and
    resume it later or store the result of a computation intensive commands
    such as blockers. This function provides a way to save the required such
    objects to disk.

    Args:
        object_to_save (Python object): Python object to save. This can be
            magellan objects such as blockers, matchers, etc.
        file_path (str): File path to store the objects.

    Returns:
        A boolean value of True is returned, if the saving was successful.

    Raises:
        AssertionError: If the file path is not of type string.
        AssertionError: If we cannot write in the given file path.
    """
    # Validate input parameters

    # # The file path is expected to be of type string.
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    # Check whether the file path is valid and if a file is already present
    # at that path.
    can_write, file_exists = ps._check_file_path(file_path)

    # Check whether we can write
    if can_write:
        # If a file already exists in that location, issue a warning and
        # overwrite the file.
        if file_exists:
            logger.warning(
                'File already exists at %s; Overwriting it' % file_path)
            # we open the file in 'wb' mode as we are writing a binary file.
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(object_to_save, file_handler)
        else:
            with open(file_path, 'wb') as file_handler:
                cloudpickle.dump(object_to_save, file_handler)

    # If we cannot write, then raise an error.
    else:
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    # Return True if everything was successful.
    return True


def load_object(file_path):
    """
    Load Python objects from disk.

    This function expected to load magellan objects from disk such as
    blockers, matchers, etc.

    Args:
        file_path (str): File path to load object from.

    Returns:
        A Python object read from the file path.

    Raises:
        AssertionError: If the file path is not of type string.
        AssertiosnError: If a file does not exist at the given file path.
    """
    # Validate input parameters

    # The file path is expected to be of type string.
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

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


def save_table(data_frame, file_path, metadata_ext='.pklmetadata'):
    """
    Pickle dataframe along with the metadata
    Args:
        data_frame (pandas dataframe): Dataframe that should be saved
        file_path (str): File path where the dataframe must be stored
    Returns:
        status (bool). Returns True if the command executes successfully
    """
    if not isinstance(data_frame, pd.DataFrame):
        logging.error('Input object is not of type pandas dataframe')
        raise AssertionError('Input object is not of type pandas dataframe')

    # input type validations
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    # input type validations
    if not isinstance(metadata_ext, six.string_types):
        logger.error('Input metadata ext is not of type string')
        raise AssertionError('Input metadata ext is not of type string')

    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + metadata_ext

    can_write, file_exists = ps._check_file_path(file_path)

    if can_write:
        if file_exists:
            logger.warning(
                'File already exists at %s; Overwriting it' % file_path)
            with open(file_path, 'wb') as f:
                cloudpickle.dump(data_frame, f)
        else:
            with open(file_path, 'wb') as f:
                cloudpickle.dump(data_frame, f)

    else:
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    metadata_dict = collections.OrderedDict()
    # get all the properties for the input data frame
    if cm.is_dfinfo_present(data_frame) is True:
        d = cm.get_all_properties(data_frame)

    # write properties to disk
    if len(d) > 0:
        # for k, v in d.iteritems():
        for k, v in six.iteritems(d):
            if isinstance(v, six.string_types) is True:
                metadata_dict[k] = v

    # try to save metadata
    can_write, file_exists = ps._check_file_path(metadata_filename)
    if can_write:
        if file_exists:
            logger.warning(
                'Metadata file already exists at %s. Overwriting it' % metadata_filename)
            # write metadata contents
            with open(metadata_filename, 'wb') as f:
                cloudpickle.dump(metadata_dict, f)
        else:
            # write metadata contents
            with open(metadata_filename, 'wb') as f:
                cloudpickle.dump(metadata_dict, f)
    # else:
    #     logger.warning('Cannot write metadata at the file path %s. Skip writing metadata file' % metadata_filename)

    return True


def load_table(file_path, metadata_ext='.pklmetadata'):
    """
    Load table from file
    Args:
        file_path (str): File path to load the file from
    Returns:
        Loaded dataframe (pandas dataframe). Returns the dataframe loaded from the file.
    """
    # load data frame from file path
    # # input validations are done in load_object

    # input type validations
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    # input type validations
    if not isinstance(metadata_ext, six.string_types):
        logger.error('Input metadata ext is not of type string')
        raise AssertionError('Input metadata ext is not of type string')

    data_frame = load_object(file_path)

    # load metadata from file path
    if ps._is_metadata_file_present(file_path, extension=metadata_ext):
        file_name, file_ext = os.path.splitext(file_path)
        metadata_filename = file_name + metadata_ext
        metadata_dict = load_object(metadata_filename)
        # update metadata in the catalog
        # for key, value in metadata_dict.iteritems():
        for key, value in six.iteritems(metadata_dict):
            if key == 'key':
                cm.set_key(data_frame, value)
            else:
                cm.set_property(data_frame, key, value)
    else:
        logger.warning('There is no metadata file')
    return data_frame
