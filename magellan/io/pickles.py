# coding=utf-8
from collections import OrderedDict
import logging
import os

import cloudpickle
import pandas as pd
import pickle
import six

from magellan.io.parsers import _is_metadata_file_present, _check_file_path
import magellan.core.catalog_manager as cm

logger = logging.getLogger(__name__)


def save_object(obj, file_path):
    """
    Save Magellan objects
    Args:
        obj (python object): This can be magellan objects such as blockers, matchers, triggers, etc.
        file_path (str): File path to store objects
    Returns:
        status (bool). Returns True if the command executes successfully
    """
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    can_write, file_exists = _check_file_path(file_path)

    if can_write:
        if file_exists:
            logger.warning('File already exists at %s; Overwriting it' % file_path)
            with open(file_path, 'wb') as f:
                cloudpickle.dump(obj, f)
        else:
            with open(file_path, 'wb') as f:
                cloudpickle.dump(obj, f)

    else:
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    return True


def load_object(file_path):
    """
    Load Magellan objects
    Args:
        file_path (str): File path to load object from
    Returns:
        result (python object). Typically magellan objects such as blockers, matchers, triggers, etc
    """
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    # check if the file exists
    if not os.path.exists(file_path):
        logger.error('File does not exist at path %s' % file_path)
        raise AssertionError('File does not exist at path %s' % file_path)

    with open(file_path, 'rb') as f:
        result = pickle.load(f)
    return result


def save_table(df, file_path, metadata_ext='.pklmetadata'):
    """
    Pickle dataframe along with the metadata
    Args:
        df (pandas dataframe): Dataframe that should be saved
        file_path (str): File path where the dataframe must be stored
    Returns:
        status (bool). Returns True if the command executes successfully
    """
    if not isinstance(df, pd.DataFrame):
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

    can_write, file_exists = _check_file_path(file_path)

    if can_write:
        if file_exists:
            logger.warning('File already exists at %s; Overwriting it' % file_path)
            with open(file_path, 'wb') as f:
                cloudpickle.dump(df, f)
        else:
            with open(file_path, 'wb') as f:
                cloudpickle.dump(df, f)

    else:
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    metadata_dict = OrderedDict()
    # get all the properties for the input data frame
    if cm.is_dfinfo_present(df) is True:
        d = cm.get_all_properties(df)

    # write properties to disk
    if len(d) > 0:
        for k, v in d.iteritems():
            if isinstance(v, six.string_types) is True:
                metadata_dict[k] = v

    # try to save metadata
    can_write, file_exists = _check_file_path(metadata_filename)
    if can_write:
        if file_exists:
            logger.warning('Metadata file already exists at %s. Overwriting it' % metadata_filename)
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


    df = load_object(file_path)

    # load metadata from file path
    if _is_metadata_file_present(file_path, ext=metadata_ext):
        file_name, file_ext = os.path.splitext(file_path)
        metadata_filename = file_name + metadata_ext
        metadata_dict = load_object(metadata_filename)
        # update metadata in the catalog
        # for key, value in metadata_dict.iteritems():
        for key, value in six.iteritems(metadata_dict):
            if key == 'key':
                cm.set_key(df, value)
            else:
                cm.set_property(df, key, value)
    else:
        logger.warning('There is no metadata file')
    return df
