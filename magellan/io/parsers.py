# coding=utf-8
import logging
import os
from collections import OrderedDict

import pandas as pd
import six

import magellan.core.catalog_manager as cm

logger = logging.getLogger(__name__)


def read_csv_metadata(file_path, **kwargs):
    """
    Read contents from the given file name along with metadata, if
    metadata is set to true.
    The file name is typically with .csv extension. Metadata is
        expected to be  with the same file name but with .metadata
    extension.

    Args:
        file_path (str): csv file path
        kwargs (dict): key value arguments to pandas read_csv

    Returns:
        result (pandas dataframe)

    Raises:
        AssertionError : If the input file path is not of type string

    Examples:
    >>> import magellan as mg
    >>> A = mg.read_csv_metadata('A.csv')
    # if A.metadata_ is present along with A.csv, the metadata information
    # will be updated for A
    >>> A.get_key()
    """
    # input type validations
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    # check if the CSV file exists
    if not os.path.exists(file_path):
        logger.error('File does not exist at path %s' % file_path)
        raise AssertionError('File does not exist at path %s' % file_path)

    ext = kwargs.pop('metadata_extn', None)
    if ext == None:
        ext = '.metadata'
    if ext.startswith('.') == False:
        ext = '.'+ext

    # update metadata from file (if present)
    if _is_metadata_file_present(file_path, ext=ext):
        file_name, file_ext = os.path.splitext(file_path)
        file_name += ext
        metadata, num_lines = _get_metadata_from_file(file_name)
    else:
        logger.warning('Metadata file is not present in the given path; proceeding to read the csv file.')
        metadata = {}

    # update metadata from kwargs
    metadata, kwargs = _update_metadata_for_read_cmd(metadata, **kwargs)
    _check_metadata_for_read_cmd(metadata)
    key = metadata.pop('key', None)
    df = pd.read_csv(file_path, **kwargs)
    if key is not None:
        cm.set_key(df, key)

    # for k, v in metadata.iteritems():
    for k, v in six.iteritems(metadata):
        if k == 'key':
            cm.set_key(df, k)
        # elif k == 'fk_ltable' and metadata.has_key('ltable') and isinstance(metadata['ltable'], pd.DataFrame):
        #     cm.validate_and_set_fk_ltable(df, metadata['fk_ltable'], metadata['ltable'],
        #                                   cm.get_key(metadata['ltable']))
        # elif k == 'fk_rtable' and metadata.has_key('rtable') and isinstance(metadata['rtable'], pd.DataFrame):
        #     cm.validate_and_set_fk_rtable(df, metadata['fk_rtable'], metadata['rtable'],
        #                                   cm.get_key(metadata['rtable']))
        else:
            cm.set_property(df, k, v)
    if cm.is_dfinfo_present(df) == False:
        cm.init_properties(df)
    return df


def to_csv_metadata(df, file_path, **kwargs):
    """
    Write csv file along with metadata
    Args:
        df (pandas dataframe): Data frame to written to disk
        file_path (str):  File path where df contents to be written. Metadata is written with the same file name
            with .metadata extension
        kwargs (dict): Key value arguments
    Returns:
        status (bool). Returns True if the file was written successfully
    Examples:
        >>> import magellan as mg
        >>> A = mg.read_csv_metadata('A.csv')
        >>> mg.to_csv_metadata(A, 'updated.csv')
    """

    if not isinstance(df, pd.DataFrame):
        logging.error('Input object is not of type pandas dataframe')
        raise AssertionError('Input object is not of type pandas dataframe')

    # input type validations
    if not isinstance(file_path, six.string_types):
        logger.error('Input file path is not of type string')
        raise AssertionError('Input file path is not of type string')

    ext = kwargs.pop('metadata_extn', None)
    if ext == None:
        ext = '.metadata'
    if ext.startswith('.') == False:
        ext = '.'+ext


    index = kwargs.pop('index', None)

    if index is None:
        kwargs['index'] = False

    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + ext

    can_write, file_exists = _check_file_path(file_path)
    if can_write:
        if file_exists:
            logger.warning('File already exists at %s; Overwriting it' % file_path)
            df.to_csv(file_path, **kwargs)
        else:
            df.to_csv(file_path, **kwargs)
    else:
        logger.error('Cannot write in the file path %s; Exiting' % file_path)
        raise AssertionError('Cannot write in the file path %s' % file_path)

    # try to write metadata
    can_write, file_exists = _check_file_path(metadata_filename)
    if can_write:
        if file_exists:
            logger.warning('Metadata file already exists at %s. Overwriting it' % metadata_filename)
            _write_metadata(df, metadata_filename)
        else:
            _write_metadata(df, metadata_filename)
    else:
        logger.warning('Cannot write metadata at the file path %s. Skip writing metadata file' % metadata_filename)

    return True


def _write_metadata(df, file_path):
    """
    Write metadata to disk
    Args:
        df (pandas dataframe): Input dataframe to be written to disk
        file_path (str): File path where the metadata should be written
    Returns:
        status (bool). Returns True if the file was written successfully or if no
            metadata was there to write
    Notes:
        This is an internal function
    """
    metadata_dict = OrderedDict()

    # get all the properties for the input data frame
    if cm.is_dfinfo_present(df) is True:
        d = cm.get_all_properties(df)

    # write properties to disk
    if len(d) > 0:
        # for k, v in d.iteritems():
        for k, v in six.iteritems(d):
            if isinstance(v, six.string_types) is False:
                metadata_dict[k] = 'POINTER'
            else:
                metadata_dict[k] = v

        with open(file_path, 'w') as f:
            # for k, v in metadata_dict.iteritems():
            for k, v in six.iteritems(metadata_dict):
                f.write('#%s=%s\n' % (k, v))

    return True


def _is_metadata_file_present(file_path, ext='.metadata'):
    """
    Check if the metadata file is present
    Args:
        file_path (str): Metadata file path. Typically path to csv file is given as input.
    Returns:
        status (bool). Returns True if the metadata is present in the specified path
    Notes:
        This is an internal function
    """
    file_name, file_ext = os.path.splitext(file_path)
    file_name += ext
    return os.path.exists(file_name)


def _get_metadata_from_file(file_path):
    """
    Get the metadata information from the file
    Args:
        file_path (str): Metadata file path
    Returns:
        metadata information (dict), and number of lines (int) read from the file
    Notes:
        This is an internal function
    """

    metadata = dict()
    # get the number of lines from the file
    num_lines = sum(1 for line in open(file_path))
    if num_lines > 0:
        # read contents from file
        with open(file_path) as f:
            for i in range(num_lines):
                line = next(f)
                if line.startswith('#'):
                    line = line.lstrip('#')
                    tokens = line.split('=')
                    assert len(tokens) is 2, "Error in file, the num tokens is not 2"
                    key = tokens[0].strip()
                    value = tokens[1].strip()
                    if value is not "POINTER":
                        metadata[key] = value
    return metadata, num_lines


def _update_metadata_for_read_cmd(metadata, **kwargs):
    """
    Update metadata for read command
    Args:
        metadata (dict): Dictionary updated from metadata file
        **kwargs (dict): Dictionary that should be updated with metadata
    Returns:
        Updated **kwargs(dict), and metadata (dict)
    Notes:
        This is an internal function
    """

    # first update from the key-value arguments
    for k in metadata.keys():
        # if kwargs.has_key(k):
        if k in kwargs:
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logger.warning('%s key had a value in file but input arg is set to None' %k)
                v = metadata.pop(k)  # remove the key-value pair

    # Add the properties from key-value arguments
    table_props = ['key', 'ltable', 'rtable', 'fk_ltable', 'fk_rtable']
    for k in table_props:
        if k in kwargs:
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logger.warning('%s key had a value in file but input arg is set to None' %k)
                v = metadata.pop(k)  # remove the key-value pair
    return metadata, kwargs


def _check_metadata_for_read_cmd(metadata):
    """
    Check the metadata for read command
    Args:
        metadata (dict): Dictionary with metadata information
    Returns:
        status (bool). Returns True if all the metadata is present
    Raises:
        AssertionError: Raised in three cases: (1) If all the metadata required is not present, (2) The ltable is
            not of type pandas dataframe, and (3) The rtable id not of type pandas dataframe
    Notes:
        This is an internal function
    """
    table_props = ['ltable', 'rtable', 'fk_ltable', 'fk_rtable']
    v = set(table_props)
    k = set(metadata.keys())
    i = v.intersection(k)
    if len(i) > 0:
        if len(i) is not len(table_props):
            logger.error('Dataframe requires all valid ltable, rtable, fk_ltable, '
                                 'fk_rtable parameters set')
            raise AssertionError('Dataframe requires all valid ltable, rtable, fk_ltable, '
                                 'fk_rtable parameters set')

        if isinstance(metadata['ltable'], pd.DataFrame) is False:
            logger.error('The parameter ltable must be set to valid Dataframe')
            raise AssertionError('The parameter ltable must be set to valid Dataframe')

        if isinstance(metadata['rtable'], pd.DataFrame) is False:
            logger.error('The parameter rtable must be set to valid Dataframe')
            raise AssertionError('The parameter rtable must be set to valid Dataframe')

    return True



def _check_file_path(file_path):
    # returns a tuple can_write, file_exists
    if os.path.exists(file_path):
        # the file is there
        return True, True
    elif os.access(os.path.dirname(file_path), os.W_OK):
        return True, False
        # the file does not exists but write privileges are given
    else:
        return False, False