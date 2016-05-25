# coding=utf-8
import logging
import os
from collections import OrderedDict

import pandas as pd

import magellan.core.catalog_manager as cm

logger = logging.getLogger(__name__)


def read_csv(file_path, **kwargs):
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
    Examples:
    >>> import magellan as mg
    >>> A = mg.read_csv('A.csv')
    # if A.metadata_ is present along with A.csv, the metadata information
    # will be updated for A
    >>> A.get_key()
    """


    # update metadata from file (if present)
    if _is_metadata_file_present(file_path):
        file_name, file_ext = os.path.splitext(file_path)
        file_name += '.metadata'
        metadata, num_lines = _get_metadata_from_file(file_name)
    else:
        metadata = {}

    # update metadata from kwargs
    metadata, kwargs = _update_metadata_for_read_cmd(metadata, **kwargs)
    _check_metadata_for_read_cmd(metadata)
    df = pd.read_csv(file_path, **kwargs)
    key = metadata.pop('key', None)
    if key is not None:
        cm.set_key(df, key)
    for k, v in metadata.iteritems():
        cm.set_property(df, k, v)
    return df


def to_csv(df, file_path, **kwargs):
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
        >>> A = mg.read_csv('A.csv')
        >>> mg.to_csv(A, 'updated.csv')
    """

    index = kwargs.pop('index', None)
    if index is None:
        kwargs['index'] = False



    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + '.metadata'

    # write metadata

    _write_metadata(df, metadata_filename)

    # write dataftame
    return df.to_csv(file_path, **kwargs)


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
        for k, v in d.iteritems():
            if isinstance(v, basestring) is False:
                metadata_dict[k] = 'POINTER'
            else:
                metadata_dict[k] = v

        with open(file_path, 'w') as f:
            for k, v in d.iteritems():
                f.write('#%s=%s\n' % (k, v))

    return True


def _is_metadata_file_present(file_path):
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
    file_name += '.metadata'
    return os.path.isfile(file_name)


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
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logger.warning('%s key had a value in file but input arg is set to None')
                v = metadata.pop(k)  # remove the key-value pair

    # Add the properties from key-value arguments
    table_props = ['key', 'ltable', 'rtable', 'fk_ltable', 'fk_rtable']
    for k in table_props:
        if kwargs.has_key(k):
            value = kwargs.pop(k)
            if value is not None:
                metadata[k] = value
            else:
                logging.getLogger(__name__).warning('%s key had a value in file but input arg is set to None')
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
            raise AssertionError('Dataframe requires all valid ltable, rtable, fk_ltable, '
                                 'fk_rtable parameters set')

        if isinstance(metadata['ltable'], pd.DataFrame) is False:
            raise AssertionError('The parameter ltable must be set to valid Dataframe')

        if isinstance(metadata['rtable'], pd.DataFrame) is False:
            raise AssertionError('The parameter rtable must be set to valid Dataframe')

    return True
