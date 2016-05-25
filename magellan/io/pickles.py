# coding=utf-8
from collections import OrderedDict
import os
import pickle
import cloud

from magellan.io.parsers import _is_metadata_file_present
import magellan.core.catalog_manager as cm

def save_object(obj, file_path):

    """
    Save Magellan objects
    Args:
        obj (python object): This can be magellan objects such as blockers, matchers, triggers, etc.
        file_path (str): File path to store objects
    Returns:
        status (bool). Returns True if the command executes successfully
    """

    with open(file_path, 'w') as f:
        cloud.serialization.cloudpickle.dump(obj, f)
    return True


def load_object(file_path):
    """
    Load Magellan objects
    Args:
        file_path (str): File path to load object from
    Returns:
        result (python object). Typically magellan objects such as blockers, matchers, triggers, etc
    """
    with open(file_path, 'r') as f:
        result = pickle.load(f)
    return result



def save_table(df, file_path):
    """
    Pickle dataframe along with the metadata
    Args:
        df (pandas dataframe): Dataframe that should be saved
        file_path (str): File path where the dataframe must be stored
    Returns:
        status (bool). Returns True if the command executes successfully
    """

    file_name, file_ext = os.path.splitext(file_path)
    metadata_filename = file_name + '.metadata'


    metadata_dict = OrderedDict()

    # get all the properties for the input data frame
    if cm.is_dfinfo_present(df) is True:
        d = cm.get_all_properties(df)

    # write properties to disk
    if len(d) > 0:
        for k, v in d.iteritems():
            if isinstance(v, basestring) is True:
                metadata_dict[k] = v

    with open(file_path, 'w') as f:
        cloud.serialization.cloudpickle.dump(df, f)

    # write metadata contents
    with open(metadata_filename, 'w') as f:
        cloud.serialization.cloudpickle.dump(metadata_dict, f)

    return True


def load_table(file_path):
    """
    Load table from file
    Args:
        file_path (str): File path to load the file from
    Returns:
        Loaded dataframe (pandas dataframe). Returns the dataframe loaded from the file.
    """

    # load data frame from file path
    df = pickle.load(file_path)

    # load metadata from file path
    if _is_metadata_file_present(file_path):
        file_name, file_ext = os.path.splitext(file_path)
        metadata_filename = file_name + '.metadata'
        metadata_dict = pickle.load(metadata_filename)
        # update metadata in the catalog
        for key, value in metadata_dict.iteritems():
            if key is 'key':
                cm.set_key(df, key)
            else:
                cm.set_property(df, key, value)
    return df