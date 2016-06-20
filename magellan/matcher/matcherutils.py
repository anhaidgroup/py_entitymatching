import logging
import math
import time
from collections import OrderedDict

import pandas as pd
import sklearn.cross_validation as cv

from magellan.utils.catalog_helper import log_info

logger = logging.getLogger(__name__)

import magellan.catalog.catalog_manager as cm


def train_test_split(labeled_data, train_proportion=0.5, random_state=None, verbose=True):
    if not isinstance(labeled_data, pd.DataFrame):
        logger.error('Input table is not of type dataframe')
        raise AssertionError('Input table is not of type dataframe')

    log_info(logger, 'Required metadata: cand.set key, fk ltable, fk rtable, '
                     'ltable, rtable, ltable key, rtable key', verbose)

    # # get metadata
    key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(labeled_data,
                                                                                          logger, verbose)

    # # validate metadata
    cm._validate_metadata_for_candset(labeled_data, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                      logger, verbose)

    num_rows = len(labeled_data)
    assert train_proportion >= 0 and train_proportion <= 1, " Train proportion is expected to be between 0 and 1"
    assert num_rows > 0, 'The input table is empty'

    train_size = int(math.floor(num_rows * train_proportion))
    test_size = int(num_rows - train_size)

    # use sk learn to split the data
    idx_values = pd.np.array(labeled_data.index.values)
    idx_train, idx_test = cv.train_test_split(idx_values, test_size=test_size, train_size=train_size,
                                              random_state=random_state)

    # construct output tables.
    lbl_train = labeled_data.ix[idx_train]
    lbl_test = labeled_data.ix[idx_test]

    # update catalog
    cm.init_properties(lbl_train)
    cm.copy_properties(labeled_data, lbl_train)

    cm.init_properties(lbl_test)
    cm.copy_properties(labeled_data, lbl_test)

    # return output tables
    result = OrderedDict()
    result['train'] = lbl_train
    result['test'] = lbl_test

    return result


def get_ts():
    t = int(round(time.time() * 1e10))
    return str(t)[::-1]
