from collections import OrderedDict

import pandas as pd
import numpy as np

from sklearn.cross_validation import KFold, cross_val_score
from sklearn.preprocessing import Imputer

from magellan.utils.generic_helper import list_diff

def select_matcher(matchers, x=None, y=None, table=None, exclude_attrs=None, target_attr=None, metric='precision', k=5,
                   random_state=None):
    x,y = get_xy_data(x, y, table, exclude_attrs, target_attr)
    dict_list = []
    max_score = 0
    sel_matcher = matchers[0]
    # header
    header = ['Name', 'Matcher', 'Num folds']
    fold_header = ['Fold ' + str(i+1) for i in range(k)]
    header.extend(fold_header)
    header.append('Mean score')
    #print header

    for m in matchers:
        matcher, scores = cross_validation(m, x, y, metric, k, random_state)
        val_list = [matcher.get_name(), matcher, k]
        val_list.extend(scores)
        val_list.append(np.mean(scores))
        #print val_list
        d = OrderedDict(zip(header, val_list))
        #print d
        dict_list.append(d)
        if np.mean(scores) > max_score:
            sel_matcher = m
            max_score = np.mean(scores)
    stats = pd.DataFrame(dict_list)
    stats = stats[header]
    res = OrderedDict()
    res['selected_matcher'] = sel_matcher
    res['cv_stats'] = stats
    return res

def cross_validation(matcher, x, y, metric, k, random_state):
    cv = KFold(len(y), k, shuffle=True, random_state=random_state)
    scores = cross_val_score(matcher.clf, x, y, scoring=metric, cv=cv)
    return matcher, scores



def get_xy_data(x, y, table, exclude_attrs, target_attr):
    if x is not None and y is not None:
        return get_xy_data_prj(x, y)
    elif table is not None and exclude_attrs is not None and target_attr is not None:
        return get_xy_data_ex(table, exclude_attrs, target_attr)
    else:
        raise SyntaxError('The arguments supplied does not match the signatures supported !!!')

def get_xy_data_prj(x, y):
    if x.columns[0] is '_id':
        x = x.values
        x = np.delete(x, 0, 1)
    else:
        x = x.values
    if y is not None:
        if not isinstance(y, pd.Series) and y.columns[0] is '_id':
            y = y.values
            y = np.delete(y, 0, 1)
        else:
            y = y.values
    return x, y

def get_xy_data_ex(table, exclude_attrs, target_attr):
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]
    attrs_to_project = list_diff(table.columns, exclude_attrs)
    table = table.to_dataframe()
    x = table[attrs_to_project].values
    y = table[target_attr].values
    y = y.ravel() # to mute warnings from svm and cross validation
    return x, y