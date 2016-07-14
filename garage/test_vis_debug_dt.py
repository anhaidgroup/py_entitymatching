# coding=utf-8
import os

import magellan as mg
from magellan.debugmatcher.debug_gui_decisiontree_matcher import _vis_debug_dt, \
    vis_tuple_debug_dt_matcher

datasets_path = os.sep.join([mg.get_install_path(), 'tests', 'test_datasets'])
path_c = os.sep.join([datasets_path, 'C.csv'])
A = mg.load_dataset('person_table_A', key='ID')
B = mg.load_dataset('person_table_B', key='ID')
C = mg.read_csv_metadata(path_c, ltable=A, rtable=B)
#
# labels = [0] * 7
# labels.extend([1] * 8)
# C['labels'] = labels
#
# feature_table = mg.get_features_for_matching(A, B)
# feature_vectors = mg.extract_feature_vecs(C, feature_table=feature_table,
#                                        attrs_after='labels')
#
# dt = mg.DTMatcher()
# dt.fit(table=feature_vectors, exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
#        target_attr='labels')
# vis_tuple_debug_dt_matcher(dt, feature_vectors.ix[0],
#                            exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'])

feature_table = mg.get_features_for_matching(A, B)

labels = [0]*7
labels.extend([1]*8)
C['labels'] = labels

feature_vectors = mg.extract_feature_vecs(C, feature_table=feature_table,
                                         attrs_after='labels')


# rf = mg.RFMatcher()
dt = mg.DTMatcher()
train_test = mg.split_train_test(feature_vectors)

train = train_test['train']
test = train_test['test']

mg.vis_debug_dt(dt, train, test,
                exclude_attrs=['_id', 'ltable_ID', 'rtable_ID', 'labels'],
               target_attr='labels')
