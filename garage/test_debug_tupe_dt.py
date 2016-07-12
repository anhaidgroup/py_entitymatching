# coding=utf-8
# coding=utf-8
import os
import magellan as mg

datasets_path = os.sep.join([mg.get_install_path(), 'datasets', 'test_datasets'])
path_c = os.sep.join([datasets_path, 'C.csv'])
A = mg.load_dataset('table_A', key='ID')
B = mg.load_dataset('table_B', key='ID')
C = mg.read_csv_metadata(path_c, ltable=A, rtable=B)
feature_table = mg.get_features_for_matching(A, B)

labels = [0]*7
labels.extend([1]*8)
C['labels'] = labels

feature_vectors = mg.extract_feature_vecs(C, feature_table=feature_table,
                                         attrs_after='labels')


rf = mg.RFMatcher()
train_test = mg.split_train_test(feature_vectors)

train = train_test['train']
test = train_test['test']

rf.fit(table=train, exclude_attrs=['ltable_ID', 'rtable_ID', '_id'], target_attr='labels')
mg.debug_randomforest_matcher(rf, A.ix[1], B.ix[2], feature_table=feature_table,
                              table_columns=feature_vectors.columns,
                              exclude_attrs=['ltable_ID', 'rtable_ID', '_id', 'labels'])
print('Hi')