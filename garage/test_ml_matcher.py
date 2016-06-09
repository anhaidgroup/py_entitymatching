import os

import magellan.matcher.matcherutils as mu
from magellan.io.parsers import read_csv_metadata
from magellan.matcher.dtmatcher import DTMatcher
from magellan.utils.generic_helper import get_install_path

feat_datasets_path = os.sep.join([get_install_path(), 'datasets', 'test_datasets', 'matcherselector'])
fpath_a = os.sep.join([feat_datasets_path, 'DBLP_demo.csv'])
fpath_b = os.sep.join([feat_datasets_path, 'ACM_demo.csv'])
fpath_c = os.sep.join([feat_datasets_path, 'dblp_acm_demo_labels.csv'])
fpath_f = os.sep.join([feat_datasets_path, 'feat_vecs.csv'])

A = read_csv_metadata(fpath_a, key='id')
B = read_csv_metadata(fpath_b, key='id')
feature_vectors = read_csv_metadata(fpath_f, ltable=A, rtable=B)
train_test = mu.train_test_split(feature_vectors)
train, test = train_test['train'], train_test['test']
dt = DTMatcher(name='DecisionTree')
dt.fit(table=train, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'], target_attr='gold')
predictions = dt.predict(table=test, exclude_attrs=['ltable.id', 'rtable.id', '_id', 'gold'],
                         target_attr='predicted',
                         append=True)
print('Done')
