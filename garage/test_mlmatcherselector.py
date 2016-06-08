# coding=utf-8
import logging
import os

import magellan as mg

logging.basicConfig(level=logging.DEBUG)
datasets_path = os.sep.join([mg.get_install_path(), 'datasets', 'test_datasets', 'matcherselector'])
path_a = os.sep.join([datasets_path, 'ACM_demo.csv'])
path_b = os.sep.join([datasets_path, 'DBLP_demo.csv'])
path_c = os.sep.join([datasets_path, 'dblp_acm_demo_labels.csv'])

A = mg.read_csv_metadata(path_a, key='id')
B = mg.read_csv_metadata(path_b, key='id')
C = mg.read_csv_metadata(path_c, ltable=B, rtable=A, fk_ltable='ltable.id', fk_rtable='rtable.id', key='_id')

feature_table = mg.get_features_for_matching(A, B)
feature_vectors = mg.extract_feature_vecs(C, feature_table=feature_table, attrs_after='gold', verbose=True)
# dtmatcher = mg.DTMatcher()
# nbmatcher = mg.NBMatcher()
# rfmatcher = mg.RFMatcher()
# svmmatcher = mg.SVMMatcher()
# linregmatcher = mg.LinRegMatcher()
# logregmatcher = mg.LogRegMatcher()
