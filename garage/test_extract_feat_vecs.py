# coding=utf-8
import magellan as mg
import pandas as pd

p = '/Users/pradap/Documents/Research/Drug-mapping/data/'
A = mg.read_csv_metadata(p+'freepharma_data_df.csv', low_memory=False,
                         key='DRUG_ID')
B = mg.read_csv_metadata(p+'shp_data_df.csv', low_memory=False,
                         key='DRUG_ID')
B['DRUG_ID'] = -1*B['DRUG_ID']

print('DONE WITH READING FILES')
# feature_table = mg.get_features_for_matching(A, B)
# feature_table = feature_table.ix[4:len(feature_table)]
feature_table = mg.load_object('./feature_table.pkl')
# mg.save_object(feature_table, './feature_table.pkl')
train = mg.read_csv_metadata('./train.csv', ltable=A, rtable=B)
K = mg.extract_feature_vecs(train, feature_table=feature_table)
print(K)
print('Hi')



#
# reviewed = mg.read_csv_metadata(
#     p + 'set1_results_match_mismatch_drugpairs_10000_reviewed.csv',
#     key='_id')
# temp = pd.isnull(reviewed['reviewed(0:nonmatch 1:match)'])
# temp1 = (temp == False)
# reviewed = reviewed[temp1]
#
#
# labeled_data = reviewed[['_id', 'ltable.DRUG_ID', 'rtable.DRUG_ID',
#                          'ltable.AHFS_SPECIFIC_CATEGORY_DESC',
#                          'ltable.DOSAGE_FORM_DESC',
#                          'ltable.DRUG',
#                          'ltable.DRUG_NAME',
#                          'ltable.DRUG_ROUTE_DESC',
#                          'ltable.DRUG_STRENGTH',
#                          'ltable.GENERIC_NAME',
#                          'ltable.NDC_CODE',
#                          'ltable.THERAPEUTIC_AHFS_ID',
#                          'ltable.THERAPEUTIC_STANDARD_DESC',
#                          'rtable.AHFS_SPECIFIC_CATEGORY_DESC',
#                          'rtable.DOSAGE_FORM_DESC',
#                          'rtable.DRUG',
#                          'rtable.DRUG_NAME',
#                          'rtable.DRUG_ROUTE_DESC',
#                          'rtable.DRUG_STRENGTH',
#                          'rtable.GENERIC_NAME',
#                          'rtable.NDC_CODE',
#                          'rtable.THERAPEUTIC_AHFS_ID',
#                          'rtable.THERAPEUTIC_STANDARD_DESC',
#                         'reviewed(0:nonmatch 1:match)',
#                          'predicted(0:nonmatch 1:match)']]
#
# labeled_data = labeled_data.rename(columns={'reviewed(0:nonmatch 1:match)': 'gold',
#                             'predicted(0:nonmatch 1:match)' : 'old_predicted'},
#                            )
#
#
# mg.set_property(labeled_data,'fk_ltable', 'ltable.DRUG_ID')
# mg.set_property(labeled_data,'fk_rtable', 'rtable.DRUG_ID')
# mg.set_property(labeled_data,'ltable', A)
# mg.set_property(labeled_data,'rtable', B)
# mg.set_key(labeled_data, '_id')
#
# train_test = mg.train_test_split(labeled_data, train_proportion=0.001)
# train = train_test['train']
# test = train_test['test']
# mg.to_csv_metadata(labeled_data, './labeled_data.csv')
# mg.to_csv_metadata(train, './train.csv')

# K = mg.extract_feature_vecs(train, feature_table=feature_table,
#                             attrs_before=[ 'ltable.AHFS_SPECIFIC_CATEGORY_DESC',
#                          'ltable.DOSAGE_FORM_DESC',
#                          'ltable.DRUG',
#                          'ltable.DRUG_NAME',
#                          'ltable.DRUG_ROUTE_DESC',
#                          'ltable.DRUG_STRENGTH',
#                          'ltable.GENERIC_NAME',
#                          'ltable.NDC_CODE',
#                          'ltable.THERAPEUTIC_AHFS_ID',
#                          'ltable.THERAPEUTIC_STANDARD_DESC',
#                          'rtable.AHFS_SPECIFIC_CATEGORY_DESC',
#                          'rtable.DOSAGE_FORM_DESC',
#                          'rtable.DRUG',
#                          'rtable.DRUG_NAME',
#                          'rtable.DRUG_ROUTE_DESC',
#                          'rtable.DRUG_STRENGTH',
#                          'rtable.GENERIC_NAME',
#                          'rtable.NDC_CODE',
#                          'rtable.THERAPEUTIC_AHFS_ID',
#                          'rtable.THERAPEUTIC_STANDARD_DESC'],
#                             attrs_after='gold')
#
# print(K)
# print('Hi')