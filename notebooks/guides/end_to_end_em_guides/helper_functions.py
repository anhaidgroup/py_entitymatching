import pandas as pd
import py_entitymatching as em
def get_missing_rows_in_candset(C, L, c_keys, l_keys):
    """
    Example usage:
    get_missing_rows_in_candset(C, L1, ['ltable_id', 'rtable_id'], ['fodors_id', 'zagats_id'])
    L1 is labeled data (with more attrs)
    """
    C1 = C[c_keys]
    L1 = L[l_keys]

    d = dict()
    for t in C1.itertuples(index=False):
        d[(t[0], t[1])] = 1

    missing_tuples_in_C = []
    for t in L1.itertuples(index=False):
        if (t[0], t[1]) not in d:
            missing_tuples_in_C.append(t)

    series_list = []

    for t in missing_tuples_in_C:
        series_list.append((L[(L[l_keys[0]] == t[0]) & (L[l_keys[1]] == t[1])]))

    if len(series_list) == 0:
        print('There are no missing tuples')
    else:
        return pd.concat(series_list)

def get_sampled_n_labeled_data(C, L, c_keys, l_keys, n, label_col, random_state=0):
    """
    Example usage:
    get_sampled_n_labeled_data(C, L, ['ltable_id', 'rtable_id'], ['fodors_id', 'zagats_id'], 450, 'gold', random_state=0)
    L is labeled data (with more attrs)
    """
    # C1 = C[c_keys]
    # L1 = L[l_keys]

    d = dict()
    for t in L[l_keys].itertuples(index=False):
        d[(t[0], t[1])] = 1

    diff_tupes_in_C = []
    for t in C[c_keys].itertuples(index=False):
        if (t[0], t[1]) not in d:
            diff_tupes_in_C.append(t)

    series_list = []

    for t in diff_tupes_in_C:
        series_list.append((C[(C[c_keys[0]] == t[0]) & (C[c_keys[1]] == t[1])]))

    if len(series_list) == 0:
        print('There are no diff tuples in C')
    else:
        neg_tuples =  pd.concat(series_list)

    # pos_tuples
    pos_tuples_in_C = []
    for t in C[c_keys].itertuples(index=False):
        if (t[0], t[1]) in d:
            pos_tuples_in_C.append(t)


    series_list = []

    for t in pos_tuples_in_C:
        series_list.append((C[(C[c_keys[0]] == t[0]) & (C[c_keys[1]] == t[1])]))

    if len(series_list) == 0:
        print('There are no diff tuples in C')
    else:
        pos_tuples = pd.concat(series_list)

    neg_tuples = neg_tuples.sample(n-len(pos_tuples), random_state=random_state)

    pos_tuples[label_col] = 1
    neg_tuples[label_col] = 0
    concat_df =  pd.concat([pos_tuples, neg_tuples], ignore_index=True)
    concat_df = concat_df.sample(frac=1).reset_index(drop=True)
    em.copy_properties(C, concat_df)
    return concat_df
