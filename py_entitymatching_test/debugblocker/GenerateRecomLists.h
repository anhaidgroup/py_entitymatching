#ifndef TEST_GENERATERECOMLISTS_H
#define TEST_GENERATERECOMLISTS_H

#include <vector>
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <algorithm>
#include <stdio.h>
#include "TopkHeader.h"
using namespace std;

typedef map<int, set<int> > CandSet;
typedef vector<vector<int> > Table;
typedef map<pair<int, int>, int> TopkRankList;

double double_max(const double a, double b);

class RecPair {
public:
  int l_rec, r_rec, rank;
  RecPair(int l_rec, int r_rec, int rank) : l_rec(l_rec), r_rec(r_rec), rank(rank){

  }
};
class GenerateRecomLists {
public:

    Table generate_config(const vector<int>& field_list, const vector<int>& ltoken_sum_vector,
                              const vector<int>& rtoken_sum_vector, const double field_remove_ratio,
                              const unsigned int ltable_size, const unsigned int rtable_size);
    Table sort_config(Table& config_lists);

    TopkRankList generate_topk_with_config(vector<int>& config, Table& ltoken_vector, Table& rtoken_vector,
                                  Table& lindex_vector, Table& rindex_vector,
                                  CandSet& cand_set, unsigned int output_size);

    vector<RecPair> generate_recom_lists(Table& ltoken_vector, Table& rtoken_vector,
                              Table& lindex_vector, Table& rindex_vector,
                              vector<int>& ltoken_sum_vector, vector<int>& rtoken_sum_vector, vector<int>& field_list,
                              CandSet& cand_set, double field_remove_ratio,
                              unsigned int output_size);

    vector<RecPair> merge_topk_lists(vector<TopkRankList >& rec_lists);
    GenerateRecomLists();
    ~GenerateRecomLists();
};


#endif //TEST_GENERATERECOMLISTS_H
