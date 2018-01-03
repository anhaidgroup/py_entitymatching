#include "GenerateRecomLists.h"
GenerateRecomLists::GenerateRecomLists() {}


GenerateRecomLists::~GenerateRecomLists() {}

void inline copy_table_and_remove_fields(const vector<int>& config, const Table& table_vector, const Table& index_vector,
                                         Table& new_table_vector, Table& new_index_vector) {
    set<int> field_set;
    for (unsigned int i = 0; i < config.size(); i ++) {
        field_set.insert(config[i]);
    }

    for (unsigned int i = 0; i < table_vector.size(); ++i) {
        new_table_vector.push_back(vector<int> ());
        new_index_vector.push_back(vector<int> ());
        for (unsigned int j = 0; j < table_vector[i].size(); ++j) {
            if (field_set.count(index_vector[i][j])) {
                new_table_vector[i].push_back(table_vector[i][j]);
                new_index_vector[i].push_back(table_vector[i][j]);
            }
        }
    }
}

vector<RecPair> GenerateRecomLists::generate_recom_lists(
                              Table& ltoken_vector, Table& rtoken_vector,
                              Table& lindex_vector, Table& rindex_vector,
                              vector<int>& ltoken_sum_vector, vector<int>& rtoken_sum_vector, vector<int>& field_list,
                              CandSet& cand_set, double field_remove_ratio,
                              unsigned int output_size) {

    Table config_lists = generate_config(field_list, ltoken_sum_vector, rtoken_sum_vector, field_remove_ratio,
                           ltoken_vector.size(), rtoken_vector.size()); 

    int config_num = config_lists.size();
    vector<TopkRankList > rec_lists(config_num);
    for (int i = 0; i < config_num; i ++) {
      rec_lists[i] = generate_topk_with_config(config_lists[i], ltoken_vector, rtoken_vector, lindex_vector, 
                                              rindex_vector, cand_set, output_size);
    }
    vector<RecPair> reclist = merge_topk_lists(rec_lists);
    return reclist;
}

/*
 * Generate Topk list for given config.
 */
TopkRankList GenerateRecomLists::generate_topk_with_config(
                              vector<int>& config,
                              Table& ltoken_vector, Table& rtoken_vector,
                              Table& lindex_vector, Table& rindex_vector,
                              CandSet& cand_set, unsigned int output_size) {
    Heap topkheap;
    Table new_ltoken_vector, new_rtoken_vector, new_lindex_vector, new_rindex_vector;
    copy_table_and_remove_fields(config, ltoken_vector, lindex_vector, new_ltoken_vector, new_lindex_vector);
    copy_table_and_remove_fields(config, rtoken_vector, rindex_vector, new_rtoken_vector, new_rindex_vector);
    topkheap = original_topk_sim_join_plain(new_ltoken_vector, new_rtoken_vector, cand_set, output_size);

    TopkRankList topkrank;
    int count = 0;
    while(!topkheap.empty()) {
      topkrank[make_pair(topkheap.top().l_rec, topkheap.top().r_rec)] = ++ count;
      topkheap.pop();
    }
    return topkrank;
}

bool cmp(const RecPair& lhs, const RecPair& rhs) {
  return lhs.rank < rhs.rank;
}

vector<RecPair> GenerateRecomLists::merge_topk_lists(vector<TopkRankList >& rec_lists) {
  vector<RecPair> rec_list;
  set<pair<int, int> > full_set;
  
  // Initialize the rankings for each topk recommendation list
  for (unsigned int i = 0; i < rec_lists.size(); i ++) {
    for (TopkRankList::iterator it = rec_lists[i].begin(); it != rec_lists[i].end(); it ++) {
      full_set.insert(make_pair(it->first.first, it->first.second));
    }
  }
  if (rec_lists.empty()) return rec_list;
  int list_size = rec_lists[0].size();

  // Get the median of each topk recommendation
  for (set<pair<int, int> >::iterator it = full_set.begin(); it != full_set.end(); it ++) {
    vector<int> tmp;
   
    // for each topk recommendation, if it does not exists in the rec_list of certain config, then its rank
    // under this config would be list_size + 1, otherwise use its original rank
    for (unsigned int i = 0; i < rec_lists.size(); i ++) {
      if (rec_lists[i].find(*it) == rec_lists[i].end()) {
        tmp.push_back(list_size + 1);
      } else {
        tmp.push_back(rec_lists[i][*it]);
      }
    }

    // use the median of ranks in all the configs as the final rank
    sort(tmp.begin(), tmp.end());
    if (tmp.size() & 1) {
      rec_list.push_back(RecPair((*it).first, (*it).second, tmp[tmp.size() / 2]));
    } else {
      rec_list.push_back(RecPair((*it).first, (*it).second, (tmp[tmp.size() / 2 - 1] + tmp[tmp.size() / 2]) / 2));
    }
  }
  sort(rec_list.begin(), rec_list.end(), cmp);
  return rec_list;
}

Table GenerateRecomLists::generate_config(const vector<int>& field_list, const vector<int>& ltoken_sum_vector,
                           const vector<int>& rtoken_sum_vector, const double field_remove_ratio,
                           const unsigned int ltable_size, const unsigned int rtable_size) {
    Table config_lists;
    vector<int> feat_list_copy = field_list;
    config_lists.push_back(feat_list_copy);
    while (feat_list_copy.size() > 1) {
        double max_ratio = 0.0;
        unsigned int ltoken_total_sum = 0, rtoken_total_sum = 0;
        int removed_field_index = -1;

        for (unsigned int i = 0; i < feat_list_copy.size(); ++i) {
            ltoken_total_sum += ltoken_sum_vector[feat_list_copy[i]];
            rtoken_total_sum += rtoken_sum_vector[feat_list_copy[i]];
        }

        double lrec_ave_len = ltoken_total_sum * 1.0 / ltable_size;
        double rrec_ave_len = rtoken_total_sum * 1.0 / rtable_size;
        double ratio = 1 - (feat_list_copy.size() - 1) * field_remove_ratio / (1.0 + field_remove_ratio) *
                 double_max(lrec_ave_len, rrec_ave_len) / (lrec_ave_len + rrec_ave_len);

        for (unsigned int i = 0; i < feat_list_copy.size(); ++i) {
            max_ratio = double_max(max_ratio, double_max(ltoken_sum_vector[feat_list_copy[i]] * 1.0 / ltoken_total_sum,
                                                         rtoken_sum_vector[feat_list_copy[i]] * 1.0 / rtoken_total_sum));
            if (ltoken_sum_vector[feat_list_copy[i]] > ltoken_total_sum * ratio ||
                    rtoken_sum_vector[feat_list_copy[i]] > rtoken_total_sum * ratio) {
                removed_field_index = i;
                break;
            }
        }

        if (removed_field_index < 0) {
            removed_field_index = feat_list_copy.size() - 1;
        }
        // add the one does not contain removed_field_index first.
        vector<int> temp = feat_list_copy;
        temp.erase(temp.begin() + removed_field_index);
        if (temp.size() > 0) {
            config_lists.push_back(temp);
        }
        for (unsigned int i = 0; i < feat_list_copy.size(); ++i) {
            if ((int) i == removed_field_index) {
              continue;
            }
            vector<int> temp = feat_list_copy;
            temp.erase(temp.begin() + i);
            if (temp.size() <= 0) {
                continue;
            }
            config_lists.push_back(temp);
        }
        feat_list_copy.erase(feat_list_copy.begin() + removed_field_index);
    }

    return config_lists;
}

Table GenerateRecomLists::sort_config(Table& config_lists) {
  if (config_lists.empty()) {
    return config_lists;
  }
  Table sorted_config_lists;
  Table temp;
  sorted_config_lists.push_back(config_lists[0]);
  for (unsigned int i = 1; i < config_lists.size(); i ++) {
    if (config_lists[i].size() == config_lists[i - 1].size()) {
      temp.push_back(config_lists[i]);
    } else {
      sorted_config_lists.push_back(config_lists[i]);
    }
  }
  for (unsigned int i = 0; i < temp.size(); i ++) {
    sorted_config_lists.push_back(temp[i]);
  }
  return sorted_config_lists;
}


double double_max(const double a, double b) {
    if (a > b) {
        return a;
    }
    return b;
}
