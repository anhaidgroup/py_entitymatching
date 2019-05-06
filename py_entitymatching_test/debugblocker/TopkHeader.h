#ifndef __TOPKHEADER_H__
#define __TOPKHEADER_H__

#include <iostream>
#include <vector>
#include <set>
#include <map>
#include <queue>
#include <string>
#include <cstdio>
#include "TopPair.h"
#include "PrefixEvent.h"

using namespace std;

typedef priority_queue<TopPair> Heap;
typedef map<int, set<int> > CandSet;
typedef map<int, set<pair<int, int> > > InvertedIndex;
typedef vector<vector<int> > Table;
typedef priority_queue<PrefixEvent> PrefixHeap;


Heap original_topk_sim_join_plain(const Table& ltoken_vector, const Table& rtoken_vector,
                                  CandSet& cand_set, const unsigned int output_size);


int original_plain_get_overlap(const vector<int>& ltoken_list, const vector<int>& rtoken_list);


void original_generate_prefix_events_impl(const Table& table, const int table_indicator,
                                          PrefixHeap& prefix_events);

void original_generate_prefix_events(const Table& ltable, const Table& rtable,
                                     PrefixHeap& prefix_events);

#endif //__TOPKHEADER_H__
