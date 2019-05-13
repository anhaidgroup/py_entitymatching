#include "TopkHeader.h"


double inline ori_absdiff(double a, double b) {
    double v = a - b;
    if (v < 0) {
        v = v * -1;
    }
    return v;
}


void original_topk_sim_join_plain_impl(const Table& ltoken_vector, const Table& rtoken_vector,
                                       CandSet& cand_set, PrefixHeap& prefix_events,
                                       Heap& topk_heap, const unsigned int output_size) {
    long int total_compared_pairs = 0;
    CandSet compared_set;

    InvertedIndex l_inverted_index, r_inverted_index;
    PrefixEvent event;
    int table_indicator, l_rec_idx, r_rec_idx, l_tok_idx, r_tok_idx, token, overlap;
    unsigned long l_len, r_len;
    double sim;

    while (prefix_events.size() > 0) {
        if (topk_heap.size() == output_size && (topk_heap.top().sim >= prefix_events.top().threshold ||
                                                ori_absdiff(topk_heap.top().sim, prefix_events.top().threshold) <= 1e-6)) {
            break;
        }
        event = prefix_events.top();
        prefix_events.pop();
        table_indicator = event.table_indicator;
        if (table_indicator == 0) {
            l_rec_idx = event.rec_idx;
            l_tok_idx = event.tok_idx;
            token = ltoken_vector[l_rec_idx][l_tok_idx];
            l_len = ltoken_vector[l_rec_idx].size();
            if (r_inverted_index.count(token)) {
                set<pair<int, int> > r_records = r_inverted_index[token];

                for (set<pair<int, int> >::iterator it = r_records.begin(); it != r_records.end(); ++it) {
                    pair<int, int> r_rec_tuple = *it;
                    r_rec_idx = r_rec_tuple.first;
                    r_tok_idx = r_rec_tuple.second;
                    r_len = rtoken_vector[r_rec_idx].size();

                    if (topk_heap.size() == output_size && (l_len < topk_heap.top().sim * r_len || l_len > r_len / topk_heap.top().sim)) {
                        continue;
                    }

                    if (cand_set.count(l_rec_idx) && cand_set[l_rec_idx].count(r_rec_idx)) {
                        continue;
                    }

                    if (compared_set.count(l_rec_idx) && compared_set[l_rec_idx].count(r_rec_idx)) {
                        continue;
                    }

                    overlap = original_plain_get_overlap(ltoken_vector[l_rec_idx], rtoken_vector[r_rec_idx]);
                    sim = overlap * 1.0 / (l_len + r_len - overlap);
                    if (topk_heap.size() == output_size) {
                        if (topk_heap.top().sim < sim) {
                            topk_heap.pop();
                            topk_heap.push(TopPair(sim, l_rec_idx, r_rec_idx));
                        }
                    } else {
                        topk_heap.push(TopPair(sim, l_rec_idx, r_rec_idx));
                    }
                    ++total_compared_pairs;
                    if (!compared_set.count(l_rec_idx)) {
                        compared_set[l_rec_idx] = set<int>();
                    }
                    compared_set[l_rec_idx].insert(r_rec_idx);
                }
            }

            double topk_heap_sim_index = 0.0;
            if (topk_heap.size() == output_size) {
                topk_heap_sim_index = topk_heap.top().sim;
            }

            double index_threshold = 1.0;
            int numer_index = l_len - l_tok_idx;
            int denom_index = l_len + l_tok_idx;
            if (denom_index > 0) {
                index_threshold = numer_index * 1.0 / denom_index;
            }

            if (index_threshold >= topk_heap_sim_index) {
                if (!l_inverted_index.count(token)) {
                    l_inverted_index[token] = set<pair<int, int> >();
                }
                l_inverted_index[token].insert(pair<int, int>(l_rec_idx, l_tok_idx));
            }

        } else {
            r_rec_idx = event.rec_idx;
            r_tok_idx = event.tok_idx;
            token = rtoken_vector[r_rec_idx][r_tok_idx];
            r_len = rtoken_vector[r_rec_idx].size();
            if (l_inverted_index.count(token)) {
                set<pair<int, int> > l_records = l_inverted_index[token];
                for (set<pair<int, int> >::iterator it = l_records.begin(); it != l_records.end(); ++it) {
                    pair<int, int> l_rec_tuple = *it;
                    l_rec_idx = l_rec_tuple.first;
                    l_tok_idx = l_rec_tuple.second;
                    l_len = ltoken_vector[l_rec_idx].size();

                    if (topk_heap.size() == output_size && (l_len < topk_heap.top().sim * r_len || l_len > r_len / topk_heap.top().sim)) {
                        continue;
                    }

                    if (cand_set.count(l_rec_idx) && cand_set[l_rec_idx].count(r_rec_idx)) {
                        continue;
                    }

                    if (compared_set.count(l_rec_idx) && compared_set[l_rec_idx].count(r_rec_idx)) {
                        continue;
                    }

                    overlap = original_plain_get_overlap(ltoken_vector[l_rec_idx], rtoken_vector[r_rec_idx]);
                    sim = overlap * 1.0 / (l_len + r_len - overlap);
                    if (topk_heap.size() == output_size) {
                        if (topk_heap.top().sim < sim) {
                            topk_heap.pop();
                            topk_heap.push(TopPair(sim, l_rec_idx, r_rec_idx));
                        }
                    } else {
                        topk_heap.push(TopPair(sim, l_rec_idx, r_rec_idx));
                    }
                    ++total_compared_pairs;
                    if (!compared_set.count(l_rec_idx)) {
                        compared_set[l_rec_idx] = set<int>();
                    }
                    compared_set[l_rec_idx].insert(r_rec_idx);
                }
            }

            double topk_heap_sim_index = 0.0;
            if (topk_heap.size() == output_size) {
                topk_heap_sim_index = topk_heap.top().sim;
            }
            double index_threshold = 1.0;
            int numer_index = r_len - r_tok_idx;
            int denom_index = r_len + r_tok_idx;
            if (denom_index > 0) {
                index_threshold = numer_index * 1.0 / denom_index;
            }
            if (index_threshold >= topk_heap_sim_index) {
                if (!r_inverted_index.count(token)) {
                    r_inverted_index[token] = set<pair<int, int> >();
                }
                r_inverted_index[token].insert(pair<int, int>(r_rec_idx, r_tok_idx));
            }

        }
    }
}


Heap original_topk_sim_join_plain(const Table& ltoken_vector, const Table& rtoken_vector,
                                  CandSet& cand_set, const unsigned int output_size) {
    PrefixHeap prefix_events;
    original_generate_prefix_events(ltoken_vector, rtoken_vector, prefix_events);

    Heap topk_heap;
    original_topk_sim_join_plain_impl(ltoken_vector, rtoken_vector, cand_set, prefix_events, topk_heap, output_size);

    return topk_heap;
}
