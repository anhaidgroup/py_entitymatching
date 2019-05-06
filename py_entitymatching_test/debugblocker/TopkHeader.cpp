#include "TopkHeader.h"


void original_generate_prefix_events_impl(const Table& table, const int table_indicator,
                                          PrefixHeap& prefix_events) {
    for (unsigned int i = 0; i < table.size(); ++i) {
        unsigned long int length = table[i].size();
        if (length > 0) {
            for (unsigned int j = 0; j < length; ++j) {
                prefix_events.push(PrefixEvent(1.0 - j * 1.0 / length, table_indicator, i, j));
            }
        }
    }
}

void original_generate_prefix_events(const Table& ltable, const Table& rtable,
                                     PrefixHeap& prefix_events) {
    original_generate_prefix_events_impl(ltable, 0, prefix_events);
    original_generate_prefix_events_impl(rtable, 1, prefix_events);
}


int original_plain_get_overlap(const vector<int>& ltoken_list, const vector<int>& rtoken_list) {
    int overlap = 0;
    set<int> rset;

    for (unsigned int i = 0; i < rtoken_list.size(); ++i) {
        rset.insert(rtoken_list[i]);
    }

    for (unsigned int i = 0; i < ltoken_list.size(); ++i) {
        if (rset.count(ltoken_list[i])) {
            ++overlap;
        }
    }

    return overlap;
}
