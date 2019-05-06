#include "PrefixEvent.h"

PrefixEvent::PrefixEvent() { }

PrefixEvent::PrefixEvent(double thres, int indicator, int rec, int tok) {
    threshold = thres;
    table_indicator = indicator;
    rec_idx = rec;
    tok_idx = tok;
}

PrefixEvent::~PrefixEvent() { }

bool PrefixEvent::operator<(const PrefixEvent &other) const
{
 return threshold < other.threshold;
}

bool PrefixEvent::operator>(const PrefixEvent &other) const
{
 return threshold > other.threshold;
}
