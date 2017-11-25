class PrefixEvent {
public:
    double threshold;
    int table_indicator;
    int rec_idx;
    int tok_idx;
    
    PrefixEvent();
    PrefixEvent(double threshold, int table_indicator, int rec_idx, int tok_idx);
    ~PrefixEvent();

    bool operator<(const PrefixEvent &other) const;
    bool operator>(const PrefixEvent &other) const;
};
