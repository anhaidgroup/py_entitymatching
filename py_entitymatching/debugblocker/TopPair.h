#ifndef TEST_TOPPAIR_H
#define TEST_TOPPAIR_H

class TopPair {
public:
    double sim;
    int l_rec;
    int r_rec;

    TopPair();
    TopPair(double similarity, int l_rec_idx, int r_rec_idx);
    ~TopPair();

    bool operator<(const TopPair &other) const;
    bool operator>(const TopPair &other) const;
};


#endif //TEST_TOPPAIR_H
