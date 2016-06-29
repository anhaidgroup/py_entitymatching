from magellan.externals.py_stringsimjoin.filter.filter_utils import get_prefix_length
from magellan.externals.py_stringsimjoin.index.index import Index
from magellan.externals.py_stringsimjoin.utils.token_ordering import order_using_token_ordering


class PositionIndex(Index):
    def __init__(self, table, index_attr, tokenizer, 
                 sim_measure_type, threshold, token_ordering):
        self.table = table
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.index = {}
        self.size_cache = []
        super(self.__class__, self).__init__()

    def build(self):
        row_id = 0
        for row in self.table:
            index_string = row[self.index_attr]
            index_attr_tokens = order_using_token_ordering(
                self.tokenizer.tokenize(index_string), self.token_ordering)
            num_tokens = len(index_attr_tokens)
            prefix_length = get_prefix_length(
                                num_tokens,
                                self.sim_measure_type, self.threshold,
                                self.tokenizer)
 
            pos = 0
            for token in index_attr_tokens[0:prefix_length]:
                if self.index.get(token) is None:
                    self.index[token] = []
                self.index.get(token).append((row_id, pos))
                pos += 1

            self.size_cache.append(num_tokens)
            row_id += 1

        return True

    def probe(self, token):
        return self.index.get(token, [])

    def get_size(self, row_id):
        return self.size_cache[row_id]
