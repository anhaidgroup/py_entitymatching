from magellan.externals.py_stringsimjoin.index.index import Index


class InvertedIndex(Index):
    def __init__(self, table, index_attr, tokenizer, 
                 cache_size_flag=False):
        self.table = table
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.index = {}
        self.size_cache = []
        self.cache_size_flag = cache_size_flag
        super(self.__class__, self).__init__()

    def build(self):
        row_id = 0
        for row in self.table:
            index_string = str(row[self.index_attr])
            index_attr_tokens = self.tokenizer.tokenize(index_string)

            for token in index_attr_tokens:
                if self.index.get(token) is None:
                    self.index[token] = []
                self.index.get(token).append(row_id)

            if self.cache_size_flag:
                self.size_cache.append(len(index_attr_tokens))
            row_id += 1

        return True

    def probe(self, token):
        return self.index.get(token, [])
