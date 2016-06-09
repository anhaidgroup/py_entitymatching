from py_stringsimjoin.index.index import Index
from py_stringsimjoin.utils.tokenizers import tokenize


class SizeIndex(Index):
    def __init__(self, table, key_attr, index_attr, tokenizer):
        self.table = table
        self.key_attr = key_attr
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.index = {}
        self.min_length = 0
        self.max_length = 0
        super(self.__class__, self).__init__()

    def build(self):
        for row in self.table:
            index_string = str(row[self.index_attr])
            # check for empty string
            if not index_string:
                continue
            num_tokens = len(tokenize(index_string, self.tokenizer))
            
            if self.index.get(num_tokens) is None:
                self.index[num_tokens] = []

            self.index.get(num_tokens).append(row[self.key_attr])

            if num_tokens < self.min_length:
                self.min_length = num_tokens
 
            if num_tokens > self.max_length:
                self.max_length = num_tokens  

        return True

    def probe(self, num_tokens):
        return self.index.get(num_tokens, [])
