import logging

import pandas as pd

logger = logging.getLogger(__name__)

def get_feature_fn(feat_str, tok, sim):
    temp = {}
    # update sim
    if sim:
        temp.update(sim)
    if tok:
        temp.update(tok)
    fn = 'def fn(ltuple, rtuple):\n'
    fn += '    '
    fn += 'return ' + feat_str
    d = parse_feat_str(feat_str, tok, sim)
    exec fn in temp
    d['function'] = temp['fn']
    d['function_source'] = fn
    return d

# parse input feature string
def parse_feat_str(str, tok, sim):
    from pyparsing import Word, alphas, alphanums, Literal, ParseException

    # initialization
    exp_flag = False
    left_attribute = 'PARSE_EXP'
    right_attribute = 'PARSE_EXP'
    left_attr_tokenizer = 'PARSE_EXP'
    right_attr_tokenizer = 'PARSE_EXP'
    sim_function = 'PARSE_EXP'

    # parse string
    # define structures for each type
    attr_name = Word(alphanums + "_" + "." + "[" +"]" +'"' +"'")
    tok_fn = Word(alphanums+"_") + "(" + attr_name + ")"
    wo_tok = Word(alphanums+"_") + "(" + attr_name + "," + attr_name + ")"
    wi_tok = Word(alphanums+"_") + "(" + tok_fn + "," + tok_fn + ")"
    feat = wi_tok | wo_tok
    try:
        f = feat.parseString(str)
    except ParseException, e:
        exp_flag = True

    if exp_flag == False:
        t = [val for val in f if val in tok.keys()]
        if len(t) is 2:
            left_attr_tokenizer = t[0]
            right_attr_tokenizer = t[1]
        s = [val for val in f if val in sim.keys()]
        if len(s) is 1:
            sim_function = s[0]
        # get left_attribute
        lt = [val for val in f if val.startswith('ltuple[')]
        if len(lt) is 1:
            lt = lt[0]
            left_attribute = lt[7:len(lt)-1].strip('"').strip("'")
        # get right_attribute
        rt = [val for val in f if val.startswith('rtuple[')]
        if len(rt) is 1:
            rt = rt[0]
            right_attribute = rt[7:len(rt)-1].strip('"').strip("'")
    else:
        pass

    # return the metadata in dictionary form
    d = {}
    d['left_attribute'] = left_attribute
    d['right_attribute'] = right_attribute
    d['left_attr_tokenizer'] = left_attr_tokenizer
    d['right_attr_tokenizer'] = right_attr_tokenizer
    d['simfunction'] = sim_function
    return d


def add_feature(feat_table, feat_name, feat_dict):
    if len(feat_table) > 0:
        feat_names = list(feat_table['feature_name'])
        if feat_name in feat_names:
            logger.warning('Input feature name is already present in feature table')
            return False

    feat_dict['feature_name'] = feat_name
    # rename function
    f = feat_dict['function']
    f_name=feat_name
    #f_name.func_name = feat_name
    exec 'f_name = f'
    feat_dict['function'] = f_name
    if len(feat_table) > 0:
        feat_table.loc[len(feat_table)] = feat_dict
    else:
        feat_table.columns = ['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer',
                              'right_attr_tokenizer', 'simfunction', 'function', 'function_source']
        feat_table.loc[len(feat_table)] = feat_dict
    return True

def create_feature_table():
    feat_table = pd.DataFrame()
    feat_table.columns = ['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer',
                          'right_attr_tokenizer', 'simfunction', 'function', 'function_source']

    return feat_table



def add_blackbox_feature(feat_table, feat_name, feat_fn):
    d = {}
    d['feature_name'] = feat_name
    d['function'] = feat_fn
    d['left_attribute'] = None
    d['right_attribute'] = None
    d['left_attr_tokenizer'] = None
    d['right_attr_tokenizer'] = None
    d['simfunction'] = None
    d['function_source'] = None

    if len(feat_table) > 0:
        feat_table.loc[len(feat_table)] = d
    else:
        feat_table.columns = ['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer',
                              'right_attr_tokenizer', 'simfunction', 'function', 'function_source']
        feat_table.loc[len(feat_table)] = d
    return True