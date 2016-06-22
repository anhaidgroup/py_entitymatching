import logging

import pandas as pd
import six

from magellan.feature.simfunctions import get_sim_funs_for_blocking, get_sim_funs_for_matching
from magellan.feature.tokenizers import  get_tokenizers_for_blocking, get_tokenizers_for_matching
from magellan.feature.attributeutils import get_attr_corres, get_attr_types

import magellan as mg


logger = logging.getLogger(__name__)


def get_features(ltable, rtable, l_attr_types, r_attr_types, attr_corres, tok_funcs, sim_funcs):

    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas dataframe')
        raise AssertionError('Input ltable is not of type pandas dataframe')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas dataframe')
        raise AssertionError('Input rtable is not of type pandas dataframe')



    if check_table_order(ltable, rtable, l_attr_types, r_attr_types, attr_corres) == False:
        logger.error('Table order is different than what is mentioned in l/r attr_types and attr_corres')
        raise AssertionError('Table order is different than what is mentioned in l/r attr_types and attr_corres')

    # initialize output feature dictionary list
    feat_dict_list = []

    # generate features for each attr. correspondence
    for ac in attr_corres['corres']:
        l_attr_type = l_attr_types[ac[0]]
        r_attr_type = r_attr_types[ac[1]]

        if l_attr_type != r_attr_type:
            logger.warning('Magellan types: %s type (%s) and %s type (%s) are different.'
                           'If you want to set them to be same and generate features, '
                           'update output from get_attr_types and use get_features command.\n'
                           % (ac[0], l_attr_type, ac[1], r_attr_type))
            continue

        feats = get_features_for_type(l_attr_type)

        # convert features to function objects
        fn_objs = conv_func_objs(feats, ac, tok_funcs, sim_funcs)
        feat_dict_list.append(fn_objs)

    df = pd.DataFrame(flatten_list(feat_dict_list))
    df = df[['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer', 'right_attr_tokenizer',
             'simfunction', 'function', 'function_source', 'is_auto_generated']]
    return df


def get_features_for_blocking(A, B):
    if not isinstance(A, pd.DataFrame):
        logger.error('Input table A is not of type pandas dataframe')
        raise AssertionError('Input table A is not of type pandas dataframe')

    if not isinstance(B, pd.DataFrame):
        logger.error('Input table B is not of type pandas dataframe')
        raise AssertionError('Input table B is not of type pandas dataframe')

    sim_funcs = get_sim_funs_for_blocking()
    tok_funcs = get_tokenizers_for_blocking()
    t_A = get_attr_types(A)
    t_B = get_attr_types(B)
    attr_corres = get_attr_corres(A, B)
    feat_table = get_features(A, B, t_A, t_B, attr_corres, tok_funcs, sim_funcs)

    # export important variables to global name space
    mg._block_t = tok_funcs
    mg._block_s = sim_funcs
    mg._atypes1 = t_A
    mg._atypes2 = t_B
    mg._block_c = attr_corres
    return feat_table


def get_features_for_matching(A, B):
    if not isinstance(A, pd.DataFrame):
        logger.error('Input table A is not of type pandas dataframe')
        raise AssertionError('Input table A is not of type pandas dataframe')

    if not isinstance(B, pd.DataFrame):
        logger.error('Input table B is not of type pandas dataframe')
        raise AssertionError('Input table B is not of type pandas dataframe')

    sim_funcs = get_sim_funs_for_matching()
    tok_funcs = get_tokenizers_for_matching()
    t_A = get_attr_types(A)
    t_B = get_attr_types(B)
    attr_corres = get_attr_corres(A, B)
    feat_table = get_features(A, B, t_A, t_B, attr_corres, tok_funcs, sim_funcs)

    # export important variables to global name space
    mg._match_t = tok_funcs
    mg._match_s = sim_funcs
    mg._atypes1 = t_A
    mg._atypes2 = t_B
    mg._match_c = attr_corres
    return feat_table


# check whether the order of tables matches with what is mentioned in  l_attr_types, r_attr_type and attr_corres
def check_table_order(ltable, rtable, l_attr_types, r_attr_types, attr_corres):

    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas dataframe')
        raise AssertionError('Input ltable is not of type pandas dataframe')

    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas dataframe')
        raise AssertionError('Input rtable is not of type pandas dataframe')

    # get the ids
    l_id = id(ltable)
    r_id = id(rtable)

    # check whether ltable id matches with id of table mentioned in l_attr_types
    if l_id != id(l_attr_types['_table']):
        logger.error('ltable is not the same as table mentioned in left attr types')
        return False

    # check whether rtable id matches with id of table mentioned in r_attr_types
    if r_id != id(r_attr_types['_table']):
        logger.error('rtable is not the same as table mentioned in right attr types')
        return False

    # check whether ltable matches with ltable mentioned in attr_corres
    if l_id != id(attr_corres['ltable']):
        logger.error('ltable is not the same as table mentioned in attr correspondence')
        return False

    # check whether rtable matches with rtable mentioned in attr_corres
    if r_id != id(attr_corres['rtable']):
        logger.error('rtable is not the same as table mentioned in attr correspondence')
        return False

    return True


# get look up table to generate features
def get_feat_lkp_tbl():
    lkp_tbl = dict()

    # THE FOLLOWING MUST BE MODIFIED
    # features for type str_eq_1w
    lkp_tbl['STR_EQ_1W'] = [('lev'), ('jaro'), ('jaro_winkler'), ('exact_match'),
                            ('jaccard', 'qgm_3', 'qgm_3')]

    # features for type str_bt_1w_5w
    lkp_tbl['STR_BT_1W_5W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                               ('cosine', 'dlm_dc0', 'dlm_dc0'),
                               ('jaccard', 'dlm_dc0', 'dlm_dc0'),
                               ('monge_elkan'), ('lev'), ('needleman_wunsch'), ('smith_waterman')

                               ]  # dlm_dc0 is the concrete space tokenizer

    # features for type str_bt_5w_10w
    lkp_tbl['STR_BT_5W_10W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                                ('cosine', 'dlm_dc0', 'dlm_dc0'),
                                ('monge_elkan'), ('lev')]

    # features for type str_gt_10w
    lkp_tbl['STR_GT_10W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                             ('cosine', 'dlm_dc0', 'dlm_dc0')]

    # features for NUMERIC type
    # lkp_tbl['NUM'] = [('rel_diff')]
    lkp_tbl['NUM'] = [('exact_match'), ('abs_norm'), ('lev')]

    # features for BOOLEAN type
    lkp_tbl['BOOL'] = [('exact_match')]

    return lkp_tbl


# get features to be generated for a type
def get_features_for_type(t):
    lkp_tbl = get_feat_lkp_tbl()
    if t is 'str_eq_1w':
        rec_fns = lkp_tbl['STR_EQ_1W']
    elif t is 'str_bt_1w_5w':
        rec_fns = lkp_tbl['STR_BT_1W_5W']
    elif t is 'str_bt_5w_10w':
        rec_fns = lkp_tbl['STR_BT_5W_10W']
    elif t is 'str_gt_10w':
        rec_fns = lkp_tbl['STR_GT_10W']
    elif t is 'numeric':
        rec_fns = lkp_tbl['NUM']
    elif t is 'boolean':
        rec_fns = lkp_tbl['BOOL']
    else:
        raise TypeError('Unknown type')
    return rec_fns

# get types
def get_magellan_str_types():
    return ['str_eq_1w', 'str_bt_1w_5w', 'str_bt_5w_10w', 'str_gt_10w', 'numeric','boolean']

# convert features from look up table to function objects
def conv_func_objs(feats, attrs, tok_funcs, sim_funcs):
    tok_list = tok_funcs.keys()
    sim_list = sim_funcs.keys()
    valid_list = [check_valid_tok_sim(i, tok_list, sim_list) for i in feats]
    # get function as a string and other meta data; finally we will get a list of tuples
    func_tuples = [get_fn_str(inp, attrs) for inp in valid_list]
    # print func_tuples
    func_objs = conv_fn_str_to_obj(func_tuples, tok_funcs, sim_funcs)
    return func_objs


# check whether tokenizers and simfunctions are allowed
# inp is of the form ('jaccard', 'qgm_3', 'qgm_3') or ('lev')
def check_valid_tok_sim(inp, simlist, toklist):
    if isinstance(inp, six.string_types):
        inp = [inp]
    assert len(inp) == 1 or len(inp) == 3, 'len of feature config should be 1 or 3'
    # check whether the sim function in features is in simlist
    if len(set(inp).intersection(simlist)) > 0:
        return inp
    # check whether the tokenizer in features is in tok list
    if len(set(inp).intersection(toklist)) > 0:
        return inp
    return None


# get function string for a feature
def get_fn_str(inp, attrs):
    if inp:
        args = []
        args.extend(attrs)
        if isinstance(inp, six.string_types) == True:
            inp = [inp]
        args.extend(inp)
        # fill function string from a template
        return fill_fn_template(*args)
    else:
        return None


# fill function template
def fill_fn_template(attr1, attr2, sim_func, tok_func_1=None, tok_func_2=None):
    # construct function string
    s = 'from magellan.feature.simfunctions import *\nfrom magellan.feature.tokenizers import *\n'
    # get the function name
    fn_name = get_fn_name(attr1, attr2, sim_func, tok_func_1, tok_func_2)
    # proceed with function construction
    fn_st = 'def ' + fn_name + '(ltuple, rtuple):'
    s += fn_st
    s += '\n'

    # add 4 spaces
    s += '    '
    fn_body = 'return '
    if tok_func_1 is not None and tok_func_2 is not None:
        fn_body = fn_body + sim_func + '(' + tok_func_1 + '(' + 'ltuple["' + attr1 + '"]'
        fn_body += '), '
        fn_body = fn_body + tok_func_2 + '(' + 'rtuple["' + attr2 + '"]'
        fn_body = fn_body + ')) '
    else:
        fn_body = fn_body + sim_func + '(' + 'ltuple["' + attr1 + '"], rtuple["' + attr2 + '"])'
    s += fn_body

    return fn_name, attr1, attr2, tok_func_1, tok_func_2, sim_func, s


# construct function name from attrs, tokenizers and sim funcs

# sim_fn_names=['jaccard', 'lev', 'cosine', 'monge_elkan',
#               'needleman_wunsch', 'smith_waterman', 'jaro', 'jaro_winkler',
#               'exact_match', 'rel_diff', 'abs_norm']
def get_fn_name(attr1, attr2, sim_func, tok_func_1=None, tok_func_2=None):
    fp = '_'.join([attr1, attr2])
    name_lkp = dict()
    name_lkp["jaccard"] = "jac"
    name_lkp["lev"] = "lev"
    name_lkp["cosine"] = "cos"
    name_lkp["monge_elkan"] = "mel"
    name_lkp["needleman_wunsch"] = "nmw"
    name_lkp["smith_waterman"] = "sw"
    name_lkp["jaro"] = "jar"
    name_lkp["jaro_winkler"] = "jwn"
    # name_lkp["soundex"] = "sdx"
    name_lkp["exact_match"] = "exm"
    name_lkp["abs_norm"] = "anm"
    name_lkp["rel_diff"] = "rdf"
    name_lkp["1"] = "1"
    name_lkp["2"] = "2"
    name_lkp["3"] = "3"
    name_lkp["4"] = "4"
    name_lkp["tok_whitespace"] = "wsp"
    name_lkp["tok_qgram"] = "qgm"
    name_lkp["tok_delim"] = "dlm"

    arg_list = [sim_func, tok_func_1, tok_func_2]
    nm_list = [name_lkp.get(tok, tok) for tok in arg_list if tok]
    sp = '_'.join(nm_list)
    return '_'.join([fp, sp])


# conv function string to function object and return with meta data
def conv_fn_str_to_obj(fn_tup, tok, sim_funcs):
    d_orig = {}
    d_orig.update(tok)
    d_orig.update(sim_funcs)
    d_ret_list = []
    for f in fn_tup:
        d_ret = {}
        name = f[0]
        attr1 = f[1]
        attr2 = f[2]
        tok_1 = f[3]
        tok_2 = f[4]
        simfunction = f[5]
        # exec(f[6] in d_orig)
        six.exec_(f[6], d_orig)
        d_ret['function'] = d_orig[name]
        d_ret['feature_name'] = name
        d_ret['left_attribute'] = attr1
        d_ret['right_attribute'] = attr2
        d_ret['left_attr_tokenizer'] = tok_1
        d_ret['right_attr_tokenizer'] = tok_2
        d_ret['simfunction'] = simfunction
        d_ret['function_source'] = f[6]
        d_ret['is_auto_generated'] = True

        d_ret_list.append(d_ret)
    return d_ret_list


def flatten_list(inp_list):
    return [item for sublist in inp_list for item in sublist]
