import logging


import pandas as pd
import six

logger = logging.getLogger(__name__)


def get_feature_fn(feat_str, tok, sim):
    if not isinstance(feat_str, six.string_types):
        logger.error('Input feature string is not of type string')
        raise AssertionError('Input feature string is not of type string')

    if not isinstance(tok, dict):
        logger.error('Input tok is not of type dict')
        raise AssertionError('Input tok. is not of type dict')

    if not isinstance(sim, dict):
        logger.error('Input sim is not of type dict')
        raise AssertionError('Input sim. is not of type dict')
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
    # six.exec_(fn, temp, temp)
    six.exec_(fn, temp)
    d['function'] = temp['fn']
    d['function_source'] = fn
    return d


# parse input feature string
def parse_feat_str(feature_string, tok, sim):
    if not isinstance(feature_string, six.string_types):
        logger.error('Input feature string is not of type string')
        raise AssertionError('Input feature string is not of type string')

    if not isinstance(tok, dict):
        logger.error('Input tok is not of type dict')
        raise AssertionError('Input tok. is not of type dict')

    if not isinstance(sim, dict):
        logger.error('Input sim is not of type dict')
        raise AssertionError('Input sim. is not of type dict')

    from pyparsing import Word, alphas, alphanums, Literal, ParseException

    # initialization
    exp_flag = False
    left_attribute = 'PARSE_EXP'
    right_attribute = 'PARSE_EXP'
    left_attr_tokenizer = 'PARSE_EXP'
    right_attr_tokenizer = 'PARSE_EXP'
    sim_function = 'PARSE_EXP'

    exp_flag=False

    # parse string
    # define structures for each type
    attr_name = Word(alphanums + "_" + "." + "[" + "]" + '"' + "'")
    tok_fn = Word(alphanums + "_") + "(" + attr_name + ")"
    wo_tok = Word(alphanums + "_") + "(" + attr_name + "," + attr_name + ")"
    wi_tok = Word(alphanums + "_") + "(" + tok_fn + "," + tok_fn + ")"
    feat = wi_tok | wo_tok
    try:
        f = feat.parseString(feature_string)
    except ParseException as e:
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
            left_attribute = lt[7:len(lt) - 1].strip('"').strip("'")
        # get right_attribute
        rt = [val for val in f if val.startswith('rtuple[')]
        if len(rt) is 1:
            rt = rt[0]
            right_attribute = rt[7:len(rt) - 1].strip('"').strip("'")
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
    if not isinstance(feat_table, pd.DataFrame):
        logger.error('Input feature table is not of type data frame')
        raise AssertionError('Input feature table is not of type data frame')

    if not isinstance(feat_name, six.string_types):
        logger.error('Input feature name is not of type string')
        raise AssertionError('Input feature name is not of type string')

    if not isinstance(feat_dict, dict):
        logger.error('Input feature dictionary is not of type dict')
        raise AssertionError('Input feature dictionary is not of type dict')

    dummy_feature_table = create_feature_table()
    if sorted(dummy_feature_table.columns) != sorted(feat_table.columns):
        logger.error('Input feature table does not have the necessary columns')
        raise AssertionError('Input feature table does not have the necessary columns')


    feat_names = list(feat_table['feature_name'])
    if feat_name in feat_names:
        logger.error('Input feature name is already present in feature table')
        raise AssertionError('Input feature name is already present in feature table')




    feat_dict['feature_name'] = feat_name
    # rename function
    f = feat_dict['function']
    f_name = feat_name
    # f.func_name = feature_name
    # feat_dict['function'].func_name = feat_name
    # six.exec_('f_name = f', locals=locals()) # check this
    # if six.PY2 == True:
    #     six.exec_('f_name=f')
    #     # feat_dict['function'] = f_name
    #     # six.exec_('f_name=feat_dict["function"]')
    #     feat_dict['function'] = f_name
    # else:
    #     pass

    if len(feat_table) > 0:
        feat_table.loc[len(feat_table)] = feat_dict
    else:
        feat_table.columns = ['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer',
                              'right_attr_tokenizer', 'simfunction', 'function', 'function_source']
        feat_table.loc[len(feat_table)] = feat_dict
    return True


def create_feature_table():
    cols = ['feature_name', 'left_attribute', 'right_attribute', 'left_attr_tokenizer',
            'right_attr_tokenizer', 'simfunction', 'function', 'function_source']

    feat_table = pd.DataFrame(columns=cols)

    return feat_table


def add_blackbox_feature(feat_table, feat_name, feat_fn):
    if not isinstance(feat_table, pd.DataFrame):
        logger.error('Input feature table is not of type data frame')
        raise AssertionError('Input feature table is not of type data frame')

    if not isinstance(feat_name, six.string_types):
        logger.error('Input feature name is not of type string')
        raise AssertionError('Input feature name is not of type string')


    dummy_feature_table = create_feature_table()
    if sorted(dummy_feature_table.columns) != sorted(feat_table.columns):
        logger.error('Input feature table does not have the necessary columns')
        raise AssertionError('Input feature table does not have the necessary columns')


    feat_names = list(feat_table['feature_name'])
    if feat_name in feat_names:
        logger.error('Input feature name is already present in feature table')
        raise AssertionError('Input feature name is already present in feature table')

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
