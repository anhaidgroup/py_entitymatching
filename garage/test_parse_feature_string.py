import magellan as mg
from magellan.feature.addfeatures import parse_feat_str

feature_string = 'jaccard(qgm_3(ltuple["zipcode"]), qgm_3(rtuple["zipcode"]))'
tok = mg.get_tokenizers_for_matching()
sim = mg.get_sim_funs_for_matching()

d = parse_feat_str(feature_string, tok, sim)
print(d)