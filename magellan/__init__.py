
from magellan.catalog.catalog import Catalog

__version__ = '0.1.0'

_catalog = Catalog.Instance()

# downsampling related methods

from magellan.sampler.down_sample import down_sample
# # io related methods
#
from magellan.io.parsers import read_csv_metadata, to_csv_metadata
from magellan.io.pickles import load_object, load_table, save_object, save_table
#
# import catalog related methods
from magellan.catalog.catalog_manager import get_property, get_all_properties, \
    set_property, del_property, del_all_properties
from magellan.catalog.catalog_manager import get_catalog, del_catalog, \
    get_catalog_len, show_properties, show_properties_for_id
from magellan.catalog.catalog_manager import is_property_present_for_df, \
    is_dfinfo_present, is_catalog_empty
from magellan.catalog.catalog_manager import get_key, set_key


#
# # blockers
from magellan.blocker.attr_equiv_blocker import AttrEquivalenceBlocker
from magellan.blocker.black_box_blocker import BlackBoxBlocker
from magellan.blocker.overlap_blocker import OverlapBlocker
from magellan.blocker.rule_based_blocker import RuleBasedBlocker

# # blocker debugger
from magellan.debugblocker.debugblocker import debug_blocker

# # blocker combiner
from magellan.blockercombiner.blockercombiner import combine_blocker_outputs_via_union

# # sampling.rst
from magellan.sampler.single_table import sample_table


# # labeling
from magellan.labeler.labeler import label_table

# # feature related stuff
from magellan.feature.simfunctions import *
from magellan.feature.tokenizers import *
from magellan.feature.attributeutils import get_attr_corres, get_attr_types
from magellan.feature.autofeaturegen import get_features, get_features_for_blocking, \
    get_features_for_matching
from magellan.feature.addfeatures import get_feature_fn, add_feature, \
    add_blackbox_feature, create_feature_table
from magellan.feature.extractfeatures import extract_feature_vecs

# # matcher related stuff
from magellan.matcher.matcherutils import train_test_split, impute_table
from magellan.matcher.dtmatcher import DTMatcher
from magellan.matcher.linregmatcher import LinRegMatcher
from magellan.matcher.logregmatcher import LogRegMatcher
from magellan.matcher.nbmatcher import NBMatcher
from magellan.matcher.rfmatcher import RFMatcher
from magellan.matcher.svmmatcher import SVMMatcher

# # matcher selector
from magellan.matcherselector.mlmatcherselection import select_matcher

# # matcher debugger
from magellan.debugmatcher.debug_decisiontree_matcher import \
    debug_decisiontree_matcher, visualize_tree
from magellan.debugmatcher.debug_randomforest_matcher import \
    debug_randomforest_matcher
from magellan.debugmatcher.debug_gui_decisiontree_matcher import vis_debug_dt
from magellan.debugmatcher.debug_gui_randomforest_matcher import vis_debug_rf

# # evaluation
from magellan.evaluation.evaluation import eval_matches, \
    get_false_negatives_as_df, get_false_positives_as_df, print_eval_summary


# # helper functions
from magellan.utils.generic_helper import get_install_path, load_dataset, \
    add_output_attributes

# global vars
_block_t = None
_block_s = None
_atypes1 = None
_atypes2 = None
_block_c = None

_match_t = None
_match_s = None
_match_c = None

# GUI related
_viewapp = None
