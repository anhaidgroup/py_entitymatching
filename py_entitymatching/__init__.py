
from py_entitymatching.catalog.catalog import Catalog

__version__ = '0.4.0'

_catalog = Catalog.Instance()

# downsampling related methods

from py_entitymatching.sampler.down_sample import down_sample
# # io related methods
#
from py_entitymatching.io.parsers import read_csv_metadata, to_csv_metadata
from py_entitymatching.io.pickles import load_object, load_table, save_object, save_table
#
# import catalog related methods
from py_entitymatching.catalog.catalog_manager import get_property, get_all_properties, \
    set_property, del_property, del_all_properties, init_properties, copy_properties
from py_entitymatching.catalog.catalog_manager import get_catalog, del_catalog, \
    get_catalog_len, show_properties, show_properties_for_id
from py_entitymatching.catalog.catalog_manager import is_property_present_for_df, \
    is_dfinfo_present, is_catalog_empty
from py_entitymatching.catalog.catalog_manager import get_key, set_key, set_fk_ltable,\
    set_fk_rtable, get_ltable, get_rtable, validate_and_set_fk_ltable, \
    validate_and_set_fk_rtable, set_ltable, set_rtable, get_fk_rtable,  \
    get_fk_ltable


# # data exploration wrappers
from py_entitymatching.explorer.openrefine.openrefine_wrapper import data_explore_openrefine
from py_entitymatching.explorer.pandastable.pandastable_wrapper import data_explore_pandastable


#
# # blockers
from py_entitymatching.blocker.attr_equiv_blocker import AttrEquivalenceBlocker
from py_entitymatching.blocker.black_box_blocker import BlackBoxBlocker
from py_entitymatching.blocker.overlap_blocker import OverlapBlocker
from py_entitymatching.blocker.rule_based_blocker import RuleBasedBlocker
from py_entitymatching.blocker.sn_blocker import SortedNeighborhoodBlocker

# # blocker debugger
from py_entitymatching.debugblocker.debugblocker import debug_blocker
from py_entitymatching.debugblocker.backup_debugblocker import backup_debug_blocker

# # blocker combiner
from py_entitymatching.blockercombiner.blockercombiner import combine_blocker_outputs_via_union

# # sampling.rst
from py_entitymatching.sampler.single_table import sample_table

# #
from py_entitymatching.gui.table_gui import view_table, edit_table

# # labeling
from py_entitymatching.labeler.labeler import label_table

# # feature related stuff
from py_entitymatching.feature.simfunctions import *
from py_entitymatching.feature.tokenizers import *
from py_entitymatching.feature.attributeutils import get_attr_corres, get_attr_types
from py_entitymatching.feature.autofeaturegen import get_features, get_features_for_blocking, \
    get_features_for_matching
from py_entitymatching.feature.addfeatures import get_feature_fn, add_feature, \
    add_blackbox_feature, create_feature_table
from py_entitymatching.feature.extractfeatures import extract_feature_vecs

# # matcher related stuff
from py_entitymatching.matcher.matcherutils import split_train_test, impute_table
from py_entitymatching.matcher.dtmatcher import DTMatcher
from py_entitymatching.matcher.linregmatcher import LinRegMatcher
from py_entitymatching.matcher.logregmatcher import LogRegMatcher
from py_entitymatching.matcher.nbmatcher import NBMatcher
from py_entitymatching.matcher.rfmatcher import RFMatcher
from py_entitymatching.matcher.svmmatcher import SVMMatcher
from py_entitymatching.matcher.booleanrulematcher import BooleanRuleMatcher
try:
    from py_entitymatching.matcher.xgboostmatcher import XGBoostMatcher
except ImportError:
    pass

# # matcher selector
from py_entitymatching.matcherselector.mlmatcherselection import select_matcher

# # matcher debugger
from py_entitymatching.debugmatcher.debug_decisiontree_matcher import \
    debug_decisiontree_matcher, visualize_tree
from py_entitymatching.debugmatcher.debug_randomforest_matcher import \
    debug_randomforest_matcher

from py_entitymatching.debugmatcher.debug_gui_decisiontree_matcher import \
    vis_debug_dt, vis_tuple_debug_dt_matcher

from py_entitymatching.debugmatcher.debug_gui_randomforest_matcher import \
    vis_debug_rf, vis_tuple_debug_rf_matcher

# # match trigger
from py_entitymatching.triggers.matchtrigger import MatchTrigger

# # evaluation
from py_entitymatching.evaluation.evaluation import eval_matches, \
    get_false_negatives_as_df, get_false_positives_as_df, print_eval_summary


# # generic helper functions
from py_entitymatching.utils.generic_helper import get_install_path, load_dataset, \
    add_output_attributes

# # pandas helper functions
from py_entitymatching.utils.pandas_helper import filter_rows, project_cols, \
    mutate_col, rename_col, preserve_metadata, drop_cols

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
