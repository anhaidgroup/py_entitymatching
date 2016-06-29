
__version__ = '0.1.0'

# import join methods
from magellan.externals.py_stringsimjoin.join.cosine_join import cosine_join
from magellan.externals.py_stringsimjoin.join.dice_join import dice_join
from magellan.externals.py_stringsimjoin.join.edit_distance_join import edit_distance_join
from magellan.externals.py_stringsimjoin.join.jaccard_join import jaccard_join
from magellan.externals.py_stringsimjoin.join.overlap_join import overlap_join
from magellan.externals.py_stringsimjoin.join.overlap_coefficient_join import overlap_coefficient_join

# import filters
from magellan.externals.py_stringsimjoin.filter.overlap_filter import OverlapFilter
from magellan.externals.py_stringsimjoin.filter.position_filter import PositionFilter
from magellan.externals.py_stringsimjoin.filter.prefix_filter import PrefixFilter
from magellan.externals.py_stringsimjoin.filter.size_filter import SizeFilter
from magellan.externals.py_stringsimjoin.filter.suffix_filter import SuffixFilter

# import matcher methods
from magellan.externals.py_stringsimjoin.matcher.apply_matcher import apply_matcher

# import profiling methods
from magellan.externals.py_stringsimjoin.profiler.profiler import profile_table_for_join

