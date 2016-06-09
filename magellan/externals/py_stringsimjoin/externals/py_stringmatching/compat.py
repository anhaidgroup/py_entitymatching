"""py-stringmatching.compat.py
The compat module defines some variables to enable Python 2 and Python 3
compatibility within a single codebase
The following are defined:
    - _range   -- use in place of xrange/range
    - _unicode -- use in place of unicode/str
    - _unichr  -- use in place of unichr/chr
    - _long    -- use in place of long/int
And:
    - numeric_type -- defines the set of numeric types
"""

import sys

# pylint: disable=invalid-name
if sys.version_info[0] == 3:  # pragma: no cover
    _range = range
    _unicode = str
    _unichr = chr
    _long = int
    numeric_type = (int, float, complex)
else:  # pragma: no cover
    _range = xrange
    _unicode = unicode
    _unichr = unichr
    _long = long
    numeric_type = (int, long, float, complex)
