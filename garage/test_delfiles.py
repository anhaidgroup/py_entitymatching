import os
import magellan as mg

from magellan.utils.generic_helper import del_files_in_dir
p = os.sep.join([mg.get_install_path(), 'datasets','test_datasets', 'sandbox'])

del_files_in_dir(p)