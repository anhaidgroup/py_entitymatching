import logging

import math
import pandas as pd
import six
import multiprocessing

logger = logging.getLogger(__name__)

class Blocker(object):
    pass

    def validate_types_params_tables(self, ltable, rtable,
		       l_output_attrs, r_output_attrs, l_output_prefix,
		       r_output_prefix, verbose, n_jobs):

        if not isinstance(ltable, pd.DataFrame):
            logger.error('Input left table is not of type pandas data frame')
            raise AssertionError('Input left table is not of type pandas data frame')

        if not isinstance(rtable, pd.DataFrame):
            logger.error('Input right table is not of type pandas data frame')
            raise AssertionError('Input right table is not of type pandas data frame')

        if l_output_attrs:
            if not isinstance(l_output_attrs, list):
                logger.error('Output attributes of left table is not of type list')
                raise AssertionError('Output attributes of left table is not of type list')
            for x in l_output_attrs:
                if not isinstance(x, six.string_types):
                    logger.error('An output attribute name of left table is not of type string')
                    raise AssertionError('An output attribute name of left table is not of type string')

        if r_output_attrs:
            if not isinstance(r_output_attrs, list):
                logger.error('Output attributes of right table is not of type list')
                raise AssertionError('Output attributes of right table is not of type list')
            for x in r_output_attrs:
                if not isinstance(x, six.string_types):
                    logger.error('An output attribute name of right table is not of type string')
                    raise AssertionError('An output attribute name of right table is not of type string')

        if not isinstance(l_output_prefix, six.string_types):
            logger.error('Output prefix of left table is not of type string')
            raise AssertionError('Output prefix of left table is not of type string')

        if not isinstance(r_output_prefix, six.string_types):
            logger.error('Output prefix of right table is not of type string')
            raise AssertionError('Output prefix of right table is not of type string')
        
        if not isinstance(verbose, bool):
            logger.error('Parameter verbose is not of type bool')
            raise AssertionError('Parameter verbose is not of type bool')

        if not isinstance(n_jobs, int):
            logger.error('Parameter n_jobs is not of type int')
            raise AssertionError('Parameter n_jobs is not of type int')

    def validate_types_params_candset(self, candset, verbose, show_progress, n_jobs):
        if not isinstance(candset, pd.DataFrame):
            logger.error('Input candset is not of type pandas data frame')
            raise AssertionError('Input candset is not of type pandas data frame')

        if not isinstance(verbose, bool):
            logger.error('Parameter verbose is not of type bool')
            raise AssertionError('Parameter verbose is not of type bool')

        if not isinstance(show_progress, bool):
            logger.error('Parameter show_progress is not of type bool')
            raise AssertionError('Parameter show_progress is not of type bool')

        if not isinstance(n_jobs, int):
            logger.error('Parameter n_jobs is not of type int')
            raise AssertionError('Parameter n_jobs is not of type int')

    def process_output_attrs(self, table, key, attrs, error_str=''):
        if attrs:
            if not isinstance(attrs, list):
                attrs = [attrs]
            assert set(attrs).issubset(table.columns) == True, 'Output are not in ' + error_str + ' table'
            attrs = [x for x in attrs if x not in [key]]
        return attrs

    def validate_output_attrs(self, ltable, rtable, l_output_attrs, r_output_attrs):
        if l_output_attrs:
            if not isinstance(l_output_attrs, list):
                l_output_attrs = [l_output_attrs]
            assert set(l_output_attrs).issubset(ltable.columns) == True, 'Left output attributes are not in the left ' \
                                                                         'table'

        if r_output_attrs:
            if not isinstance(r_output_attrs, list):
                r_output_attrs = [r_output_attrs]
            assert set(r_output_attrs).issubset(rtable.columns) == True, 'Right output attributes are not in the right'\
                                                                         ' table'

    def get_attrs_to_retain(self, l_key, r_key, l_output_attrs, r_output_attrs, l_output_prefix, r_output_prefix):

        ret_cols = [l_output_prefix+l_key, r_output_prefix+r_key]
        if l_output_attrs:
            ret_cols.extend(l_output_prefix+c for c in l_output_attrs)
        if r_output_attrs:
            ret_cols.extend(r_output_prefix+c for c in r_output_attrs)

        return ret_cols

    def get_attrs_to_project(self, key, block_attr, output_attrs):
        if not output_attrs:
            output_attrs = []
        if key not in output_attrs:                                             
            output_attrs.append(key)                                            
        if block_attr not in output_attrs:                                      
            output_attrs.append(block_attr)                                     
        return output_attrs

    def get_split_params(self, n_procs):
        m = int(math.sqrt(n_procs))
        while n_procs % m != 0:
            m = m - 1
        n = n_procs / m
        return m, n
    
    def get_num_procs(self, n_jobs):        
        # determine number of processes to launch parallely
        n_cpus = multiprocessing.cpu_count()
        n_procs = n_jobs
        if n_jobs < 0:
            n_procs = n_cpus + 1 + n_jobs

