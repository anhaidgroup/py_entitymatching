class Blocker(object):
    pass

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

