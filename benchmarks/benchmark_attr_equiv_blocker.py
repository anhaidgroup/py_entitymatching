# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os

import magellan  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])
ab = mg.AttrEquivalenceBlocker()


class TimeBlockTablesRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        self.l_block_attr = 'PHONENUMBER'
        self.r_block_attr = 'PHONENUMBER'
        self.l_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.r_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables(self):
        ab.block_tables(self.A, self.B, self.l_block_attr,
                        self.r_block_attr, self.l_output_attrs,
                        self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_block_attr
        del self.r_block_attr
        del self.l_output_attrs
        del self.r_output_attrs


class TimeBlockTablesElectronics:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'electronics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'electronics', 'B.csv'])
        self.l_block_attr = 'Brand'
        self.r_block_attr = 'Brand'
        self.l_output_attrs = ['Brand', 'Amazon_Price']
        self.r_output_attrs = ['Brand', 'Price']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables(self):
        C = ab.block_tables(self.A, self.B, self.l_block_attr,
                            self.r_block_attr, self.l_output_attrs,
                            self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_block_attr
        del self.r_block_attr
        del self.l_output_attrs
        del self.r_output_attrs


class TimeBlockTablesAnime:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'anime', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'anime', 'B.csv'])
        self.l_block_attr = 'Year'
        self.r_block_attr = 'Year'
        self.l_output_attrs = ['Title', 'Year', 'Episodes']
        self.r_output_attrs = ['Title', 'Year', 'Episodes']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables(self):
        ab.block_tables(self.A, self.B, self.l_block_attr,
                        self.r_block_attr, self.l_output_attrs,
                        self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_block_attr
        del self.r_block_attr
        del self.l_output_attrs
        del self.r_output_attrs


class TimeBlockCandsetAnime:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'anime', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'anime', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'ID')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'ID')
        self.C = ab.block_tables(A, B, 'Year', 'Year',
                                 ['Title', 'Year', 'Episodes'],
                                 ['Title', 'Year', 'Episodes'])
        self.l_block_attr = 'Episodes'
        self.r_block_attr = 'Episodes'

    def time_block_candset(self):
        ab.block_candset(self.C, self.l_block_attr, self.r_block_attr)

    def teardown(self):
        del self.C
        del self.l_block_attr
        del self.r_block_attr


class TimeBlockTablesBooks:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        self.l_block_attr = 'Author'
        self.r_block_attr = 'Author'
        self.l_output_attrs = ['Title', 'Author', 'ISBN13', 'Publisher',
                               'Publication_Date']
        self.r_output_attrs = ['Title', 'Author', 'ISBN13', 'Publisher',
                               'Publication_Date']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables(self):
        ab.block_tables(self.A, self.B, self.l_block_attr,
                        self.r_block_attr, self.l_output_attrs,
                        self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_block_attr
        del self.r_block_attr
        del self.l_output_attrs
        del self.r_output_attrs


class TimeBlockCandsetBooks:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'ID')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'ID')
        self.C = ab.block_tables(A, B, 'Author', 'Author',
                                 ['Title', 'Author', 'ISBN13', 'Publisher'],
                                 ['Title', 'Author', 'ISBN13', 'Publisher'])
        self.l_block_attr = 'ISBN13'
        self.r_block_attr = 'ISBN13'

    def time_block_candset(self):
        ab.block_candset(self.C, self.l_block_attr, self.r_block_attr)

    def teardown(self):
        del self.C
        del self.l_block_attr
        del self.r_block_attr


class TimeBlockTablesCitations:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        self.l_block_attr = 'year'
        self.r_block_attr = 'year'
        self.l_output_attrs = ['title', 'author', 'year', 'ENTRYTYPE']
        self.r_output_attrs = ['title', 'author', 'year', 'ENTRYTYPE']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables(self):
        ab.block_tables(self.A, self.B, self.l_block_attr,
                        self.r_block_attr, self.l_output_attrs,
                        self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_block_attr
        del self.r_block_attr
        del self.l_output_attrs
        del self.r_output_attrs


class TimeBlockTablesBikes:
    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([p, 'datasets', 'example_datasets', 'bikes', 'A.csv'])
        path_for_B = os.sep.join([p, 'datasets', 'example_datasets', 'bikes', 'B.csv'])
        l_key = 'id'
        r_key = 'id'
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, l_key)
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, r_key)
        self.l_block_attr = 'city_posted'
        self.r_block_attr = 'city_posted'
        self.l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']
        self.r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']
        self.ab = mg.AttrEquivalenceBlocker()

    def time_block_tables(self):
        self.ab.block_tables(self.A, self.B, self.l_block_attr,
                             self.r_block_attr, self.l_output_attrs,
                             self.r_output_attrs, verbose=False)

    def teardown(self):
        del self.A
        del self.B
        del self.ab


class TimeBlockCandsetBikes:
    timeout = 300.0

    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([p, 'datasets', 'example_datasets', 'bikes', 'A.csv'])
        path_for_B = os.sep.join([p, 'datasets', 'example_datasets', 'bikes', 'B.csv'])
        l_key = 'id'
        r_key = 'id'
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, l_key)
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, r_key)
        l_block_attr_1 = 'city_posted'
        r_block_attr_1 = 'city_posted'
        l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                          'color', 'model_year']
        r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                          'color', 'model_year']
        self.ab = mg.AttrEquivalenceBlocker()
        self.C = self.ab.block_tables(self.A, self.B, l_block_attr_1,
                                      r_block_attr_1, l_output_attrs,
                                      r_output_attrs, verbose=False)
        self.l_block_attr = 'model_year'
        self.r_block_attr = 'model_year'

    def time_block_candset(self):
        self.ab.block_candset(self.C, self.l_block_attr, self.r_block_attr,
                              verbose=False, show_progress=False)

    def teardown(self):
        del self.A
        del self.B
        del self.C
        del self.ab
