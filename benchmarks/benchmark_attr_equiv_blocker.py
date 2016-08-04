# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os
import sys

import py_entitymatching  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])
ab = mg.AttrEquivalenceBlocker()


class TimeBlockTablesAnime:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'anime', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'anime', 'B.csv'])
        self.l_block_attr = 'Year'
        self.r_block_attr = 'Year'
        self.l_output_attrs = ['Title', 'Year', 'Episodes']
        self.r_output_attrs = ['Title', 'Year', 'Episodes']
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'anime\' not found. Please visit the project"
                  " website to download the dataset.")
            raise SystemExit

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
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'id')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'id')
        except AssertionError:
            print("Dataset \'bikes\' not found. Please visit the project"
                  " website to download the dataset.")        
            raise SystemExit

        self.l_block_attr = 'city_posted'
        self.r_block_attr = 'city_posted'
        self.l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']
        self.r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']

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
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'books\' not found. Please visit the project"
                  " website to download the dataset.")        
            raise SystemExit

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


class TimeBlockTablesCitations:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        self.l_block_attr = 'year'
        self.r_block_attr = 'year'
        self.l_output_attrs = ['title', 'author', 'year', 'ENTRYTYPE']
        self.r_output_attrs = ['title', 'author', 'year', 'ENTRYTYPE']
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'citations\' not found. Please visit the project"
                  " website to download the dataset.")        
            raise SystemExit
      
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
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'electronics\' not found. Please visit the project"
                  " website to download the dataset.")
            raise SystemExit
        

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


class TimeBlockTablesRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        self.l_block_attr = 'PHONENUMBER'
        self.r_block_attr = 'PHONENUMBER'
        self.l_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.r_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'restaurants\' not found. Please visit the project"
                  " website to download the dataset.")
            raise SystemExit

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
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'ID')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'ID')
            self.C = ab.block_tables(A, B, 'Year', 'Year',
                                     ['Title', 'Year', 'Episodes'],
                                     ['Title', 'Year', 'Episodes'])
        except AssertionError:
            print("Dataset \'anime\' not found. Please visit the project"
                  " website to download the dataset.")
            raise SystemExit

        self.l_block_attr = 'Episodes'
        self.r_block_attr = 'Episodes'

    def time_block_candset(self):
        ab.block_candset(self.C, self.l_block_attr, self.r_block_attr)

    def teardown(self):
        del self.C
        del self.l_block_attr
        del self.r_block_attr


class TimeBlockCandsetBikes:
    timeout = 300.0

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'id')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'id')
            l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                              'color', 'model_year']
            r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                              'color', 'model_year']
            self.C = ab.block_tables(A, B, 'city_posted', 'city_posted',
                                     l_output_attrs, r_output_attrs)
        except AssertionError:
            print("Dataset \'bikes\' not found. Please visit the project"
                  " website to download the dataset.")        
            raise SystemExit
        
        self.l_block_attr = 'model_year'
        self.r_block_attr = 'model_year'

    def time_block_candset(self):
        ab.block_candset(self.C, self.l_block_attr, self.r_block_attr)

    def teardown(self):
        del self.C
        del self.l_block_attr
        del self.r_block_attr


class TimeBlockCandsetBooks:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'ID')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'ID')
            self.C = ab.block_tables(A, B, 'Author', 'Author',
                                     ['Title', 'Author', 'ISBN13', 'Publisher'],
                                     ['Title', 'Author', 'ISBN13', 'Publisher'])
        except AssertionError:
            print("Dataset \'books\' not found. Please visit the project"
                  " website to download the dataset.")        
            raise SystemExit

        self.l_block_attr = 'ISBN13'
        self.r_block_attr = 'ISBN13'

    def time_block_candset(self):
        ab.block_candset(self.C, self.l_block_attr, self.r_block_attr)

    def teardown(self):
        del self.C
        del self.l_block_attr
        del self.r_block_attr
