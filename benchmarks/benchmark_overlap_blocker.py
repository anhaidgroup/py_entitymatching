# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os
import sys

import py_entitymatching  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])
ob = mg.OverlapBlocker()

class TimeBlockTablesBooks:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'books\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_tables_title_2(self):
        ob.block_tables(self.A, self.B, 'Title', 'Title', overlap_size=2,
                        l_output_attrs=['Title', 'Author', 'ISBN13',
                                        'Publisher','Publication_Date'],
                        r_output_attrs=['Title', 'Author', 'ISBN13',
                                        'Publisher','Publication_Date'])

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesBeer:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'Label')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'Label')
        except AssertionError:
            print("Dataset \'beer\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_tables_beer_name_3(self):
        ob.block_tables(self.A, self.B, 'Beer_Name', 'Beer_Name',
                        overlap_size=3, l_output_attrs=['Beer_Name'],
                        r_output_attrs=['Beer_Name'])

    def time_block_tables_brew_factory_name_2(self):
        ob.block_tables(self.A, self.B, 'Brew_Factory_Name',
                        'Brew_Factory_Name', overlap_size=2,
                        l_output_attrs=['Beer_Name', 'ABV'],
                        r_output_attrs=['Beer_Name', 'ABV'])

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesCitations:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'ID')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'ID')
        except AssertionError:
            print("Dataset \'anime\' not found. Please visit the project"
                  " website to download the dataset.")
            raise SystemExit

    def time_block_tables_title_1(self):
        ob.block_tables(self.A, self.B, 'author', 'author', rem_stop_words=True,
                        l_output_attrs=['title','author','year','ENTRYTYPE'],
                        r_output_attrs=['title','author','year','ENTRYTYPE'])

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesEbooks:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'record_id')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'record_id')
        except AssertionError:
            print("Dataset \'ebooks\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_tables_author_2(self):
        ob.block_tables(self.A, self.B, 'author', 'author', overlap_size=2,
                        l_output_attrs = ['title', 'author', 'length', 'price'],
                        r_output_attrs = ['title', 'author', 'length', 'price'])

    def time_block_tables_title_2(self):
        ob.block_tables(self.A, self.B, 'title', 'title', overlap_size=2,
                        rem_stop_words=True,
                        l_output_attrs=['title', 'author', 'publisher', 'date'],
                        r_output_attrs=['title', 'author', 'publisher', 'date'])

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesMusic:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        self.l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight',
                               'Released', 'Song_Name', 'Time']
        self.r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright',
                               'Released', 'Song_Name', 'Time']
        try:
            self.A = mg.read_csv_metadata(path_for_A)
            mg.set_key(self.A, 'Sno')
            self.B = mg.read_csv_metadata(path_for_B)
            mg.set_key(self.B, 'Sno')
        except AssertionError:
            print("Dataset \'music\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_tables_album_name_1(self):
        ob.block_tables(self.A, self.B, 'Album_Name', 'Album_Name',
                        rem_stop_words=True,
                        l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockTablesRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
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

    def time_block_tables_address_4(self):
        ob.block_tables(self.A, self.B, 'ADDRESS', 'ADDRESS',
                        overlap_size=4,
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)

    def time_block_tables_name_1(self):
        ob.block_tables(self.A, self.B, 'NAME', 'NAME',
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockCandsetEbooks:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'record_id')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'record_id')
            self.C = ob.block_tables(A, B, 'title', 'title', overlap_size=2,
                                     rem_stop_words=True,
                                     l_output_attrs=['title', 'author', 'publisher', 'date'],
                                     r_output_attrs=['title', 'author', 'publisher', 'date'])
        except AssertionError:
            print("Dataset \'ebooks\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_candset_publisher_1(self):
        ob.block_candset(self.C, 'publisher', 'publisher', rem_stop_words=True)

    def teardown(self):
        del self.C

class TimeBlockCandsetMusic:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'Sno')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'Sno')
            l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight',
                              'Released', 'Song_Name', 'Time']
            r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright',
                              'Released', 'Song_Name', 'Time']
            self.C = ob.block_tables(A, B, 'Album_Name', 'Album_Name',
				     rem_stop_words=True,
                                     l_output_attrs=l_output_attrs,
                                     r_output_attrs=r_output_attrs)
        except AssertionError:
            print("Dataset \'music\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_candset(self):
        ob.block_candset(self.C, 'Artist_Name', 'Artist_Name',
                         rem_stop_words=True)

    def teardown(self):
        del self.C

class TimeBlockCandsetMusic2:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        try:
            A = mg.read_csv_metadata(path_for_A)
            mg.set_key(A, 'Sno')
            B = mg.read_csv_metadata(path_for_B)
            mg.set_key(B, 'Sno')
            l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight',
                              'Released', 'Song_Name', 'Time']
            r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright',
                              'Released', 'Song_Name', 'Time']
            C = ob.block_tables(A, B, 'Album_Name', 'Album_Name',
                                rem_stop_words=True,
                                l_output_attrs=l_output_attrs,
                                r_output_attrs=r_output_attrs)
            self.D = ob.block_candset(C, 'Artist_Name', 'Artist_Name',
                                      rem_stop_words=True)
        except AssertionError:
            print("Dataset \'music\' not found. Please visit the project "
                  "website to download the dataset.")
            raise SystemExit

    def time_block_candset(self):
        ob.block_candset(self.D, 'Song_Name', 'Song_Name', rem_stop_words=True)

    def teardown(self):
        del self.D
