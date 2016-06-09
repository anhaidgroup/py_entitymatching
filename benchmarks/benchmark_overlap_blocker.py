# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os
import sys

import magellan  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])
ob = mg.OverlapBlocker()


class TimeBlockTablesRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        #self.l_overlap_attr = 'ADDRESS'
        #self.r_overlap_attr = 'ADDRESS'
        self.l_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.r_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables_address_4(self):
        C = ob.block_tables(self.A, self.B, 'ADDRESS',
                        'ADDRESS', overlap_size=4,
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_address_4_njobs_2(self):
        C = ob.block_tables(self.A, self.B, 'ADDRESS', 'ADDRESS',
                            overlap_size=4,
			    l_output_attrs=self.l_output_attrs,
                            r_output_attrs=self.r_output_attrs, n_jobs=2)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_address_4_njobs_4(self):
        C = ob.block_tables(self.A, self.B, 'ADDRESS', 'ADDRESS',
                            overlap_size=4,
			    l_output_attrs=self.l_output_attrs,
                            r_output_attrs=self.r_output_attrs, n_jobs=4)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_address_4_njobs_all(self):
        C = ob.block_tables(self.A, self.B, 'ADDRESS', 'ADDRESS',
                            overlap_size=4,
			    l_output_attrs=self.l_output_attrs,
                            r_output_attrs=self.r_output_attrs, n_jobs=-1)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_name_1(self):
        C = ob.block_tables(self.A, self.B, 'NAME',
                        'NAME',
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_name_1_njobs_2(self):
        C = ob.block_tables(self.A, self.B, 'NAME', 'NAME',
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs, n_jobs=2)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_name_1_njobs_4(self):
        C = ob.block_tables(self.A, self.B, 'NAME', 'NAME',
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs, n_jobs=4)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_name_1_njobs_all(self):
        C = ob.block_tables(self.A, self.B, 'NAME', 'NAME',
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs, n_jobs=-1)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        #del self.l_overlap_attr
        #del self.r_overlap_attr
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockTablesMusic:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        #self.l_overlap_attr = 'ADDRESS'
        #self.r_overlap_attr = 'ADDRESS'
        self.l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight', 'Released', 'Song_Name', 'Time']
        self.r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright', 'Released', 'Song_Name', 'Time']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'Sno')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'Sno')

    def time_block_tables_album_name_1(self):
        C = ob.block_tables(self.A, self.B, 'Album_Name',
                        'Album_Name', rem_stop_words=True,
			l_output_attrs=self.l_output_attrs,
                        r_output_attrs=self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        #del self.l_overlap_attr
        #del self.r_overlap_attr
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockCandsetMusic:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'Sno')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'Sno')
        l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight', 'Released', 'Song_Name', 'Time']
        r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright', 'Released', 'Song_Name', 'Time']
        self.C = ob.block_tables(A, B, 'Album_Name', 'Album_Name',
				 rem_stop_words=True, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs)

    def time_block_candset(self):
        D = ob.block_candset(self.C, 'Artist_Name', 'Artist_Name', rem_stop_words=True)
        print >> sys.stderr, 'size of D:', len(D)

    def teardown(self):
        del self.C

class TimeBlockCandsetMusic2:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'Sno')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'Sno')
        l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight', 'Released', 'Song_Name', 'Time']
        r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright', 'Released', 'Song_Name', 'Time']
        C = ob.block_tables(A, B, 'Album_Name', 'Album_Name',
				 rem_stop_words=True, l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs)
        self.D = ob.block_candset(C, 'Artist_Name', 'Artist_Name', rem_stop_words=True)

    def time_block_candset(self):
        E = ob.block_candset(self.D, 'Song_Name', 'Song_Name', rem_stop_words=True)
        print >> sys.stderr, 'size of E:', len(E)

    def teardown(self):
        del self.D

class TimeBlockTablesEbooks:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'record_id')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'record_id')

    def time_block_tables_author_2(self):
        C = ob.block_tables(self.A, self.B, 'author', 'author', overlap_size=2,
                            l_output_attrs = ['title', 'author', 'length', 'price'],
                            r_output_attrs = ['title', 'author', 'length', 'price'])
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_title_2(self):
        C = ob.block_tables(self.A, self.B, 'title', 'title', overlap_size=2, rem_stop_words=True,
                            l_output_attrs=['title', 'author', 'publisher', 'date'],
                            r_output_attrs=['title', 'author', 'publisher', 'date'])
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B

class TimeBlockCandsetEbooks:
    timeout=3600.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'record_id')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'record_id')
        self.C = ob.block_tables(A, B, 'title', 'title', overlap_size=2, rem_stop_words=True,
                                 l_output_attrs=['title', 'author', 'publisher', 'date'],
                                 r_output_attrs=['title', 'author', 'publisher', 'date'])

    def time_block_candset_publisher_1(self):
        D = ob.block_candset(self.C, 'publisher', 'publisher', rem_stop_words=True)
        print >> sys.stderr, 'size of D:', len(D)

    def teardown(self):
        del self.C

class TimeBlockTablesBeer:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'Label')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'Label')

    def time_block_tables_beer_name_3(self):
        C = ob.block_tables(self.A, self.B, 'Beer_Name', 'Beer_Name', overlap_size=3,
                            l_output_attrs=['Beer_Name'], r_output_attrs=['Beer_Name'])
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_brew_factory_name_2(self):
        C = ob.block_tables(self.A, self.B, 'Brew_Factory_Name', 'Brew_Factory_Name', overlap_size=2,
                            l_output_attrs=['Beer_Name', 'ABV'], r_output_attrs=['Beer_Name', 'ABV'])
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesBooks:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables_title_2(self):
        C = ob.block_tables(self.A, self.B, 'Title', 'Title', overlap_size=2,
                            l_output_attrs=['Title','Author','ISBN13','Publisher','Publication_Date'],
                            r_output_attrs=['Title','Author','ISBN13','Publisher','Publication_Date'])
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B

class TimeBlockTablesCitations:
    timeout=500.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_block_tables_title_1(self):
        C = ob.block_tables(self.A, self.B, 'author', 'author', rem_stop_words=True,
                            l_output_attrs=['title','author','year','ENTRYTYPE'],
                            r_output_attrs=['title','author','year','ENTRYTYPE'])
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B


"""
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
"""
