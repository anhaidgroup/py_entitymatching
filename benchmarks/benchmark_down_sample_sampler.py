# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os

from magellan.sampler.down_sample import down_sample
from magellan.utils.generic_helper import get_install_path
from magellan.io.parsers import read_csv_metadata

import sys
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")

p = get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])


class TimeDownSampleRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleElectronics:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'electronics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'electronics', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleAnime:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'anime', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'anime', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleBooks:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleCitations:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleBikes:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleCosmetics:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'cosmetics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'cosmetics', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
        self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleEbooks:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleMovies:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'movies', 'tableA.csv'])
        path_for_B = os.sep.join([datasets_path, 'movies', 'tableB.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A)
        self.B = read_csv_metadata(path_for_B)

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleMusic:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
        self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleBeer:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        self.size = 100
        self.y = 500
        self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
        self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')

    def time_down_sample_tables(self):
        down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

# Below two datasets might not be available in the package and have to be downloaded from the website.

class TimeDownSampleASongs1:
    timeout = 600.0

    def time_down_sample_tables(self):
        path_for_A = os.sep.join([datasets_path, 'songs', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'songs', 'A.csv'])
        self.size = 200
        self.y = 1000
        try:
            self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y)
            del self.A
            del self.B
            del self.size
            del self.y
        except AssertionError:
            print("Songs dataset is not available here. Please visit the website to download this dataset")

class TimeDownSampleASongs2:
    timeout = 600.0

    def time_down_sample_tables(self):
        path_for_A = os.sep.join([datasets_path, 'songs', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'songs', 'A.csv'])
        self.size = 400
        self.y = 2000

        try:
            self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y)
            del self.A
            del self.B
            del self.size
            del self.y
        except AssertionError:
            print("Songs dataset is not available here. Please visit the website to download this dataset")

class TimeDownSampleCitation1:
    timeout = 1000.0

    def time_down_sample_tables(self):
        path_for_A = os.sep.join([datasets_path, 'citation', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citation', 'B.csv'])
        self.size = 200
        self.y = 1000

        try:
            self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y)
            del self.A
            del self.B
            del self.size
            del self.y
        except AssertionError:
            print("Citation dataset is not available here. Please visit the website to download this dataset")

class TimeDownSampleCitation2:
    timeout = 1000.0

    def time_down_sample_tables(self):
        path_for_A = os.sep.join([datasets_path, 'citation', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citation', 'B.csv'])
        self.size = 100
        self.y = 1000

        try:
            self.A = read_csv_metadata(path_for_A, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_B, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y)
            del self.A
            del self.B
            del self.size
            del self.y
        except AssertionError:
            print("Citation dataset is not available here. Please visit the website to download this dataset")
