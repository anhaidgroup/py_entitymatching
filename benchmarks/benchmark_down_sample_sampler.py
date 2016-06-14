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

PATH = get_install_path()
DATASET_PATH = os.sep.join([PATH, 'datasets', 'example_datasets'])

class TimeDownSampleRestaurants:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'restaurants', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'restaurants', 'B.csv'])
        self.size = 500
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleElectronics:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'electronics', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'electronics', 'B.csv'])
        self.size = 500
        self.y_param = 5
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleAnime:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'anime', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'anime', 'B.csv'])
        self.size = 1000
        self.y_param = 1
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleBooks:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'books', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'books', 'B.csv'])
        self.size = 2000
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleCitations:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citations', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citations', 'B.csv'])
        self.size = 3000
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleBikes:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'bikes', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'bikes', 'B.csv'])
        self.size = 2500
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleCosmetics:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'cosmetics', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'cosmetics', 'B.csv'])
        self.size = 4000
        self.y_param = 1
        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleEbooks:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'ebooks', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'ebooks', 'B.csv'])
        self.size = 3000
        self.y_param = 1
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleMovies:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'movies', 'tableA.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'movies', 'tableB.csv'])
        self.size = 1000
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a)
            self.B = read_csv_metadata(path_for_b)
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleMusic:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'music', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'music', 'B.csv'])
        self.size = 1500
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleBeer:
    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'beer', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'beer', 'B.csv'])
        self.size = 500
        self.y_param = 10
        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleASongs1:
    timeout = 2000.0

    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        self.size = 2000
        self.y_param = 2
        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Please visit the website to download this dataset")

class TimeDownSampleASongs2:
    timeout = 2000.0

    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        self.size = 3000
        self.y_param = 1

        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Songs dataset is not available here. Please visit the website to download this dataset")

class TimeDownSampleASongs3:
    timeout = 2000.0

    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        self.size = 4000
        self.y_param = 1

        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Songs dataset is not available here. Please visit the website to download this dataset")


class TimeDownSampleCitation1:
    timeout = 2000.0

    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citation', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citation', 'B.csv'])
        self.size = 1000
        self.y_param = 1

        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Citation dataset is not available here. Please visit the website to download this dataset")

class TimeDownSampleCitation2:
    timeout = 2000.0

    def time_down_sample_tables(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citation', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citation', 'B.csv'])
        self.size = 2000
        self.y_param = 1

        try:
            self.A = read_csv_metadata(path_for_a, encoding='iso-8859-1')
            self.B = read_csv_metadata(path_for_b, encoding='iso-8859-1')
            down_sample(self.A, self.B, self.size, self.y_param)
            del self.A
            del self.B
            del self.size
            del self.y_param
        except AssertionError:
            print("Citation dataset is not available here. Please visit the website to download this dataset")
