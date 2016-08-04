# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os

import py_entitymatching as em
from py_entitymatching.utils.generic_helper import get_install_path

import sys

if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")

PATH = get_install_path()
DATASET_PATH = os.sep.join([PATH, 'datasets', 'example_datasets'])


class TimeDownSampleRestaurants:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'restaurants', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'restaurants', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 500
            self.y_param = 2
        except AssertionError:
            print("Dataset \'restaurants\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleElectronics:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'electronics', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'electronics', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 500
            self.y_param = 5
        except AssertionError:
            print("Dataset \'electronics\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleAnime:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'anime', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'anime', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 1000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'anime\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleBooks:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'books', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'books', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 2000
            self.y_param = 2
        except AssertionError:
            print("Dataset \'books\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleCitations:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citations', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citations', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 3000
            self.y_param = 2
        except AssertionError:
            print("Dataset \'citations\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleBikes:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'bikes', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'bikes', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 2500
            self.y_param = 2
        except AssertionError:
            print("Dataset \'bikes\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleCosmetics:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'cosmetics', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'cosmetics', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 4000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'cosmetics\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleEbooks:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'ebooks', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'ebooks', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 3000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'ebooks\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleMovies:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'movies', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'movies', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 1000
            self.y_param = 2
        except AssertionError:
            print("Dataset \'movies\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleMusic:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'music', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'music', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 1500
            self.y_param = 2
        except AssertionError:
            print("Dataset \'music\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleBeer:
    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'beer', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'beer', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 500
            self.y_param = 10
        except AssertionError:
            print("Dataset \'beer\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleASongs1:
    timeout = 2000.0

    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 2000
            self.y_param = 2
        except AssertionError:
            print("Dataset \'songs\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleASongs2:
    timeout = 2000.0

    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 3000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'songs\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleASongs3:
    timeout = 2000.0

    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'songs', 'A.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 4000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'songs\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleCitation1:
    timeout = 2000.0

    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citation', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citation', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 1000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'citation\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param


class TimeDownSampleCitation2:
    timeout = 2000.0

    def setup(self):
        path_for_a = os.sep.join([DATASET_PATH, 'citation', 'A.csv'])
        path_for_b = os.sep.join([DATASET_PATH, 'citation', 'B.csv'])
        try:
            self.A = em.read_csv_metadata(path_for_a)
            self.B = em.read_csv_metadata(path_for_b)
            self.size = 2000
            self.y_param = 1
        except AssertionError:
            print("Dataset \'citation\' not found. Please visit the project website to download the dataset.")
            raise SystemExit

    def time_down_sample_tables(self):
        em.down_sample(self.A, self.B, self.size, self.y_param)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y_param