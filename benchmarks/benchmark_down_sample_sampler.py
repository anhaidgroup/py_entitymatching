# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os

import magellan  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])

class TimeDownSampleRestaurants:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

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
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

class TimeDownSampleBeer:
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        self.size = 200
        self.y = 1000
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')

    def time_down_sample_tables(self):
        mg.down_sample(self.A, self.B, self.size, self.y)

    def teardown(self):
        del self.A
        del self.B
        del self.size
        del self.y

