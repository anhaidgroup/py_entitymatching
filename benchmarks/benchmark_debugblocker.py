import os
import py_entitymatching as em
import py_entitymatching.catalog.catalog_manager as cm
import py_entitymatching.debugblocker.debugblocker as db

p = em.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets', 'debugblocker'])
ab = em.AttrEquivalenceBlocker()


class TimeDebugBlockingAnime:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'anime', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'anime', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'anime', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'ID')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'ID')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_ID', 'rtable_ID', self.A, self.B)
        except AssertionError:
            print("Dataset \'anime\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingBeer:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'beer', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A, encoding='iso-8859-1')
            em.set_key(self.A, 'Label')
            self.B = em.read_csv_metadata(path_for_B, encoding='iso-8859-1')
            em.set_key(self.B, 'Label')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_Label', 'rtable_Label', self.A, self.B)
        except AssertionError:
            print("Dataset \'beer\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingBikes:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'bikes', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'id')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'id')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_id', 'rtable_id', self.A, self.B)
        except AssertionError:
            print("Dataset \'bikes\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingBooks:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'books', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'books', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'books', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'ID')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'ID')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_ID', 'rtable_ID', self.A, self.B)
        except AssertionError:
            print("Dataset \'books\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingCitations:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'citations', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'citations', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'citations', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'ID')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'ID')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_ID', 'rtable_ID', self.A, self.B)
        except AssertionError:
            print("Dataset \'citations\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingCosmetics:

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'cosmetics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'cosmetics', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'cosmetics', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A, encoding='iso-8859-1')
            em.set_key(self.A, 'Product_id')
            self.B = em.read_csv_metadata(path_for_B, encoding='iso-8859-1')
            em.set_key(self.B, 'ID')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_Product_id', 'rtable_ID', self.A, self.B)
        except AssertionError:
            print("Dataset \'cosmetics\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingEbooks:
    timeout=900.0

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'ebooks', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'ebooks', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'ebooks', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'record_id')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'record_id')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_record_id', 'rtable_record_id', self.A, self.B)
        except AssertionError:
            print("Dataset \'ebooks\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C)

    def teardown(self):
        del self.A
        del self.B
        del self.C


class TimeDebugBlockingElectronics:
    timeout=900.0

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'electronics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'electronics', 'B.csv'])
        path_for_C = os.sep.join([datasets_path, 'electronics', 'C.csv'])

        try:
            self.A = em.read_csv_metadata(path_for_A)
            em.set_key(self.A, 'ID')
            self.B = em.read_csv_metadata(path_for_B)
            em.set_key(self.B, 'ID')
            self.C = em.read_csv_metadata(path_for_C)
            cm.set_candset_properties(self.C, '_id', 'ltable_ID', 'rtable_ID', self.A, self.B)
            self.attr_corres = [('ID', 'ID'), ('Brand', 'Brand'), ('Name', 'Name'),
                                ('Amazon_Price', 'Price'), ('Features', 'Features')]
            self.output_size = 200
        except AssertionError:
            print("Dataset \'electronics\' not found. Please visit the project"
                  " website to download the dataset.")

    def time_debug_blocking(self):
        db.debug_blocker(self.A, self.B, self.C,
                         self.output_size, self.attr_corres)

    def teardown(self):
        del self.A
        del self.B
        del self.C
        del self.attr_corres
        del self.output_size

