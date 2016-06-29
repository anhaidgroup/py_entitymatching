# Write the benchmarking functions here.                                        
# See "Writing benchmarks" in the asv docs for more information.

import os
import sys

import magellan  as mg

p = mg.get_install_path()
datasets_path = os.sep.join([p, 'datasets', 'example_datasets'])
bb = mg.BlackBoxBlocker()
ob = mg.OverlapBlocker()
ab = mg.AttrEquivalenceBlocker()

def _restaurants_function(x, y):
    # x, y will be of type pandas series
    black_list = ['for', 'the', 'of', 'a', 'an', 'and', '&', 'on', 'cafe',
                  'restaurant', 'grill', 'pizza', 'pizzeria', 'pub', 'bar']
    # get name attribute
    x_name = x['NAME'].lower()
    y_name = y['NAME'].lower()
    # get last names
    x_name = x_name.split(' ')
    y_name = y_name.split(' ')
    # check if last names match
    for index in range(len(x_name)):
        x_name[index].replace(' ', '')
        if(x_name[index] in black_list):
            continue
        for z in range(len(y_name)):
            y_name[z].replace(' ', '')
            if(x_name[index] == "" or y_name[z] == ""):
                continue
            if(y_name[z] in black_list):
                continue
            if(x_name[index] == y_name[z]):
                return False
            else:
                if len(x_name[index]) > len(y_name[z]):
                    x_name[index], y_name[z] = y_name[z], x_name[index]
                distances = range(len(x_name[index]) + 1)
                for index2,char2 in enumerate(y_name[z]):
                    newDistances = [index2 + 1]
                    for index1, char1 in enumerate(x_name[index]):
                        if char1 == char2:
                            newDistances.append(distances[index1])
                        else:
                            newDistances.append(1 + min((distances[index1],
                                                distances[index1 + 1],
                                                newDistances[-1])))
                    distances = newDistances
                if(distances[-1] < 3 and len(x_name[index]) > 2 and len(y_name[z]) > 2):
                    return False
    return True

def _bikes_function(x, y):
    # x, y will be of type pandas series
    # get kilometer driven attribute
    x_km_driven = x['km_driven']
    y_km_driven = y['km_driven']
    # max_value = max(x_km_driven, y_km_driven)
    # percent_weight = (PERCENT_WEIGHT_ABOVE_BELOW/100)* max_value
    # check if kilometer driven difference is less than 1000
    if abs(x_km_driven - y_km_driven) <= 1000:
        return False
    else:
        return True

def _electronics_function(x, y):
    try:
        x_price = x['Amazon_Price']
        x_price = x_price.replace(',', '')
        x_price = x_price.replace('$', '')
        y_price = y['Price']
        y_price = y_price.replace(',', '')
        y_price = y_price.replace('$', '')
        x_price = float(x_price)
        y_price = float(y_price)
    except:
        return True
    if x_price > 1.4 * y_price or x_price < 0.6 * y_price:
        return True
    else:
        return False

def _music_function(ltuple, rtuple):
    rtuple_date=None
    ltuple_date=None
    try:
        rtuple_date=rtuple['Released']
        ltuple_date=ltuple['Released']
        if len(rtuple_date)==0 or len(ltuple_date):
            return True
        date_object1 = datetime.strptime(rtuple.strip(), '%B %d, %Y')
        date_object2 = datetime.strptime(ltuple.strip(), '%d-%b-%y')
        return abs(date_object1-date_object2).days > 3
    except:
        return False

# The blocker function should drop tuple pairs whose ABV values are similar
# The function has to do the following steps
#  1) Get ABV attributes from each of the tuples
#  2) Check whether the ABV is a missing value
#  3) Translate ABV from string to float value
#  4) Compute and check if two ABV values are similar
def _beer_function(x, y):
    # x, y will be of type pandas series
    
    # get ABV attribute
    x_ABV = x['ABV']
    y_ABV = y['ABV']

    # if missing value exists
    if x_ABV == '-':
        return False
    if y_ABV == '-':
        return False

    # translate ABV string to float value
    x_ABV_Value = float(x_ABV[0 : len(x_ABV) - 1])
    y_ABV_Value = float(y_ABV[0 : len(y_ABV) - 1])

    # check if two ABV values are similar by relative threshold t = 0.01
    if abs(x_ABV_Value - y_ABV_Value) / max(x_ABV_Value, y_ABV_Value) > 0.01:
        return True
    else:
        return False

class TimeBlockTablesRestaurants:
    timeout=10000.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        self.l_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.r_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')
        bb.set_black_box_function(_restaurants_function)

    def time_block_tables(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_2(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=2)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_4(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=4)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_all(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=-1)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockCandsetRestaurants:
    timeout=300.0
    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'restaurants', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'restaurants', 'B.csv'])
        l_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        r_output_attrs = ['NAME', 'PHONENUMBER', 'ADDRESS']
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'ID')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'ID')
        self.C = ob.block_tables(A, B, 'NAME', 'NAME',
                                 l_output_attrs=l_output_attrs,
                                 r_output_attrs=r_output_attrs)
        bb.set_black_box_function(_restaurants_function)

    def time_block_candset(self):
        D = bb.block_candset(self.C)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_2(self):
        D = bb.block_candset(self.C, n_jobs=2)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_4(self):
        D = bb.block_candset(self.C, n_jobs=4)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_all(self):
        D = bb.block_candset(self.C, n_jobs=-1)
        print >> sys.stderr, 'size of D:', len(D)

    def teardown(self):
        del self.C

class TimeBlockTablesBikes:
    timeout = 10000.0

    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'id')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'id')
        self.l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']
        self.r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                               'color', 'model_year']
        bb.set_black_box_function(_bikes_function)

    def time_block_tables(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_2(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=2)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_4(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=4)
        print >> sys.stderr, 'size of C:', len(C)

    def time_block_tables_njobs_all(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs, n_jobs=-1)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockCandsetBikes:
    timeout = 300.0

    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([datasets_path, 'bikes', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'bikes', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'id')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'id')
        l_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                          'color', 'model_year']
        r_output_attrs = ['bike_name', 'city_posted', 'km_driven', 'price',
                          'color', 'model_year']
        C = ab.block_tables(A, B, 'city_posted', 'city_posted',
                            l_output_attrs, r_output_attrs)
        self.D = ab.block_candset(C, 'model_year', 'model_year')
        bb.set_black_box_function(_bikes_function)

    def time_block_candset(self):
        E = bb.block_candset(self.D)
        print >> sys.stderr, 'size of D:', len(self.D)
        print >> sys.stderr, 'size of E:', len(E)

    def time_block_candset_njobs_2(self):
        E = bb.block_candset(self.D, n_jobs=2)
        print >> sys.stderr, 'size of D:', len(self.D)
        print >> sys.stderr, 'size of E:', len(E)

    def time_block_candset_njobs_4(self):
        E = bb.block_candset(self.D, n_jobs=4)
        print >> sys.stderr, 'size of D:', len(self.D)
        print >> sys.stderr, 'size of E:', len(E)

    def time_block_candset_njobs_all(self):
        E = bb.block_candset(self.D, n_jobs=-1)
        print >> sys.stderr, 'size of D:', len(self.D)
        print >> sys.stderr, 'size of E:', len(E)

    def teardown(self):
        del self.D
"""
class TimeBlockTablesElectronics:
    timeout = 10000.0

    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([datasets_path, 'electronics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'electronics', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'ID')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'ID')
        self.l_output_attrs = ['Brand', 'Amazon_Price']
        self.r_output_attrs = ['Brand', 'Price']
        bb.set_black_box_function(_electronics_function)

    def time_block_tables(self):
        C = bb.block_tables(self.A, self.B, self.l_output_attrs, self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs
"""
class TimeBlockCandsetElectronics:
    timeout = 300.0

    def setup(self):
        p = mg.get_install_path()
        path_for_A = os.sep.join([datasets_path, 'electronics', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'electronics', 'B.csv'])
        A = mg.read_csv_metadata(path_for_A)
        mg.set_key(A, 'ID')
        B = mg.read_csv_metadata(path_for_B)
        mg.set_key(B, 'ID')
        self.C = ab.block_tables(A, B, 'Brand', 'Brand', ['Brand', 'Amazon_Price'],
                                 ['Brand', 'Price'])
        bb.set_black_box_function(_electronics_function)

    def time_block_candset(self):
        D = bb.block_candset(self.C)
        print >> sys.stderr, 'size of C:', len(self.C)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_2(self):
        D = bb.block_candset(self.C, n_jobs=2)
        print >> sys.stderr, 'size of C:', len(self.C)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_4(self):
        D = bb.block_candset(self.C, n_jobs=4)
        print >> sys.stderr, 'size of C:', len(self.C)
        print >> sys.stderr, 'size of D:', len(D)

    def time_block_candset_njobs_all(self):
        D = bb.block_candset(self.C, n_jobs=-1)
        print >> sys.stderr, 'size of C:', len(self.C)
        print >> sys.stderr, 'size of D:', len(D)

    def teardown(self):
        del self.C
"""
class TimeBlockTablesMusic:
    timeout=10000.0

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'music', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'music', 'B.csv'])
        self.l_output_attrs = ['Album_Name', 'Artist_Name', 'CopyRight', 'Released', 'Song_Name', 'Time']
        self.r_output_attrs = ['Album_Name', 'Artist_Name', 'Copyright', 'Released', 'Song_Name', 'Time']
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'Sno')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'Sno')
        bb.set_black_box_function(_music_function)

    def time_block_tables(self):
        C = bb.block_tables(self.A, self.B,
                            self.l_output_attrs,
                            self.r_output_attrs)
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B
        del self.l_output_attrs
        del self.r_output_attrs

class TimeBlockTablesBeer:
    timeout=10000.0

    def setup(self):
        path_for_A = os.sep.join([datasets_path, 'beer', 'A.csv'])
        path_for_B = os.sep.join([datasets_path, 'beer', 'B.csv'])
        self.A = mg.read_csv_metadata(path_for_A)
        mg.set_key(self.A, 'Label')
        self.B = mg.read_csv_metadata(path_for_B)
        mg.set_key(self.B, 'Label')
        bb.set_black_box_function(_beer_function)

    def time_block_tables(self):
        C = bb.block_tables(self.A, self.B, ['ABV'], ['ABV'])
        print >> sys.stderr, 'size of C:', len(C)

    def teardown(self):
        del self.A
        del self.B


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

"""
