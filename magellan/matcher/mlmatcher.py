import pandas as pd
import numpy as np

from sklearn.preprocessing import Imputer

from magellan.matcher.matcher import Matcher
from magellan.utils.generic_helper import list_diff
import magellan.catalog.catalog_manager as cm

class MLMatcher(Matcher):
    def fit_sklearn(self, x, y, check_rem=True):
        x, y = self.get_data_for_sklearn(x, y, check_rem=check_rem)
        self.clf.fit(x, y)
        return True

    def fit_ex_attrs(self, table, exclude_attrs, target_attr):
        if not isinstance(exclude_attrs, list):
            exclude_attrs = [exclude_attrs]
        attrs_to_project = list_diff(table.columns, exclude_attrs)
        x = table[attrs_to_project]
        y = table[target_attr]
        self.fit_sklearn(x, y, check_rem=False)


    def fit(self, x=None, y=None, table=None, exclude_attrs=None, target_attr=None):
        if x is not None and y is not None:
            self.fit_sklearn(x, y)
        elif (table is not None and exclude_attrs is not None) and target_attr is not None:
            self.fit_ex_attrs(table, exclude_attrs, target_attr)
            #print 'After fitting'
        else:
            raise SyntaxError('The arguments supplied does not match the signatures supported !!!')


    def predict_sklearn(self, x, check_rem=True):

        # Note: here check_rem is just passing what is coming in i.e it can be true or false based up on who is calling
        # it
        x = self.get_data_for_sklearn(x, check_rem=check_rem)
        y = self.clf.predict(x)
        return y

    # variant of predict method, where data is derived based on exclude attributes

    def predict_ex_attrs(self, table, exclude_attrs):
        if not isinstance(exclude_attrs, list):
            exclude_attrs = [exclude_attrs]
        table = table.to_dataframe()
        attrs_to_project = list_diff(table.columns, exclude_attrs)
        x = table[attrs_to_project]
        y = self.predict_sklearn(x, check_rem=False)
        return y

    # predict method
    def predict(self, x=None, table=None, exclude_attrs=None, target_attr=None, append=False, inplace=True):
        if x is not None:
            y = self.predict_sklearn(x)
            if table is not None and target_attr is not None and append is True:
                if inplace == True:
                    table[target_attr] = y
                    return table
                else:
                    tbl = table.copy()
                    tbl[target_attr] = y
                    return tbl
        elif table is not None and exclude_attrs is not None:
            y = self.predict_ex_attrs(table, exclude_attrs)
            if target_attr is not None and append is True:
                if inplace == True:
                    table[target_attr] = y
                    return table
                else:
                    tbl = table.copy()
                    tbl[target_attr] = y
                    return tbl

        else:
            raise SyntaxError('The arguments supplied does not match the signatures supported !!!')
        return y

    # get and set name of matcher
    def get_name(self):
        return self.name

    def set_name(self, name):
        self.name = name



# helper functions

    def get_data_for_sklearn(self, x, y=None, check_rem=True):
        # check to see if we have to remove id column
        if x.columns[0] is '_id' and check_rem is True:
            x = x.values
            x = np.delete(x, 0, 1)
        else:
            x = x.values
        if y is not None:
            if not isinstance(y, pd.Series) and y.columns[0] is '_id' and check_rem is True:
                y = y.values
                y = np.delete(y, 0, 1)
            else:
                y = y.values
        # if mg._impute_flag == True:
        #     imp = Imputer(missing_values='NaN', strategy='median', axis=0)
        #     imp.fit(x)
        #     x = imp.transform(x)
        if y is not None:
            return x, y
        else:
            return x

