"""
This module contains functions related to ML-matcher, that is common across
all the ML-matchers.
"""
import logging

import pandas as pd
import numpy as np

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.matcher.matcher import Matcher
from py_entitymatching.matcher.matcherutils import get_true_lbl_index
import py_entitymatching.utils.catalog_helper as ch
import py_entitymatching.utils.generic_helper as gh

logger = logging.getLogger(__name__)


class MLMatcher(Matcher):
    """
    ML Matcher class.
    """
    def _fit_sklearn(self, x, y, check_rem=True):
        """
        This function mimics fit method supported by sk-learn.
        """
        # From the given input, derive the data that can be used for sk-learn
        #  methods.
        x, y = self._get_data_for_sklearn(x, y, check_rem=check_rem)
        # Call the fit method from the underlying classifier.
        self.clf.fit(x, y)
        return True

    def _fit_ex_attrs(self, table, exclude_attrs, target_attr):
        """
        This function supports the fit method, where the DataFrame can be
        given as input along with what attributes must be excluded and the
        target attribute.
        """
        # Validate the input parameters.
        # # We expect the input table to be of type pandas DataFrame.
        if not isinstance(table, pd.DataFrame):
            logger.error('Input table is not of type DataFrame')
            raise AssertionError('Input table is not of type DataFrame')

        # Convert the exclude attributes into list (if the input is not of list)
        if not isinstance(exclude_attrs, list):
            exclude_attrs = [exclude_attrs]

        # Check if the exclude attributes are present in the input table. If
        # not, raise an error.
        if not ch.check_attrs_present(table, exclude_attrs):
            logger.error(
                'The attributes mentioned in exclude_attrs is not present ' \
                'in the input table')
            raise AssertionError(
                'The attributes mentioned in exclude_attrs is not present ' \
                'in the input table')

        # Check if the target attribute is present in the input table. If
        # not, raise an error.
        if not ch.check_attrs_present(table, target_attr):
            logger.error('The target_attr is not present in the input table')
            raise AssertionError(
                'The target_attr is not present in the input table')

        # We now remove duplicate attributes from the exclude_attrs
        exclude_attrs = gh.list_drop_duplicates(exclude_attrs)

        # We explicitly append target attribute to exclude attributes
        if target_attr not in exclude_attrs:
            exclude_attrs.append(target_attr)

        # Now, we get the attributes to project
        attributes_to_project = gh.list_diff(list(table.columns), exclude_attrs)

        # Get the predictors and the target attribute from the input table
        # based on the exclude attrs and the target attribute.
        x = table[attributes_to_project]
        y = table[target_attr]

        self._fit_sklearn(x, y, check_rem=False)

    def fit(self, x=None, y=None, table=None, exclude_attrs=None,
            target_attr=None):
        """
        Fit interface for the matcher.

        Specifically, there are two ways the user can call the fit method.
        First, interface similar to scikit-learn where the feature vectors
        and target attribute given as projected DataFrame.
        Second, give the DataFrame and explicitly specify the feature vectors
        (by specifying the attributes to be excluded) and the target attribute.

        A point to note is all the input parameters have a default value of
        None. This is done to support both the interfaces in a single function.

        Args:
            x (DataFrame): The input feature vectors given as pandas
             DataFrame (defaults to None).
            y (DatFrame): The input target attribute given as pandas
                DataFrame with a single column (defaults to None).
            table (DataFrame): The input pandas DataFrame containing feature
                vectors and target attribute (defaults to None).
            exclude_attrs (list): The list of attributes that should be
                excluded from the input table to get the feature vectors.
            target_attr (string): The target attribute in the input table.
        """
        # Check if x and y is given, then call a function that handles
        # sk-learn like interface input.
        if x is not None and y is not None:
            self._fit_sklearn(x, y)
        # Check if table and its associated attributes, then call the
        # appropriate function that handles it.
        elif (
                table is not None and exclude_attrs is not None) \
                and target_attr is not None:
            self._fit_ex_attrs(table, exclude_attrs, target_attr)
        else:
        # If the syntax is not what we expect, raise an syntax error.
            raise SyntaxError(
                'The arguments supplied does not match the signatures '
                'supported !!!')

    def _predict_sklearn(self, x, check_rem=True, return_prob=False):
        # Function that implements, predict interface mimic-ing sk-learn's
        # predict interface.

        # Here check_rem parameter requires a bit of explanation. The
        # check_rem flag checks if the input table has '_id' attribute if so
        # and if check_rem is True then we remove the '_id' attribute from
        # the table.
        # Note: Here check_rem is just passing what is coming in i.e it can be
        # true or false based up on who is calling it.
        x = self._get_data_for_sklearn(x, check_rem=check_rem)
        # Call the underlying predict function.
        y = self.clf.predict(x)
        if not return_prob:
            # Return the predictions
            return y
        else:
            _p = self.clf.predict_proba(x)
            true_index = get_true_lbl_index(self.clf)
            return y, _p[:, true_index]

    def _predict_ex_attrs(self, table, exclude_attrs, return_prob=False):
        """
        Variant of predict method, where data is derived based on exclude
        attributes.
        """
        # Validate input parameters
        # # We expect input table to be a pandas DataFrame.
        if not isinstance(table, pd.DataFrame):
            logger.error('Input table is not of type DataFrame')
            raise AssertionError('Input table is not of type DataFrame')

        # # We expect the exclude attributes to be a list, if not convert it
        # into a list.
        if not isinstance(exclude_attrs, list):
            exclude_attrs = [exclude_attrs]

        # Check if the input table contains the attributes to be excluded. If
        #  not raise an error.
        if not ch.check_attrs_present(table, exclude_attrs):
            logger.error(
                'The attributes mentioned in exclude_attrs is not present ' \
                'in the input table')
            raise AssertionError(
                'The attributes mentioned in exclude_attrs is not present ' \
                'in the input table')

        # Get the attributes to project.
        attributes_to_project = gh.list_diff(list(table.columns), exclude_attrs)
        # Get feature vectors and the target attribute
        x = table[attributes_to_project]


        # Do the predictions and return the probabilities (if required)
        res = self._predict_sklearn(x, check_rem=False, return_prob=return_prob)
        return res

        # if not just do the predictions and return the result
        # if not return_prob:
        #     # Do the predictions using the ML-based matcher.
        #     y = self._predict_sklearn(x, check_rem=False)
        #
        #     # Finally return the predictions
        #     return y
        # else:
        #     res = self._predict_sklearn()

    # predict method
    def predict(self, x=None, table=None, exclude_attrs=None, target_attr=None,
                append=False, return_probs=False,
                probs_attr=None, inplace=True):
        """
        Predict interface for the matcher.

        Specifically, there are two ways the user can call the predict method.
        First, interface similar to scikit-learn where the feature vectors
        given as projected DataFrame.
        Second, give the DataFrame and explicitly specify the feature vectors
        (by specifying the attributes to be excluded) .

        A point to note is all the input parameters have a default value of
        None. This is done to support both the interfaces in a single function.


        Args:
            x (DataFrame): The input pandas DataFrame containing only feature
                vectors (defaults to None).
            table (DataFrame): The input pandas DataFrame containing feature
                vectors, and may be other attributes (defaults to None).
            exclude_attrs (list): A list of attributes to be excluded from the
                input table to get the feature vectors (defaults to None).
            target_attr (string): The attribute name where the predictions
                need to be stored in the input table (defaults to None).
            probs_attr (string): The attribute name where the prediction probabilities 
                need to be stored in the input table (defaults to None).
            append (boolean): A flag to indicate whether the predictions need
                to be appended in the input DataFrame (defaults to False).
            return_probs (boolean): A flag to indicate where the prediction probabilities
                need to be returned (defaults to False). If set to True, returns the 
                probability if the pair was a match.
            inplace (boolean): A flag to indicate whether the append needs to be
                done inplace (defaults to True).

        Returns:
            An array of predictions or a DataFrame with predictions updated.

        """
        # If x is not none, call the predict method that mimics sk-learn
        # predict method.
        if x is not None:
            y = self._predict_sklearn(x, return_prob=return_probs)
        # If the input table and the exclude attributes are not None,
        # then call the appropriate predict method.
        elif table is not None and exclude_attrs is not None:
            y = self._predict_ex_attrs(table, exclude_attrs, return_prob=return_probs)
            # If the append is True, update the table
            if target_attr is not None and append is True:
                # If inplace is True, then update the input table.
                if inplace:
                    if return_probs:
                        table[target_attr] = y[0]
                        table[probs_attr] = y[1]
                        # Return the updated table
                        return table
                    else:
                        # Return the updated table
                        table[target_attr] = y
                        return table
                else:
                # else, create a copy and update it.
                    table_copy = table.copy()
                    if return_probs:
                        table_copy[target_attr] = y[0]
                        table_copy[probs_attr] = y[1]
                    else:
                        table_copy[target_attr] = y
                    # copy the properties from the input table to the output
                    # table.
                    cm.copy_properties(table, table_copy)
                    # Return the new table.
                    return table_copy

        else:
            # else, raise a syntax error
            raise SyntaxError(
                'The arguments supplied does not match '
                'the signatures supported !!!')
        # Return the predictions
        return y

    # get and set name of matcher
    def get_name(self):
        # Return the name of the matcher
        return self.name

    def set_name(self, name):
        # Set the name of the matcher
        self.name = name

    # helper functions
    def _get_data_for_sklearn(self, x, y=None, check_rem=True):
        """
        Gets data in a format that can be used to call sk-learn methods such
        as fit and predict.
        """
        # Validate input parameters.
        # # We expect the input object (x) to be of type pandas DataFrame.
        if not isinstance(x, pd.DataFrame):
            logger.error('Input table is not of type DataFrame')
            raise AssertionError('Input table is not of type DataFrame')

        # Check to see if we have to remove id column
        if x.columns[0] == '_id' and check_rem == True:
            logger.warning(
                'Input table contains "_id". '
                'Removing this column for processing')
            # Get the values from the DataFrame
            x = x.values
            # Remove the first column ('_id')
            x = np.delete(x, 0, 1)
        else:
            # Get the values from the DataFrame
            x = x.values
        if y is not None:
            # Remove the _id column from the input.
            if not isinstance(y, pd.Series) and y.columns[0] == '_id' \
                    and check_rem == True:
                logger.warning(
                    'Input table contains "_id". '
                    'Removing this column for processing')
                # Get the values from the DataFrame
                y = y.values
                y = np.delete(y, 0, 1)
            else:
                # Get the values from the DataFrame
                y = y.values
            # Return both x and y
            return x, y
        else:
            # Return x
            return x
