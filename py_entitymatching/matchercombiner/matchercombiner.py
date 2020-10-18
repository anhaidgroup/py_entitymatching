from math import ceil
import pandas as pd
import numpy as np

class MajorityVote(object):
    """
    THIS CLASS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK. 
    
    The goal of this combiner is to combine a list of predictions from multiple 
    matchers to produce a consolidated prediction. In this majority voting-based 
    combining, the prediction that occurs most is returned as the consolicated 
    prediction. If there is no clear winning prediction (for example, 0 and 1 occuring 
    equal number of times) then 0 is returned.
    
    Implementation wise, there should be a combiner command to which an object of this 
    class must be given as a parameter. Based on this parameter, the combiner command 
    will use this object to combine the predictions.
    """
    def __init__(self):
        pass

    def combine(self, predictions):
        """
        Combine a list of predictions from matchers using majority voting.
        
        Args:
            predictions (DataFrame): A table containing predictions from multiple 
                                    matchers. 
        
        Returns:
            A list of consolidated predictions.
        
        Examples:
            >>> dt = DTMatcher()
            >>> rf = RFMatcher()
            >>> nb = NBMatcher()
            >>> dt.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label') # H is training set containing feature vectors
            >>> dt.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='dt_predictions') # L is the test set for which we should get predictions. 
            >>> rf.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
            >>> rf.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='rf_predictions')  
            >>> nb.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
            >>> nb.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='nb_predictions')
            >>> mv_combiner = MajorityVote()
            >>> L['consol_predictions'] = mv_combiner.combine(L[['dt_predictions', 'rf_predictions', 'nb_predictions']])
        """
        combined_prediction = np.apply_along_axis(lambda x: np.argmax(
            np.bincount(x)), axis=1, arr=predictions)
        return combined_prediction

class WeightedVote(object):
    """
    THIS CLASS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK. 

    The goal of this combiner is to combine a list of predictions from multiple 
    matchers to produce a consolidated prediction. In this weighted voting-based 
    combining, each prediction is given a weight, we compute a weighted sum of these 
    predictions and compare the result to a threshold. If the result is greater than or equal to 
    the threshold then the consolidated prediction is returned as a match (i.e., 1) else
    returned as a no-match.
    
    Implementation wise, there should be a combiner command to which an object of this 
    class must be given as a parameter. Based on this parameter, the combiner command 
    will use this object to combine the predictions.
    """

    def __init__(self, weights=None, threshold=None):
        """
        Constructor for weighted voting-based combiner.
        
        Args:
            weights (list): A list of real-valued numbers.
            threshold (float): The threshold to which the weighted sum must be compared to.
        """
        self.weights = weights
        self.threshold = threshold

    def combine(self, predictions):
        """
        Combine a list of predictions from matchers using weighted voting.

        Args:
            predictions (DataFrame): A table containing predictions from multiple 
                                    matchers. 

        Returns:
            A list of consolidated predictions.

        Examples:
            >>> dt = DTMatcher()
            >>> rf = RFMatcher()
            >>> nb = NBMatcher()
            >>> dt.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label') # H is training set containing feature vectors
            >>> dt.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='dt_predictions') # L is the test set for which we should get predictions. 
            >>> rf.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
            >>> rf.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='rf_predictions')  
            >>> nb.fit(table=H, exclude_attrs=['_id', 'l_id', 'r_id'], target_attr='label')
            >>> nb.predict(table=L, exclude_attrs=['id', 'l_id', 'r_id'], append=True, inplace=True, target_attr='nb_predictions')
            >>> wv_combiner = WeightedVote(weights=[0.1, 0.2, 0.1], threshold=0.2)
            >>> L['consol_predictions'] = wv_combiner.combine(L[['dt_predictions', 'rf_predictions', 'nb_predictions']])
        """

        num_matchers = predictions.shape[1]
        if self.weights is not None:
            assert num_matchers is len(num_matchers), 'Num matchers and weights do not match'
            w = np.asarray(self.weights)
        else:
            w = np.ones(num_matchers, )

        if self.threshold is None:
            t = ceil((num_matchers+1.0)/2.0)
        else:
            t = self.threshold

        combined_prediction = np.apply_along_axis(lambda x: 1 if
        np.inner(x, w) >= t else 0, axis=1, arr=predictions)
        return combined_prediction
