try:
    from tkinter import *
except ImportError as e:
    from Tkinter import *

from py_entitymatching.utils.validation_helper import validate_object_type
import pandas as pd


def data_explore_pandastable(df):
    """
    Wrapper function for pandastable. Gives user a GUI to examine and edit
    the dataframe passed in using pandastable.

    Args:
        df (Dataframe): The pandas dataframe to be explored with pandastable.

    Raises:
        AssertionError: If `df` is not of type pandas DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')
        >>> em.data_explore_pandastable(A)

    """

    # Validate input parameters
    # # We expect the df to be of type pandas DataFrame
    validate_object_type(df, pd.DataFrame, 'Input df')
    DataExplorePandastable(df)


class DataExplorePandastable(Frame):
    """
    A wrapper for pandastable.
    """

    def __init__(self, df):
        # Import
        try:
            from pandastable import Table, TableModel
        except ImportError:
            raise ImportError('Pandastable is not installed. Please install pandastable to use '
                              'pandastable data exploration functions.')

        self.parent = None
        Frame.__init__(self)
        self.main = self.master
        self.main.geometry('600x400+200+100')
        self.main.title('Explore Data')
        f = Frame(self.main)
        f.pack(fill=BOTH, expand=1)
        # set the table in the GUI
        self.table = pt = Table(f, dataframe=df,
                                showtoolbar=True, showstatusbar=True)
        pt.show()
        self.mainloop()

