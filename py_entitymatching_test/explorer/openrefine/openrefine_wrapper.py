import os.path
import json
import pandas
import webbrowser
import requests
import six
from six.moves import urllib
import tempfile
from py_entitymatching.utils.validation_helper import validate_object_type
import pandas as pd


def data_explore_openrefine(df, server='http://127.0.0.1:3333', name=None):
    """
    Wrapper function for using OpenRefine. Gives user a GUI to examine and edit
    the dataframe passed in using OpenRefine.

    Args:
        df (Dataframe): The pandas dataframe to be explored with pandastable.
        server (String): The address of the OpenRefine server (defaults to
            http://127.0.0.1:3333).
        name (String): The name given to the file and project in OpenRefine.

    Raises:
        AssertionError: If `df` is not of type pandas DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')
        >>> em.data_explore_openrefine(A, name='Table')

    """
    # Validate input parameters
    # # We expect the df to be of type pandas DataFrame
    validate_object_type(df, pd.DataFrame, 'Input df')
    return DataExploreOpenRefine(df, server, name)


class DataExploreOpenRefine:
    """
    A wrapper for OpenRefine.
    """

    def __init__(self, df, server='http://127.0.0.1:3333', name=None):
        self.server = server[:-1] if server.endswith('/') else server
        # write the pandas frame to csv file
        # create temp file
        __, file_name = tempfile.mkstemp(suffix='.csv')
        outfile = os.fdopen(__, 'r+')
        df.to_csv(outfile, index=False)
        outfile.close()
        file_path = file_name
        if name is not None:
            project_name = name
        else:
            project_name = file_name

        values = {
            'project-name': project_name
        }
        outfile = open(file_path, 'r+')
        files = {'file': outfile}
        url = self.server + '/command/core/create-project-from-upload'
        response = requests.post(url, files=files, data=values)
        url_params = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)
        outfile.close()
        os.remove(file_name)
        if 'project' in url_params:
            self.id = id = url_params['project'][0]
            self.project_name = project_name
            # open the project in the webbrowser.
            webbrowser.open(self.server + '/project?project=' + id, new=1)

    def export_pandas_frame(self, format='tsv'):
        """
        Exports the data from OpenRefine and transfers it a pandas Dataframe

        Args:
            format (String): Project format

        Returns:
            The new pandas frame with the data changed by the GUI operation

         Examples:
            >>> import py_entitymatching as em
            >>> A = em.read_csv_metadata('path_to_csv_dir/table.csv', key='ID')
            >>> em.data_explore_openrefine(A, name='Table')
            >>> df = p.export_pandas_frame()

        """

        values = {
            'engine': '{"facets":[],"mode":"row-based"}',
            'project': self.id,
            'format': format
        }
        response = requests.post(self.server + '/command/core/export-rows/' + self.project_name + '.' + format,
                                 data=values)
        st = six.StringIO(response.content.decode('utf-8'))
        df = pandas.read_csv(st, sep="\t")
        self.delete_project()
        return df

    def delete_project(self):
        """
        Delete the openrefine project
        """
        values = {
            'project': self.id
        }
        response = requests.post(self.server + '/command/core/delete-project', data=values)
        response_json = json.loads(response.content.decode('utf-8'))
        return 'code' in response_json and response_json['code'] == 'ok'
