# Import the libraries
import os
import pandas as pd


def read_original_file(file_path):
    """
    Read the data from a file
    @param file_path: The path to the file
    @return: The data
    """
    data = pd.read_csv(file_path, sep=None, engine='python')
    # Remove unamed columns
    data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
    return data


def save_file(data, path=None, file_name=None, target_folder=None):
    """
    Save a dataframe to a csv file
    @param data: The dataframe to be saved
    @param path: The path to the file
    @param file_name: The name of the file
    @param target_folder: The folder to save the file
    @return: The path to the file
    """
    # Get the path to the airflow project
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    file_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', target_folder, file_name) if path is None else path
    data.to_csv(file_path, index=False)
    return file_path
