# Import modules
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from utils.file_utils import read_original_file, save_file


@task(
    task_id='standardize_column_names_with_logic',
    trigger_rule='all_success'
)
def standardize_website_data(website_name):
    """
    Standardize the column names of each website files and the files names
    The standardization will be rule based. It can later be optimized.
    @param website_name: The name of the website
    @return: The website name and the paths to its files
    """
    # Import modules for this task
    import os
    import logging
    from glob import glob
    import pandas as pd
    from dateutil.parser import parse

    ORDER_FILE_NAME = Variable.get('ORDER_FILE_NAME', 'orders')
    PRODUCT_FILE_NAME = Variable.get('PRODUCT_FILE_NAME', 'products')
    TRANSACTION_FILE_NAME = Variable.get(
        'TRANSACTION_FILE_NAME', 'transactions')
    # Get the path to the airflow project
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    def standardize_file_name(df):
        """
        Standardize the file name of a dataframe based on the columns names
        orders -> transaction_id | quantity | ean
        products -> ean | name | price
        transactions -> transaction_id | transaction_date
        @param df: The dataframe to be standardized
        @return: The standardized dataframe
        """
        columns = df.columns
        if set(columns) == set(['transaction_id', 'quantity', 'ean']):
            return ORDER_FILE_NAME
        elif set(columns) == set(['ean', 'name', 'price']):
            return PRODUCT_FILE_NAME
        else:
            return TRANSACTION_FILE_NAME

    def is_date(string, fuzzy=False):
        """
        Return whether the string can be interpreted as a date.
        @param string: str, string to check for date
        @param fuzzy: bool, ignore unknown tokens in string if True
        """
        try:
            parse(string, fuzzy=fuzzy)
            return True
        except ValueError:
            return False

    def map_columns(df):
        """
        Map the dataframe columns to the columns we expect
        ean | name | quantity | price | transaction_id | transaction_date
        This mapping is based on rules, it can be optimized later
        @params df: the merged dataframe from the website
        @returns df: the transform df with the right columns
        """
        column_mapping = {}

        # Only map based on not null values
        df = df.dropna()

        for column in df.columns:
            column_name = column.lower()
            column_type = df[column].dtype

            # By default, we map the column to unknown
            column_mapping[column] = 'unknown'

            # If the column name contains the word ean, we map it to ean
            if 'ean' in column_name:
                column_mapping[column] = 'ean'
                continue

            # If the column value have multiple words
            if df[column].apply(lambda x: isinstance(x, str) and len(x.split()) > 1).all():
                column_mapping[column] = 'name'
                continue

            # If the column name contains the word date or the column values not null have a dateformat, we map it to transaction_date
            if 'date' in column_name or df[column].apply(lambda x: isinstance(x, str) and is_date(x)).all():
                column_mapping[column] = 'transaction_date'
                continue

            # If the column name contains the word id or the column contains a string of 36 characters, we map it to transaction_id.
            # We assume it is a uuid
            if 'id' in column_name or df[column].apply(lambda x: isinstance(x, str) and len(x) == 36).all():
                column_mapping[column] = 'transaction_id'
                continue

            # If the column type is int64 or float64 and the mean value is less than 3.0, we map it to quantity
            if column_type in ['int64', 'float64'] and df[column].mean() < 3.0:
                column_mapping[column] = 'quantity'
                continue

            # If the column type is float64 and the mean value is less than 3.0, we map it to price
            if column_type == 'float64' and df[column].mean() > 3.0:
                column_mapping[column] = 'price'
                continue

        return column_mapping

    logging.info(f'[INFO] Standardizing the column names of {website_name}')
    # Get the path to the website files
    website_files_paths = glob(os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', f"input/{website_name}_*.csv"
    ))

    # Skip the website if there are no three files
    if len(website_files_paths) != 3:
        logging.info(
            f'[INFO] {website_name} files are not complete, skipping it')
        raise AirflowSkipException(
            f'{website_name} files are not complete, skipping it')

    standardized_files_paths = {}
    # Read the data from each file and clean it
    for website_file_path in website_files_paths:
        # Read the data
        data = read_original_file(website_file_path)

        # Skip the website if the file is empty
        if data.empty:
            logging.info(
                f'[INFO] {website_file_path} is empty, skipping it')
            raise AirflowSkipException(
                f'{website_file_path} is empty, skipping it')

        logging.info(
            f'[INFO] Standardizing the column names of {website_file_path}')
        # Clean the data
        column_mapping = map_columns(data)
        # Rename the columns
        data = data.rename(columns=column_mapping)

        # Check if all the columns names are present in our target columns
        # If not, we raise an exception
        if not set(data.columns).issubset({
            'ean', 'name', 'quantity', 'price', 'transaction_id', 'transaction_date'
        }):
            logging.info(
                f'[INFO] The standardization of {website_file_path} with logic failed')
            raise AirflowSkipException(
                f'The columns of {website_file_path} could not be identified')

        # identify the df
        file_type = standardize_file_name(data)
        # Save the data
        standardized_files_paths[file_type] = save_file(
            data, None, f"{website_name}_{file_type}.csv", 'temp')

    return {'website_name': website_name, 'files_paths': standardized_files_paths}
