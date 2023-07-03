# Import modules
from airflow.decorators import task
from airflow.models import Variable
from utils.file_utils import save_file
from utils.config_utils import read_config_file


@task(
    task_id='clean_data',
    trigger_rule='all_success'
)
def clean_website_data(website_data):
    """
    Clean the data of each website
    The cleaning includes:
    1. Drop duplicates
    2. Standardize the decimal delimiter
    3. Handle missing values
    4. Handle outliers
    @param website_data: The name of the website and the paths to its files
    @return: The path to the cleaned files
    """
    # Import modules for this task
    import os
    import logging
    import pandas as pd

    ORDER_FILE_NAME = Variable.get('ORDER_FILE_NAME', 'orders')
    PRODUCT_FILE_NAME = Variable.get('PRODUCT_FILE_NAME', 'products')
    TRANSACTION_FILE_NAME = Variable.get(
        'TRANSACTION_FILE_NAME', 'transactions')
    # Get the path to the airflow
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    def drop_duplicates(df, file_type, website_name):
        """
        Drop duplicates from a dataframe
        @param df: The dataframe to be cleaned
        @param website_name: The name of the website
        @return: The cleaned dataframes
        """
        logging.info(
            f'[INFO] Dropping duplicates from {website_name} - {file_type} file - {len(df) - len(df.drop_duplicates())} duplicates')
        df.drop_duplicates(inplace=True)
        return df

    def standardize_column_decimal_delimiter(df, column_name):
        """
        Standardize the decimal delimiter of a column in a dataframe
        @param df: The dataframe to be cleaned
        @param column_name: The name of the column to be standardized
        @return: The cleaned dataframe
        """
        logging.info(
            f'[INFO] Standardizing decimal delimiter of {column_name}')
        df[column_name] = df[column_name].astype(
            str).str.replace(',', '.')
        df[column_name] = df[column_name].astype(float)
        return df

    def standardize_dataframe_decimal_delimiter(df, file_type, website_name):
        """
        Standardize the decimal delimiter of a dataframe
        @param df: The dataframe to be cleaned
        @param file_type: The type of the file - orders, products, transactions
        @param website_name: The name of the website
        @return: The cleaned dataframe
        """
        logging.info(
            f'[INFO] Standardizing decimal delimiter of {website_name}')
        if file_type == ORDER_FILE_NAME:
            df = standardize_column_decimal_delimiter(df, 'quantity')
        elif file_type == PRODUCT_FILE_NAME:
            df = standardize_column_decimal_delimiter(df, 'price')
        return df

    def handle_outliers(df, file_type, website_name):
        """
        Handle outliers of a dataframe
        @param df: The dataframe to be cleaned
        @param file_type: The type of the file - orders, products, transactions
        @param website_name: The name of the website
        @return: The cleaned dataframe
        """
        logging.info(f'[INFO] Handling outliers of {website_name}')
        outlier_config = read_config_file(
            os.path.join(AIRFLOW_DAGS_DIR, 'include', 'config', 'outliers.json'))
        # Remove outliers
        if file_type in outlier_config:
            for column_config in outlier_config[file_type]:
                df = df[(df[column_config['column']] > column_config['low']) & (
                    df[column_config['column']] < column_config['high'])]
        return df

    def drop_missing_values(df, file_type, scope='all', relevant_columns=[]):
        """
        Drop rows with missing values in relevant columns from a dataframe
        @param df: The dataframe to be cleaned
        @param file_type: The type of the file - orders, products, transactions
        @param scope: The scope of the relavant columns - all, limited
        @param relevant_columns: The columns to be checked for missing values
        @return: The cleaned dataframe
        """
        # Get the rows missing relevant information based on the scope
        missing_values = df[df.isnull().any(
            axis=1)] if scope == 'all' else df[df[relevant_columns].isnull().any(axis=1)]
        # Save the missing values in a csv file
        if len(missing_values):
            save_file(missing_values, None,
                      f'{website_name}_missing_{file_type}.csv', 'missing')
        # Drop the rows with missing values
        df.drop(missing_values.index, inplace=True)
        return df

    def handle_missing_values(df, file_type, website_name):
        """
        Handle missing values of a dataframe
        Remove rows with all missing values
        Remove columns with all missing values
        @param df: The dataframe to be cleaned
        @param file_type: The type of the file - orders, products, transactions
        @param website_name: The name of the website
        @return: The cleaned dataframe
        """
        logging.info(
            f'[INFO] Handling missing values of {website_name} - {file_type} file - {df.isnull().sum().sum()} missing values')
        # Remove rows with all missing values
        df.dropna(axis=0, how='all', inplace=True)
        # Remove columns with all missing values
        df.dropna(axis=1, how='all', inplace=True)

        # Handle missing values in the orders file and transactions file
        if file_type == ORDER_FILE_NAME or file_type == TRANSACTION_FILE_NAME:
            # Drop rows with missing values in relevant columns
            df = drop_missing_values(df, file_type)
        else:
            # Drop rows with missing values in relevant columns in products file
            df = drop_missing_values(
                df, file_type, 'limited', ['ean', 'price'])
        return df

    def clean_file(df, file_type, website_name):
        """
        Clean one of the files of a website
        Check for missing values, duplicates, decimal delimiters, outliers, etc.
        @param df: The dataframe to be cleaned
        @param file_type: The type of the file - orders, products, transactions
        @param website_name: The name of the website
        @return: The cleaned dataframe
        """
        # Drop duplicates
        df = drop_duplicates(df, file_type, website_name)
        # Handle missing values
        df = handle_missing_values(df, file_type, website_name)
        # Change the decimal delimiter
        df = standardize_dataframe_decimal_delimiter(
            df, file_type, website_name)
        # Handle outliers
        df = handle_outliers(df, file_type, website_name)
        return df

    # Get the website name
    website_name = website_data['website_name']
    # Get the paths to the website files
    website_files_path = website_data['files_paths']

    logging.info(f'[INFO] Cleaning the data of {website_name}')

    clean_files_paths = {}
    for file_type, file_path in website_files_path.items():
        # Read the website data
        df = pd.read_csv(file_path)
        # Clean the data
        df = clean_file(df, file_type, website_name)
        # Save the data
        clean_files_paths[file_type] = save_file(
            df, None, f'{website_name}_{file_type}.csv', 'temp')

    return {'website_name': website_name, 'files_paths': clean_files_paths}
