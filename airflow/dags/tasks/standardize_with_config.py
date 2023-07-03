# Import modules
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from utils.file_utils import read_original_file, save_file
from utils.config_utils import read_config_file


@task(
    task_id='standardize_column_names_with_config',
    trigger_rule='all_success'
)
def standardize_website_data(website_name):
    """
    Standardize the column names of each website files and the files names
    The standardization will be based on a config file
    @param website_name: The name of the website
    @return: The website name and the paths to its files
    """

    import os
    import logging

    # Get the path to the airflow
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    def standardize_column_names(df, column_names):
        """
        Standardize the column names of a dataframe
        @param df: The dataframe to be standardized
        @param column_names: The column names to be standardized
        @return: The standardized dataframe
        """
        df = df.rename(columns=column_names)
        return df

    logging.info(f'[INFO] Standardizing the data of {website_name}')

    # Read the config file
    config = read_config_file(
        os.path.join(AIRFLOW_DAGS_DIR, 'include', 'config', 'mapping',
                     f'{website_name}.json')
    )

    standardized_files_paths = {}
    input_files_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', 'input'
    )
    for file_type, file_type_config in config.items():
        logging.info(f'[INFO] Standardizing {file_type} data')
        # Read the file type - Orders, Transactions, etc.
        df = read_original_file(os.path.join(input_files_path,
                                             file_type_config['file_name']))
        # Skip the file if it's empty
        if df.empty:
            logging.info(
                f'[INFO] Skipping {website_name} - {file_type} data, the file is empty')
            raise AirflowSkipException(
                f'{website_name} - {file_type} data is empty, skipping')
        # Standardize the column names
        df = standardize_column_names(df, file_type_config['columns'])
        # Save the standardized file
        standardized_files_paths[file_type] = save_file(
            df, None, f"{website_name}_{file_type}.csv", 'temp')

    return {'website_name': website_name, 'files_paths': standardized_files_paths}
