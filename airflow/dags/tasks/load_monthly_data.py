# Import modules
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from utils.file_utils import save_file


@task(
    task_id='load_monthly_data',
    trigger_rule='all_success'
)
def load_final_data(file_path):
    """
    Load product monthly data to an output file
    @param file_path: The path to the cleaned files
    @return: The path to the monthly data
    """

    # Import modules for this task
    import os
    import logging
    from glob import glob
    import pandas as pd

    FINAL_DATA_TABLE_NAME = Variable.get(
        'FINAL_DATA_TABLE_NAME', 'products_monthly_sales')
    # Get the path to the airflow project
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    def clean_final_data(data):
        """
        Clean the final data
        @param data: The final data
        @return: The cleaned final data
        """
        data.dropna(subset=['amount'], inplace=True)
        return data

    def clean_temp_folder(dir_path):
        """
        Clean the temp folder
        """
        logging.info('[INFO] Cleaning the temp folder')
        files = glob(f'{dir_path}/*')
        for f in files:
            os.remove(f)

    def save_to_database(data, hook):
        """
        Save the final data to a database
        @param data: The final data
        @param hook: The postgresql hook
        """
        logging.info(
            f'[INFO] Saving the final data to a database - {len(data)} rows')
        hook.insert_rows(
            FINAL_DATA_TABLE_NAME, list(
                data.itertuples(index=False, name=None)),
            replace=True, replace_index=['transaction_date', 'ean', 'price'],
            target_fields=['transaction_date', 'ean',
                           'price', 'quantity', 'amount']
        )

    def save_monthly_data(data, monthly_data_path):
        """
        Save the monthly data to a file
        @param data: The monthly data,
        @param monthly_data_path: The path to the monthly data
        @return: The path to the monthly data
        """
        logging.info('[INFO] Saving the monthly data')
        postgresql_hook = PostgresHook(
            postgres_conn_id='postgres_db')
        # Save the data to a database
        save_to_database(data, postgresql_hook)
        # Save the data to a file
        monthly_data = postgresql_hook.get_pandas_df(
            f'SELECT * FROM {FINAL_DATA_TABLE_NAME}')
        monthly_data_path = save_file(monthly_data, monthly_data_path)

    logging.info('[INFO] Loading the final data')

    # Get the path to the monthly data
    monthly_data_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', 'output', f'products_monthly_sales.csv')

    # Load the data
    data = pd.read_csv(file_path)
    # Clean the data
    data = clean_final_data(data)

    # Save the monthly data
    save_monthly_data(data, monthly_data_path)

    # Clean the temp folder
    clean_temp_folder(os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', 'temp'))

    return monthly_data_path
