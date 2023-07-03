# Import the modules
from airflow.decorators import task
from airflow.models import Variable
from utils.file_utils import save_file


@task(
    task_id='merge_data',
    trigger_rule='all_success'
)
def merge_data(website_data):
    """
    Merge the data of a website
    @param website_data: The name of the website and the paths to its files
    @return: The name of the website and the path to the merged file
    """
    import logging
    import pandas as pd
    from functools import reduce

    ORDER_FILE_NAME = Variable.get('ORDER_FILE_NAME', 'orders')
    PRODUCT_FILE_NAME = Variable.get('PRODUCT_FILE_NAME', 'products')
    TRANSACTION_FILE_NAME = Variable.get(
        'TRANSACTION_FILE_NAME', 'transactions')

    def merge(file_paths):
        """
        Merge the files based on common columns
        @param file_paths: The paths to the files of a website
        """
        dfs = [pd.read_csv(file_path) for file_path in file_paths]
        df_merged = reduce(
            lambda left, right: pd.merge(left, right, how='left'),
            dfs
        )
        return df_merged

    website_name = website_data['website_name']
    files_paths = website_data['files_paths']

    logging.info(f'[INFO] Merging {website_name} data')

    dfs = [files_paths[TRANSACTION_FILE_NAME],
           files_paths[ORDER_FILE_NAME], files_paths[PRODUCT_FILE_NAME]]
    df_merged = merge(dfs)

    # save the data
    merged_file_path = save_file(
        df_merged, None, f'{website_name}_merged_data.csv', 'temp')

    return {'website_name': website_name, 'file_path': merged_file_path}
