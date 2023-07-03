# Import modules
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from utils.file_utils import save_file


@task(
    task_id='filter_previous_months_data',
    trigger_rule='all_success'
)
def filter_website_data(website_data):
    """
    Filter the data of the two previous months for each website
    @param website_data: The name of the website and the paths to the website data
    @return: The name of the website and the path to the filtered data
    """

    # Import modules for this task
    import pendulum
    import logging
    import pandas as pd

    TRANSACTION_FILE_NAME = Variable.get(
        'TRANSACTION_FILE_NAME', 'transactions')

    def filter_data(data, two_months_ago_date):
        """
        Filter the transactions of the two previous months
        @param data: The dataframe to be filtered
        @param two_months_ago_date: The date of two months ago
        @return: The filtered dataframe
        """
        # Filter the data
        data = data[data['transaction_date'].notnull()]
        # Convert the transaction date to date
        data['transaction_date'] = pd.to_datetime(
            data['transaction_date']).dt.date
        data = data[data['transaction_date'] >= two_months_ago_date.date()]
        return data

    website_name = website_data['website_name']
    website_files_paths = website_data['files_paths']

    logging.info(f'[INFO] Filtering {website_name} data')

    # Get transaction data
    data = pd.read_csv(website_files_paths[TRANSACTION_FILE_NAME])

    # Get the execution date
    context = get_current_context()
    execution_date = context['execution_date']
    # Two months ago
    two_months_ago = execution_date - pendulum.duration(months=2)
    logging.info(
        f'[INFO] Filtering data for {website_name} from {two_months_ago} to {execution_date}')
    # Filter the data
    data = filter_data(data, two_months_ago)

    # If there is no data for the last two months, skip the task
    if len(data) == 0:
        logging.info(
            f'[INFO] No data for {website_name} in the last two months - skipping')
        raise AirflowSkipException(
            f'No data for {website_name} in the last two months')

    # Save the data
    save_file(data, website_files_paths[TRANSACTION_FILE_NAME])

    return {'website_name': website_name, 'files_paths': website_files_paths}
