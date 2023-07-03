# Import modules
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException


@task(
    task_id='extract_websites_names_from_files_paths'
)
def get_website_names():
    """
    Get the names of all the websites from the files paths
    """
    import os
    import logging

    logging.info('[INFO] Getting the names of all the websites')

    # Get the path to the airflow
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))
    # Get the path to the websites files
    websites_files_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', 'input'
    )
    # Get the names of the files in the websites files path
    websites_files = os.listdir(websites_files_path)

    # Skip if there are no files
    if len(websites_files) == 0:
        logging.info('[INFO] No files found')
        raise AirflowSkipException('No files found')

    # Get the names of the websites
    websites_names = list(set([f.split('_')[0] for f in websites_files]))
    logging.info(f'[INFO] Found {len(websites_names)} websites')
    return websites_names
