# Import modules
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException


@task(
    task_id='extract_websites_names_from_config_files_paths'
)
def get_website_names():
    """
    Get the names of all the websites from the config files paths
    @return: The names of all the websites
    """
    import os
    import logging

    logging.info('[INFO] Getting the names of all the websites')

    # Get the path to the airflow
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    # Get the path to the websites files
    config_dir_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'config', 'mapping'
    )
    # Get the names of the files in the websites files path
    websites_config_files = os.listdir(config_dir_path)

    # Skip if there are no config files
    if len(websites_config_files) == 0:
        logging.info('[INFO] No config files found')
        raise AirflowSkipException('No config files found')

    # Get the names of the websites
    websites_names = [os.path.basename(
        os.path.splitext(f)[0]) for f in websites_config_files]
    logging.info(
        f'[INFO] Found {len(websites_names)} websites - {websites_names}')
    return websites_names
