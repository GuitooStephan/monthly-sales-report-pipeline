"""
This dag generates the previous months sales report for each product and updates the monthly sales report file and the database
The products data originates from our ecommerce parterns database
"""

# Importing modules
import pendulum
from airflow.decorators import dag, task_group
# Importing tasks
from tasks.extract_websites_names_from_files_paths import get_website_names
from tasks.clean_data import clean_website_data
from tasks.transform_data import transform_websites_data
from tasks.standardize_with_config import standardize_website_data
from tasks.load_monthly_data import load_final_data
from tasks.merge_data import merge_data
from tasks.filter_previous_months_data import filter_website_data

# Define the default arguments for the dag
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('Europe/Paris').add(days=-1),
    'provide_context': True,
    'catchup': False,
}


@dag(
    default_args=default_args,
    schedule_interval='@monthly',
    dag_id='generate_previous_months_sales_with_std_config'
)
def generate_products_previous_months_sales_with_std_config():
    """
    This dag generates the previous months sales report for each product and updates the monthly sales report file and the database
    The products data originates from our ecommerce parterns database
    The data has already been downloaded and is stored in ./include/data/input/*.csv
    The pipeline is as follows:
    1. Get the name of each website
    2. Standardize the data of each website - columns and file names using config files
    3. Join the data of each website
    4. Filter the data to keep only the previous 2 months
    5. Clean the data
    6. Generate the previous months sales report
    7. Update the monthly sales report to a database or a csv file
    """
    # Group tasks specific to each website
    @task_group(
        group_id='website_specific_tasks',
        prefix_group_id=False
    )
    def website_specific_tasks(website_name):
        """
        Group tasks specific to each website
        @param website_name: The name of the website
        """
        # Standardize the data of each website - columns and file names using config files
        standardize_data = standardize_website_data(website_name)
        # Filter the data to keep only the previous 2 months
        filter_data = filter_website_data(standardize_data)
        # Clean the data
        clean_data = clean_website_data(filter_data)
        # Merge the data
        merged_data = merge_data(clean_data)
        return merged_data

    # Get the names of the websites and apply the tasks specific to each website
    websites_data = website_specific_tasks.expand(
        website_name=get_website_names())
    # Transform the data of each website
    transform_data = transform_websites_data(websites_data)
    # Load the monthly sales report
    load_final_data(transform_data)


dag = generate_products_previous_months_sales_with_std_config()

if __name__ == "__main__":
    dag.test(execution_date=pendulum.today('Europe/Paris').add(days=-1))
