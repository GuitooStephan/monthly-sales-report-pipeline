"""
This dag generates a monthly sales report for each product and loads it to a database or a csv file
The products data originates from our ecommerce parterns database
"""

# Importing modules
import pendulum
from airflow.decorators import dag, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
# Importing tasks
from tasks.extract_websites_names_from_files_paths import get_website_names
from tasks.clean_data import clean_website_data
from tasks.transform_data import transform_websites_data
from tasks.standardize_with_logic import standardize_website_data
from tasks.load_monthly_data import load_final_data
from tasks.merge_data import merge_data

# Define the default arguments for the dag
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('Europe/Paris').add(days=-1),
    'provide_context': True,
    'catchup': False,
}


@dag(
    default_args=default_args,
    schedule_interval='@once',
    dag_id='generate_products_monthly_sales_with_std_rules'
)
def generate_products_monthly_sales_with_std_rules():
    """
    This dag generates a monthly sales report for each product and loads it to a database or a csv file
    The products data originates from our ecommerce parterns database
    The data has already been downloaded and is stored in ./include/data/input/*.csv
    The pipeline is as follows:
    1. Get the name of each website
    2. Standardize the data of each website - columns and file names using rules
    3. Join the data of each website
    4. Clean the data
    5. Generate the monthly sales report
    6. Load the monthly sales report to a database or a csv file
    """

    # Create the table to store the monthly sales report
    create_monthly_sales_report_table = PostgresOperator(
        task_id='create_monthly_sales_report_table',
        postgres_conn_id='postgres_db',
        sql='sql/create_monthly_sales_report_table.sql'
    )

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
        # Standardize the data of each website - columns and file names using rules
        standardize_data = standardize_website_data(website_name)
        # Clean the data
        clean_data = clean_website_data(standardize_data)
        # Join the data of each website
        merged_data = merge_data(clean_data)
        return merged_data

    # Get the names of the websites and apply the tasks specific to each website
    websites_data = website_specific_tasks.expand(
        website_name=get_website_names())
    # Transform the data of each website
    transform_data = transform_websites_data(websites_data)
    # Create the monthly sales report table
    transform_data >> create_monthly_sales_report_table
    # Load the monthly sales report
    create_monthly_sales_report_table >> load_final_data(transform_data)


dag = generate_products_monthly_sales_with_std_rules()

if __name__ == "__main__":
    dag.test(execution_date=pendulum.today('Europe/Paris').add(days=-1))
