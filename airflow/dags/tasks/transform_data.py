from airflow.decorators import task


@task(
    task_id='transform_data',
    trigger_rule='none_failed_or_skipped'
)
def transform_websites_data(websites_data):
    """
    Transform the data of all the websites
    The data is cleaned before being passed to this task
    The transformation includes:
    1. aggregate the data
    @param websites_data: The names and the paths to the cleaned files of all the websites
    @return: The paths to the transformed files
    """
    # Import modules for this task
    import os
    from functools import reduce
    import logging
    import pandas as pd

    # Get the path to the airflow project
    AIRFLOW_DAGS_DIR = os.path.dirname(os.path.dirname(__file__))

    def aggregate_data(df):
        """
        Aggregate the data of a website
        The aggregation is based on the columns names ean and the transaction month/year
        @params df: The dataframe to be aggregated
        """
        logging.info(
            f'[INFO] Aggregating the data with columns {",".join(df.columns.to_list())}')
        # aggregate the data by ean and transaction month/year and calculate the total revenue per month/year
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['transaction_month_year'] = df['transaction_date'].dt.strftime(
            '%Y-%m-01')
        df['transaction_month_year'] = pd.to_datetime(
            df['transaction_month_year']).dt.date
        df = df.groupby(['transaction_month_year', 'ean', 'price']).agg(
            {
                # The name won't be relevant for what we are trying to achieve - TO BE CONFIRMED
                # 'name': 'first',
                'quantity': 'sum'
            }
        ).reset_index()
        df['amount'] = df['price'] * df['quantity']
        # rename the columns
        df = df.rename(
            columns={
                'transaction_month_year': 'transaction_date',
            }
        )
        return df

    def concat_files(websites_data):
        """
        Concatenate the files of all the websites
        @param websites_data: The names and the paths to the cleaned files of all the websites
        """
        # Get the paths to the files of all the websites
        files_paths = [website_data['file_path']
                       for website_data in websites_data]
        # Concatenate the files
        concat_df = pd.concat([pd.read_csv(file_path)
                              for file_path in files_paths])
        return concat_df

    # transform the data of each website and concatenate the results
    concat_df = concat_files(websites_data)

    # aggregate the data
    agg_df = aggregate_data(concat_df)

    # save the data
    agg_data_path = os.path.join(
        AIRFLOW_DAGS_DIR, 'include', 'data', 'temp', 'merged_data.csv')
    agg_df.to_csv(agg_data_path, index=False)

    return agg_data_path
