# glue_scripts/latest_dam_data_etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date, lit
import sys
import logging
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta
import pymysql

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

def get_recent_data_from_table(connection, table_name, limit=10):
    """
    Query the most recent rows from a given table.
    """
    query = f"SELECT * FROM {table_name} ORDER BY date DESC LIMIT %s"
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            return rows
    except Exception as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        return []

def insert_or_update_specific_dam_analysis(connection, result_dict):
    """
    Insert or update the specific_dam_analysis table with the latest analysis results.
    """
    insert_query = """
        INSERT INTO specific_dam_analysis (
            dam_id, analysis_date,
            avg_storage_volume_12_months, avg_storage_volume_5_years, avg_storage_volume_20_years,
            avg_percentage_full_12_months, avg_percentage_full_5_years, avg_percentage_full_20_years,
            avg_storage_inflow_12_months, avg_storage_inflow_5_years, avg_storage_inflow_20_years,
            avg_storage_release_12_months, avg_storage_release_5_years, avg_storage_release_20_years
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_storage_volume_12_months = VALUES(avg_storage_volume_12_months),
            avg_storage_volume_5_years = VALUES(avg_storage_volume_5_years),
            avg_storage_volume_20_years = VALUES(avg_storage_volume_20_years),
            avg_percentage_full_12_months = VALUES(avg_percentage_full_12_months),
            avg_percentage_full_5_years = VALUES(avg_percentage_full_5_years),
            avg_percentage_full_20_years = VALUES(avg_percentage_full_20_years),
            avg_storage_inflow_12_months = VALUES(avg_storage_inflow_12_months),
            avg_storage_inflow_5_years = VALUES(avg_storage_inflow_5_years),
            avg_storage_inflow_20_years = VALUES(avg_storage_inflow_20_years),
            avg_storage_release_12_months = VALUES(avg_storage_release_12_months),
            avg_storage_release_5_years = VALUES(avg_storage_release_5_years),
            avg_storage_release_20_years = VALUES(avg_storage_release_20_years)
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    result_dict['dam_id'],
                    result_dict['analysis_date'],
                    result_dict.get('avg_storage_volume_12_months'),
                    result_dict.get('avg_storage_volume_5_years'),
                    result_dict.get('avg_storage_volume_20_years'),
                    result_dict.get('avg_percentage_full_12_months'),
                    result_dict.get('avg_percentage_full_5_years'),
                    result_dict.get('avg_percentage_full_20_years'),
                    result_dict.get('avg_storage_inflow_12_months'),
                    result_dict.get('avg_storage_inflow_5_years'),
                    result_dict.get('avg_storage_inflow_20_years'),
                    result_dict.get('avg_storage_release_12_months'),
                    result_dict.get('avg_storage_release_5_years'),
                    result_dict.get('avg_storage_release_20_years')
                )
            )
            connection.commit()
            logger.info("Successfully inserted/updated the analysis results in 'specific_dam_analysis' table.")
    except Exception as e:
        logger.error(f"Error inserting/updating data in 'specific_dam_analysis': {e}")
        raise

def compute_overall_averages(dam_data_df, current_date):
    """
    Compute overall averages across all dams for specified time periods.
    """
    logger.info("Computing overall averages across all dams.")
    
    # Define time periods
    date_12_months_ago = current_date - timedelta(days=365)
    date_5_years_ago = current_date - timedelta(days=5*365)
    date_20_years_ago = current_date - timedelta(days=20*365)
    
    # Filter data for each time period
    dam_data_12_months = dam_data_df.filter(col('date') >= lit(date_12_months_ago.strftime('%Y-%m-%d')))
    dam_data_5_years = dam_data_df.filter(col('date') >= lit(date_5_years_ago.strftime('%Y-%m-%d')))
    dam_data_20_years = dam_data_df.filter(col('date') >= lit(date_20_years_ago.strftime('%Y-%m-%d')))
    
    logger.info(f"Records for last 12 months: {dam_data_12_months.count()}")
    logger.info(f"Records for last 5 years: {dam_data_5_years.count()}")
    logger.info(f"Records for last 20 years: {dam_data_20_years.count()}")
    
    # Function to compute averages
    def compute_avg(df, period_name):
        logger.info(f"Computing averages for {period_name}")
        return df.agg(
            avg('storage_volume').alias(f'avg_storage_volume_{period_name}'),
            avg('percentage_full').alias(f'avg_percentage_full_{period_name}'),
            avg('storage_inflow').alias(f'avg_storage_inflow_{period_name}'),
            avg('storage_release').alias(f'avg_storage_release_{period_name}')
        ).collect()[0].asDict()
    
    # Compute averages
    avg_12_months = compute_avg(dam_data_12_months, '12_months')
    avg_5_years = compute_avg(dam_data_5_years, '5_years')
    avg_20_years = compute_avg(dam_data_20_years, '20_years')
    
    # Prepare overall result
    overall_result = {
        'analysis_date': current_date.strftime('%Y-%m-%d'),
        'avg_storage_volume_12_months': avg_12_months.get('avg_storage_volume_12_months'),
        'avg_storage_volume_5_years': avg_5_years.get('avg_storage_volume_5_years'),
        'avg_storage_volume_20_years': avg_20_years.get('avg_storage_volume_20_years'),
        'avg_percentage_full_12_months': avg_12_months.get('avg_percentage_full_12_months'),
        'avg_percentage_full_5_years': avg_5_years.get('avg_percentage_full_5_years'),
        'avg_percentage_full_20_years': avg_20_years.get('avg_percentage_full_20_years'),
        'avg_storage_inflow_12_months': avg_12_months.get('avg_storage_inflow_12_months'),
        'avg_storage_inflow_5_years': avg_5_years.get('avg_storage_inflow_5_years'),
        'avg_storage_inflow_20_years': avg_20_years.get('avg_storage_inflow_20_years'),
        'avg_storage_release_12_months': avg_12_months.get('avg_storage_release_12_months'),
        'avg_storage_release_5_years': avg_5_years.get('avg_storage_release_5_years'),
        'avg_storage_release_20_years': avg_20_years.get('avg_storage_release_20_years')
    }
    
    logger.info(f"Computed overall averages: {overall_result}")
    
    return overall_result

def insert_or_update_overall_dam_analysis(connection, overall_result):
    """
    Insert or update the overall_dam_analysis table with the computed overall averages.
    """
    insert_query = """
        INSERT INTO overall_dam_analysis (
            analysis_date,
            avg_storage_volume_12_months, avg_storage_volume_5_years, avg_storage_volume_20_years,
            avg_percentage_full_12_months, avg_percentage_full_5_years, avg_percentage_full_20_years,
            avg_storage_inflow_12_months, avg_storage_inflow_5_years, avg_storage_inflow_20_years,
            avg_storage_release_12_months, avg_storage_release_5_years, avg_storage_release_20_years
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_storage_volume_12_months = VALUES(avg_storage_volume_12_months),
            avg_storage_volume_5_years = VALUES(avg_storage_volume_5_years),
            avg_storage_volume_20_years = VALUES(avg_storage_volume_20_years),
            avg_percentage_full_12_months = VALUES(avg_percentage_full_12_months),
            avg_percentage_full_5_years = VALUES(avg_percentage_full_5_years),
            avg_percentage_full_20_years = VALUES(avg_percentage_full_20_years),
            avg_storage_inflow_12_months = VALUES(avg_storage_inflow_12_months),
            avg_storage_inflow_5_years = VALUES(avg_storage_inflow_5_years),
            avg_storage_inflow_20_years = VALUES(avg_storage_inflow_20_years),
            avg_storage_release_12_months = VALUES(avg_storage_release_12_months),
            avg_storage_release_5_years = VALUES(avg_storage_release_5_years),
            avg_storage_release_20_years = VALUES(avg_storage_release_20_years)
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    overall_result['analysis_date'],
                    overall_result.get('avg_storage_volume_12_months'),
                    overall_result.get('avg_storage_volume_5_years'),
                    overall_result.get('avg_storage_volume_20_years'),
                    overall_result.get('avg_percentage_full_12_months'),
                    overall_result.get('avg_percentage_full_5_years'),
                    overall_result.get('avg_percentage_full_20_years'),
                    overall_result.get('avg_storage_inflow_12_months'),
                    overall_result.get('avg_storage_inflow_5_years'),
                    overall_result.get('avg_storage_inflow_20_years'),
                    overall_result.get('avg_storage_release_12_months'),
                    overall_result.get('avg_storage_release_5_years'),
                    overall_result.get('avg_storage_release_20_years')
                )
            )
            connection.commit()
            logger.info("Successfully inserted/updated the analysis results in 'overall_dam_analysis' table.")
    except Exception as e:
        logger.error(f"Error inserting/updating data in 'overall_dam_analysis': {e}")
        raise

def main():
    """
    Main function to connect to the database, fetch recent data, perform analysis, and insert results.
    """
    logger.info("Starting Glue job 'latest_dam_data_etl'.")

    # Retrieve arguments
    args = getResolvedOptions(
        sys.argv,
        [
            'DB_HOST',
            'DB_PORT',
            'DB_NAME',
            'DB_USER',
            'DB_PASSWORD'
        ]
    )
    db_host = args['DB_HOST']
    db_port = args['DB_PORT']
    db_name = args['DB_NAME']
    db_user = args['DB_USER']
    db_password = args['DB_PASSWORD']
    dam_id = '203042'  # Toonumbar Dam

    jdbc_url = f"jdbc:mysql://{db_host}:{db_port}/{db_name}"

    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "com.mysql.jdbc.Driver"
    }

    # Initialize Spark session
    spark = SparkSession.builder.appName("latest_dam_data_etl").getOrCreate()
    logger.info("Spark session initialized.")

    current_date = datetime.now()

    try:
        # Connect to the database
        connection = pymysql.connect(
            host=db_host,
            port=int(db_port),
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info(f"Connected to the database '{db_name}'.")

        # Build the query for specific dam
        specific_query = f"(SELECT * FROM dam_resources WHERE dam_id = '{dam_id}' AND date >= '{(current_date - timedelta(days=20*365)).strftime('%Y-%m-%d')}') as dam_data"

        logger.info("Reading data from 'dam_resources' table for specific dam.")
        dam_data_df = spark.read.jdbc(
            url=jdbc_url,
            table=specific_query,
            properties=connection_properties
        )
        record_count = dam_data_df.count()
        logger.info(f"Data read successfully. Number of records fetched: {record_count}")

        # Ensure 'date' column is of date type
        dam_data_df = dam_data_df.withColumn('date', to_date(col('date')))
        logger.info("Converted 'date' column to date type.")

        # Filter data for each time period
        date_12_months_ago = current_date - timedelta(days=365)
        date_5_years_ago = current_date - timedelta(days=5*365)
        date_20_years_ago = current_date - timedelta(days=20*365)

        dam_data_12_months = dam_data_df.filter(col('date') >= lit(date_12_months_ago.strftime('%Y-%m-%d')))
        dam_data_5_years = dam_data_df.filter(col('date') >= lit(date_5_years_ago.strftime('%Y-%m-%d')))
        dam_data_20_years = dam_data_df.filter(col('date') >= lit(date_20_years_ago.strftime('%Y-%m-%d')))

        logger.info(f"Records for last 12 months: {dam_data_12_months.count()}")
        logger.info(f"Records for last 5 years: {dam_data_5_years.count()}")
        logger.info(f"Records for last 20 years: {dam_data_20_years.count()}")

        # Function to compute averages
        def compute_averages(df, period_name):
            logger.info(f"Computing averages for {period_name}.")
            avg_df = df.select(
                avg('storage_volume').alias(f'avg_storage_volume_{period_name}'),
                avg('percentage_full').alias(f'avg_percentage_full_{period_name}'),
                avg('storage_inflow').alias(f'avg_storage_inflow_{period_name}'),
                avg('storage_release').alias(f'avg_storage_release_{period_name}')
            )
            return avg_df

        # Compute averages for specific dam
        avg_12_months = compute_averages(dam_data_12_months, '12_months').collect()[0].asDict()
        avg_5_years = compute_averages(dam_data_5_years, '5_years').collect()[0].asDict()
        avg_20_years = compute_averages(dam_data_20_years, '20_years').collect()[0].asDict()

        # Prepare result dictionary for specific dam
        specific_result = {
            'dam_id': dam_id,
            'analysis_date': current_date.strftime('%Y-%m-%d'),
            **avg_12_months,
            **avg_5_years,
            **avg_20_years
        }

        logger.info(f"Computed specific dam averages: {specific_result}")

        # Insert or update specific_dam_analysis table
        insert_or_update_specific_dam_analysis(connection, specific_result)

        # Compute overall averages across all dams
        logger.info("\nStarting computation of overall dam averages.")
        overall_result = compute_overall_averages(dam_data_df, current_date)

        # Insert or update overall_dam_analysis table
        insert_or_update_overall_dam_analysis(connection, overall_result)

    except Exception as e:
        logger.error(f"An error occurred during the Glue job execution: {e}")
        sys.exit(1)
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            logger.info("Database connection closed.")
        spark.stop()
        logger.info("Spark session stopped.")
    
    logger.info("Glue job 'latest_dam_data_etl' completed successfully.")

if __name__ == "__main__":
    main()
