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

def main():
    logger.info("Starting Glue job 'latest_dam_data_etl'")

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
    logger.info("Spark session initialized")

    current_date = datetime.now()
    date_12_months_ago = current_date - timedelta(days=365)
    date_5_years_ago = current_date - timedelta(days=5*365)
    date_20_years_ago = current_date - timedelta(days=20*365)

    # Build the query
    query = f"(SELECT * FROM dam_resources WHERE dam_id = '{dam_id}' AND date >= '{date_20_years_ago.strftime('%Y-%m-%d')}') as dam_data"

    logger.info("Reading data from dam_resources table")
    dam_data_df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_properties
    )
    logger.info(f"Data read successfully. Number of records fetched: {dam_data_df.count()}")

    # Ensure 'date' column is of date type
    dam_data_df = dam_data_df.withColumn('date', to_date(col('date')))
    logger.info("Date column converted to date type")

    # Filter data for each time period
    date_12_months_ago_str = date_12_months_ago.strftime('%Y-%m-%d')
    date_5_years_ago_str = date_5_years_ago.strftime('%Y-%m-%d')

    dam_data_12_months = dam_data_df.filter(col('date') >= lit(date_12_months_ago_str))
    dam_data_5_years = dam_data_df.filter(col('date') >= lit(date_5_years_ago_str))

    logger.info(f"Records for last 12 months: {dam_data_12_months.count()}")
    logger.info(f"Records for last 5 years: {dam_data_5_years.count()}")

    # Function to compute averages
    def compute_averages(df, period_name):
        logger.info(f"Computing averages for {period_name}")
        avg_df = df.select(
            avg('storage_volume').alias(f'avg_storage_volume_{period_name}'),
            avg('percentage_full').alias(f'avg_percentage_full_{period_name}'),
            avg('storage_inflow').alias(f'avg_storage_inflow_{period_name}'),
            avg('storage_release').alias(f'avg_storage_release_{period_name}')
        )
        return avg_df

    # Compute averages for each time period
    avg_12_months = compute_averages(dam_data_12_months, '12_months')
    avg_5_years = compute_averages(dam_data_5_years, '5_years')
    avg_20_years = compute_averages(dam_data_df, '20_years')

    # Collect results
    avg_12_months_collect = avg_12_months.collect()[0].asDict()
    avg_5_years_collect = avg_5_years.collect()[0].asDict()
    avg_20_years_collect = avg_20_years.collect()[0].asDict()

    result_dict = {
        'dam_id': dam_id,
        'analysis_date': current_date.strftime('%Y-%m-%d'),
        **avg_12_months_collect,
        **avg_5_years_collect,
        **avg_20_years_collect
    }

    logger.info(f"Computed averages: {result_dict}")

    # Insert or update the record in specific_dam_analysis table
    logger.info("Connecting to RDS database to insert results")
    try:
        connection = pymysql.connect(
            host=db_host,
            port=int(db_port),
            user=db_user,
            password=db_password,
            database=db_name
        )

        with connection.cursor() as cursor:
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
            logger.info("Successfully inserted/updated the analysis results in specific_dam_analysis table")
    except Exception as e:
        logger.error(f"Error inserting data into specific_dam_analysis: {e}")
        raise
    finally:
        connection.close()
        logger.info("RDS connection closed")

    logger.info("Glue job completed successfully")

if __name__ == "__main__":
    main()
