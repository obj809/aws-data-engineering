# lambda_load_rds_glue/lambda_load_rds_glue.py

import logging
import pymysql  # Ensure this library is included in the deployment package
import boto3
import os
import json

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def connect_to_rds():
    """
    Connect to the AWS RDS instance.
    """
    db_host = os.getenv('DB_HOST')
    db_port = int(os.getenv('DB_PORT', '3306'))
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    missing_vars = [
        var_name for var_name in ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        if not os.getenv(var_name)
    ]
    if missing_vars:
        logger.error(f"Missing environment variables for RDS connection: {', '.join(missing_vars)}")
        return None

    try:
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=5
        )
        logger.info(f"Successfully connected to the RDS database '{db_name}' at {db_host}:{db_port}.")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to the RDS database. Exception: {e}")
        return None

def fetch_data_from_s3(bucket_name, object_key):
    """
    Fetch the content of the S3 object.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        logger.info(f"Successfully fetched data from S3 bucket '{bucket_name}', object '{object_key}'.")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data from S3 bucket '{bucket_name}', object '{object_key}'. Exception: {e}")
        return None

def replace_latest_data(connection, data):
    """
    Replace all entries in the 'latest_data' table with the provided data.
    """
    try:
        with connection.cursor() as cursor:
            # Truncate the table
            cursor.execute("TRUNCATE TABLE latest_data;")
            logger.info("Successfully truncated the 'latest_data' table.")

            # Prepare insert query
            insert_query = """
                INSERT INTO latest_data (dam_id, dam_name, date, storage_volume, percentage_full, storage_inflow, storage_release)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            # Insert new data
            for record in data:
                dams = record.get('dams', [])
                for dam in dams:
                    for resource in dam.get('resources', []):
                        cursor.execute(insert_query, (
                            dam['dam_id'],
                            dam['dam_name'],
                            resource['date'],
                            resource['storage_volume'],
                            resource['percentage_full'],
                            resource['storage_inflow'],
                            resource['storage_release']
                        ))

            # Commit changes
            connection.commit()
            logger.info("Successfully replaced data in the 'latest_data' table.")
    except Exception as e:
        logger.error(f"Failed to replace data in the 'latest_data' table. Exception: {e}")
        raise

def insert_into_dam_resources(connection, data):
    """
    Insert data into the 'dam_resources' table without affecting existing records.
    """
    try:
        with connection.cursor() as cursor:
            # Prepare insert query
            insert_query = """
                INSERT INTO dam_resources (dam_id, date, storage_volume, percentage_full, storage_inflow, storage_release)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                storage_volume = VALUES(storage_volume),
                percentage_full = VALUES(percentage_full),
                storage_inflow = VALUES(storage_inflow),
                storage_release = VALUES(storage_release)
            """

            # Iterate through each record in the data
            for record in data:
                dams = record.get('dams', [])
                for dam in dams:
                    for resource in dam.get('resources', []):
                        cursor.execute(insert_query, (
                            dam['dam_id'],
                            resource['date'],
                            resource['storage_volume'],
                            resource['percentage_full'],
                            resource['storage_inflow'],
                            resource['storage_release']
                        ))

            # Commit changes
            connection.commit()
            logger.info("Successfully inserted data into the 'dam_resources' table.")
    except Exception as e:
        logger.error(f"Failed to insert data into the 'dam_resources' table. Exception: {e}")
        raise

def lambda_handler(event, context):
    """
    AWS Lambda handler function triggered by S3 events.
    """
    logger.info("Lambda lambda_load_rds_glue started.")
    logger.info(f"Event received: {event}")

    try:
        # Extract bucket name and object key from the event
        records = event['Records']
        for record in records:
            s3_info = record['s3']
            bucket_name = s3_info['bucket']['name']
            object_key = s3_info['object']['key']
            logger.info(f"Triggered by S3 bucket '{bucket_name}', object '{object_key}'.")

            # Fetch data from S3
            data = fetch_data_from_s3(bucket_name, object_key)
            if not data:
                logger.error("No data fetched from S3. Exiting Lambda execution.")
                return {
                    "statusCode": 500,
                    "body": "Failed to fetch data from S3."
                }

            # Connect to RDS
            connection = connect_to_rds()
            if not connection:
                logger.error("Database connection failed. Exiting Lambda execution.")
                return {
                    "statusCode": 500,
                    "body": "Failed to connect to the database."
                }

            try:
                # Replace latest_data table content
                replace_latest_data(connection, data)

                # Insert data into dam_resources table
                insert_into_dam_resources(connection, data)
            finally:
                connection.close()
                logger.info("RDS connection closed.")

    except Exception as e:
        logger.error(f"Unhandled exception in Lambda: {e}")
        return {
            "statusCode": 500,
            "body": f"Error processing S3 event: {e}"
        }

    logger.info(f"S3 bucket '{bucket_name}' has been successfully updated. Lambda function triggered as a result.")
    return {
        "statusCode": 200,
        "body": f"S3 bucket '{bucket_name}' has been successfully updated. Data replaced in 'latest_data' table and added to 'dam_resources' table."
    }
