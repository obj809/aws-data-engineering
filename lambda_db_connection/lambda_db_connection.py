# lambda_db_connection/lambda_db_connection.py

import json
import os
import pymysql  # Ensure this library is included in the deployment package
import logging
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def decimal_default(obj):
    """
    Helper function to convert Decimal objects to float or string.
    Modify as needed based on how you want to handle Decimals.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    logger.info("Starting DB connection test.")

    # Retrieve database connection details from environment variables
    db_host = os.getenv('DB_HOST')
    db_port = int(os.getenv('DB_PORT', '3306'))
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    # Validate environment variables
    missing_vars = []
    for var, value in [('DB_HOST', db_host), ('DB_NAME', db_name), ('DB_USER', db_user), ('DB_PASSWORD', db_password)]:
        if not value:
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return {
            "statusCode": 500,
            "body": f"Missing environment variables: {', '.join(missing_vars)}"
        }
    
    try:
        # Establish connection to the RDS MySQL instance
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=5
        )
        
        if connection.open:
            logger.info(f"Successfully connected to the database '{db_name}' at {db_host}:{db_port}.")
            # Optionally, perform a simple query to verify
            with connection.cursor() as cursor:
                cursor.execute("SELECT DATABASE();")
                result = cursor.fetchone()
                logger.info(f"Connected to database: {result[0]}")

            # Query the 'dams' table and log each entry
            logger.info("Querying the 'dams' table...")
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SELECT * FROM dams;")
                dams = cursor.fetchall()
                
                if dams:
                    logger.info(f"Retrieved {len(dams)} entries from the 'dams' table:")
                    for dam in dams:
                        # Convert Decimal objects to float for JSON serialization
                        dam_serializable = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in dam.items()}
                        logger.info(json.dumps(dam_serializable))
                else:
                    logger.info("No entries found in the 'dams' table.")
        
        connection.close()
        logger.info("Database connection closed.")
        
        return {
            "statusCode": 200,
            "body": json.dumps("Successfully connected to the database, retrieved dams data, and logged the connection.")
        }
    
    except Exception as e:
        logger.error(f"Failed to connect to the database or retrieve data. Exception: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Failed to connect to the database or retrieve data. Exception: {str(e)}"
        }
