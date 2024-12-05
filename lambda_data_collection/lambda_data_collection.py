import boto3
import json
import os
import pymysql  # Ensure this library is included in the deployment package
import requests  # Ensure this library is included in the deployment package
import logging
from decimal import Decimal
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secrets():
    """
    Fetch secrets from AWS Secrets Manager.
    """
    aws_region = os.getenv('CUSTOM_AWS_REGION', 'ap-southeast-2')
    secret_name = os.getenv('SECRET_NAME')

    if not secret_name:
        logger.error("SECRET_NAME environment variable is not set.")
        return None

    logger.debug(f"AWS_REGION resolved to {aws_region}")
    logger.debug(f"SECRET_NAME resolved to {secret_name}")

    secrets_client = boto3.client('secretsmanager', region_name=aws_region)

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        logger.debug("Secrets Manager response received.")
        logger.debug(f"SecretString: {response.get('SecretString', 'No SecretString found')}")

        secret_data = json.loads(response['SecretString'])
        logger.debug("Parsed secret data successfully.")
        return secret_data
    except Exception as e:
        logger.error(f"Failed to fetch secrets. Exception: {e}")
        return None

def connect_to_database(db_host, db_port, db_name, db_user, db_password):
    """
    Establish a connection to the RDS MySQL instance.
    """
    try:
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=5
        )
        logger.info(f"Successfully connected to the database '{db_name}' at {db_host}:{db_port}.")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to the database. Exception: {e}")
        return None

def query_dams_table(connection):
    """
    Query the 'dams' table to retrieve all dam entries.
    """
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM dams;")
            dams = cursor.fetchall()
            
            if dams:
                logger.info(f"Retrieved {len(dams)} entries from the 'dams' table.")
                return dams
            else:
                logger.info("No entries found in the 'dams' table.")
                return []
    except Exception as e:
        logger.error(f"Error querying the 'dams' table. Exception: {e}")
        return []

def fetch_dam_resources(dam_id, headers, retries=3, delay=5):
    """
    Fetch the latest dam resources from the API for a specific dam_id with retry logic.
    """
    BASE_URL = "https://api.onegov.nsw.gov.au"
    ENDPOINT_TEMPLATE = "/waternsw-waterinsights/v1/dams/{dam_id}/resources/latest"

    for attempt in range(1, retries + 1):
        try:
            endpoint = ENDPOINT_TEMPLATE.format(dam_id=dam_id)
            url = BASE_URL + endpoint
            response = requests.get(url, headers=headers)
            status_code = response.status_code

            if status_code == 200:
                if attempt > 1:
                    logger.info(f"Successfully fetched resources for dam_id {dam_id} after {attempt - 1} retries.")
                return response.json()
            elif status_code == 204:
                logger.warning(f"No data available for dam_id {dam_id}. Retrying in {delay} seconds... (Attempt {attempt}/{retries})")
                time.sleep(delay)
            elif status_code == 408:
                logger.warning(f"Traffic limit exceeded for dam_id {dam_id}. Retrying in {delay} seconds... (Attempt {attempt}/{retries})")
                time.sleep(delay)
            elif status_code == 422:
                logger.error(f"Invalid dam_id {dam_id} or internal server error (status 422). Skipping...")
                return None
            else:
                logger.error(f"Error: Received status code {status_code} for dam_id {dam_id}")
                logger.error(f"Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while fetching resources for dam_id {dam_id}: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds... (Attempt {attempt}/{retries})")
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch resources for dam_id {dam_id} after {retries} attempts.")
                return None
    return None

def publish_to_sns(sns_client, sns_topic_arn, source, detail_type, detail):
    """
    Publish a structured message to SNS.
    """
    try:
        # Create a message structure to customize the email message
        message_structure = {
            "default": json.dumps(detail),
            "email": f"""
            Hello,

            This is a notification from your AWS Lambda function.

            Event Source: {source}
            Event Type: {detail_type}
            Details: {json.dumps(detail, indent=2)}

            Thank you,
            Your AWS Lambda Function
            """
        }

        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message_structure),
            Subject="Secret Updated Notification",
            MessageStructure='json'
        )
        logger.debug(f"SNS Publish Response: {json.dumps(response, indent=2)}")
        return response
    except Exception as e:
        logger.error(f"Failed to publish message to SNS. Exception: {e}")
        return None

def get_s3_client():
    """
    Initialize and return an S3 client.
    """
    return boto3.client('s3')

def upload_to_s3(s3_client, bucket_name, key, data):
    """
    Upload data to the specified S3 bucket.
    """
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data, default=decimal_default),
            ContentType='application/json'
        )
        logger.info(f"Successfully uploaded data to S3 bucket '{bucket_name}' with key '{key}'.")
    except Exception as e:
        logger.error(f"Failed to upload data to S3. Exception: {e}")

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    logger.info("Lambda lambda_data_collection started.")
    logger.info(f"Event received: {json.dumps(event, indent=2)}")

    # Access and log secrets
    secret_data = get_secrets()
    if not secret_data:
        logger.error("Failed to retrieve secrets.")
        return {
            "statusCode": 500,
            "body": "Failed to retrieve secrets."
        }

    logger.info("Successfully retrieved secrets:")
    for key, value in secret_data.items():
        logger.info(f"Secret Variable - {key}: {value}")

    # Extract API credentials from secrets
    API_KEY = secret_data.get("API_KEY")
    ACCESS_TOKEN = secret_data.get("ACCESS_TOKEN")

    if not API_KEY or not ACCESS_TOKEN:
        logger.error("API_KEY and/or ACCESS_TOKEN are missing in secrets.")
        return {
            "statusCode": 500,
            "body": "API_KEY and/or ACCESS_TOKEN are missing in secrets."
        }

    # Set up headers for API requests
    HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "apikey": API_KEY,
    }

    # Extract information from the event if necessary
    source = event.get('source', 'Unknown Source')
    detail_type = event.get('detail-type', 'Unknown DetailType')
    detail = event.get('detail', {})
    message_detail = detail.get('message', 'No details provided.')

    logger.debug(f"Event Source: {source}")
    logger.debug(f"Event DetailType: {detail_type}")
    logger.debug(f"Event Detail: {json.dumps(detail, indent=2)}")

    # Prepare default message for other protocols
    default_message = {
        "Source": source,
        "DetailType": detail_type,
        "Detail": detail
    }

    # Publish to SNS
    sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
    if not sns_topic_arn:
        logger.error("SNS_TOPIC_ARN environment variable is not set.")
        return {
            "statusCode": 500,
            "body": "SNS_TOPIC_ARN environment variable is not set."
        }

    sns_client = boto3.client('sns')

    sns_response = publish_to_sns(sns_client, sns_topic_arn, source, detail_type, default_message)
    if not sns_response:
        logger.error("Failed to publish SNS notification.")
        return {
            "statusCode": 500,
            "body": "Failed to publish SNS notification."
        }

    # Database Connection and Querying
    db_host = os.getenv('DB_HOST')
    db_port = int(os.getenv('DB_PORT', '3306'))
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    # Validate database environment variables
    missing_db_vars = []
    for var, value in [('DB_HOST', db_host), ('DB_NAME', db_name), ('DB_USER', db_user), ('DB_PASSWORD', db_password)]:
        if not value:
            missing_db_vars.append(var)

    if missing_db_vars:
        logger.error(f"Missing environment variables for DB connection: {', '.join(missing_db_vars)}")
        return {
            "statusCode": 500,
            "body": f"Missing environment variables for DB connection: {', '.join(missing_db_vars)}"
        }

    connection = connect_to_database(db_host, db_port, db_name, db_user, db_password)
    if not connection:
        logger.error("Database connection failed.")
        return {
            "statusCode": 500,
            "body": "Database connection failed."
        }

    dams = query_dams_table(connection)
    connection.close()
    logger.info("Database connection closed.")

    if not dams:
        logger.info("No dams to process.")
        return {
            "statusCode": 200,
            "body": "Lambda executed successfully, notification sent via SNS, and no dams to process."
        }

    total_dams_count = len(dams)  # Store the total number of dams
    all_dam_resources = []
    successful_requests_count = 0  # Counter for successful API requests

    for dam in dams:
        dam_id = dam.get("dam_id")
        if dam_id is None:
            logger.warning(f"Dam entry without 'dam_id': {dam}")
            continue

        logger.info(f"Fetching resources for dam_id {dam_id}...")
        dam_resources = fetch_dam_resources(dam_id, HEADERS)

        if dam_resources:
            successful_requests_count += 1  # Increment counter
            all_dam_resources.append(dam_resources)
            logger.info(f"Successfully fetched resources for dam_id {dam_id}: {json.dumps(dam_resources, indent=2)}")
        else:
            logger.warning(f"Failed to fetch resources for dam_id {dam_id}.")

    # Initialize S3 client
    s3_client = get_s3_client()
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    if not s3_bucket:
        logger.error("S3_BUCKET_NAME environment variable is not set.")
        return {
            "statusCode": 500,
            "body": "S3_BUCKET_NAME environment variable is not set."
        }

    # Use a fixed key to overwrite the data each time
    s3_key = "dam_resources.json"

    upload_to_s3(s3_client, s3_bucket, s3_key, all_dam_resources)

    # Optionally, you can store 'all_dam_resources' to a database or another service
    # For this example, we've uploaded the data to S3

    logger.info("All dam resources collected and uploaded to S3.")

    logger.info(f"Number of successful API requests: {successful_requests_count}")

    # Add the check here
    if successful_requests_count == total_dams_count:
        logger.info("All API requests were successful.")
    else:
        logger.warning(f"Number of successful API requests ({successful_requests_count}) does not match the total number of dams ({total_dams_count}).")

    return {
        "statusCode": 200,
        "body": f"Lambda executed successfully, notification sent via SNS, and {successful_requests_count} successful API requests made."
    }
