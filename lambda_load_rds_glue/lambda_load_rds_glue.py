# lambda_load_rds_glue/lambda_load_rds_glue.py

import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda handler function triggered by S3 events.
    """
    logger.info("Lambda lambda_load_rds_glue started.")
    logger.info(f"Event received: {event}")

    # Extract bucket name and object key from the event
    try:
        records = event['Records']
        for record in records:
            s3_info = record['s3']
            bucket_name = s3_info['bucket']['name']
            object_key = s3_info['object']['key']
            logger.info(f"Triggered by S3 bucket '{bucket_name}', object '{object_key}'.")
    except Exception as e:
        logger.error(f"Error processing S3 event: {e}")
        return {
            "statusCode": 500,
            "body": f"Error processing S3 event: {e}"
        }

    # Log success message
    logger.info(f"S3 bucket '{bucket_name}' has been successfully updated. Lambda function triggered as a result.")

    return {
        "statusCode": 200,
        "body": f"S3 bucket '{bucket_name}' has been successfully updated. Lambda function triggered as a result."
    }
