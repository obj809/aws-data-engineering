# scripts/list_s3_contents.py

import boto3
import os
import json
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_s3_client():
    """
    Initialize and return an S3 client.
    """
    try:
        s3_client = boto3.client('s3')
        logger.info("Successfully initialized S3 client.")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client. Exception: {e}")
        raise

def list_s3_objects(s3_client, bucket_name):
    """
    List all objects in the specified S3 bucket.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        objects = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj['Key'])
        
        if not objects:
            logger.info(f"No objects found in bucket '{bucket_name}'.")
        else:
            logger.info(f"Found {len(objects)} objects in bucket '{bucket_name}'.")
        
        return objects
    except ClientError as e:
        logger.error(f"Failed to list objects in bucket '{bucket_name}'. Exception: {e}")
        raise

def get_object_content(s3_client, bucket_name, object_key):
    """
    Retrieve and return the content of the specified S3 object.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        logger.info(f"Successfully retrieved content for object '{object_key}'.")
        return content
    except ClientError as e:
        logger.error(f"Failed to retrieve object '{object_key}' from bucket '{bucket_name}'. Exception: {e}")
        return None

def main():
    """
    Main function to list and print contents of all objects in the S3 bucket.
    """
    # Retrieve the S3 bucket name from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not bucket_name:
        logger.error("Environment variable 'S3_BUCKET_NAME' is not set.")
        print("Please set the 'S3_BUCKET_NAME' environment variable and try again.")
        return
    
    # Initialize S3 client
    try:
        s3_client = get_s3_client()
    except Exception:
        print("Failed to initialize S3 client. Check logs for more details.")
        return
    
    # List objects in the bucket
    try:
        objects = list_s3_objects(s3_client, bucket_name)
    except Exception:
        print("Failed to list objects in the S3 bucket. Check logs for more details.")
        return
    
    if not objects:
        print(f"No objects found in bucket '{bucket_name}'.")
        return
    
    # Retrieve and print content of each object
    for obj_key in objects:
        print(f"\n--- Content of '{obj_key}' ---")
        content = get_object_content(s3_client, bucket_name, obj_key)
        if content:
            try:
                # Attempt to pretty-print JSON content if applicable
                parsed_content = json.loads(content)
                pretty_content = json.dumps(parsed_content, indent=2)
                print(pretty_content)
            except json.JSONDecodeError:
                # If not JSON, print as plain text
                print(content)
        else:
            print("Failed to retrieve content.")

if __name__ == "__main__":
    main()
