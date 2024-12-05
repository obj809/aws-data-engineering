# scripts/invoke_lambda_test_request.py

from dotenv import load_dotenv
import boto3
import os

# Load environment variables from the .env file
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
LAMBDA_FUNCTION_NAME = "lambda_test_request"

lambda_client = boto3.client("lambda", region_name=AWS_REGION)

def trigger_lambda():
    try:
        response = lambda_client.invoke(FunctionName=LAMBDA_FUNCTION_NAME)
        print("Lambda function triggered successfully.")
        print("Response:", response['Payload'].read().decode("utf-8"))
    except Exception as e:
        print("Error triggering Lambda function:", e)

if __name__ == "__main__":
    trigger_lambda()
