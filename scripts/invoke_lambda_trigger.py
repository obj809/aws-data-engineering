# scripts/invoke_lambda_trigger.py

from dotenv import load_dotenv
import boto3
import os

# Load environment variables from the .env file at the project root
load_dotenv()

# Retrieve the AWS region from environment variables
AWS_REGION = os.getenv('AWS_REGION')

# Specify the Lambda function name directly in the script
LAMBDA_FUNCTION_NAME = 'lambda_trigger'

# Initialize the AWS Lambda client with the specified region
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def trigger_lambda():
    try:
        # Invoke the Lambda function without a payload
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType="RequestResponse"  # Wait for the function to execute
        )
        
        # Decode and print the response from the Lambda function
        print("Lambda function triggered successfully.")
        print("Response:", response['Payload'].read().decode("utf-8"))
    except Exception as e:
        print("Error triggering Lambda function:", e)

if __name__ == "__main__":
    trigger_lambda()
