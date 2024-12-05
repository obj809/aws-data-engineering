# lambda_trigger/lambda_trigger.py

import boto3
import json
import os
import base64
import requests

def get_secrets():
    aws_region = os.getenv('CUSTOM_AWS_REGION', 'ap-southeast-2')
    secret_name = os.getenv('SECRET_NAME')
    print(f"Debug: AWS_REGION resolved to {aws_region}")
    print(f"Debug: SECRET_NAME resolved to {secret_name}")
    
    secrets_client = boto3.client('secretsmanager', region_name=aws_region)

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        print(f"Debug: Secrets Manager response: {response}")
        
        secret_data = json.loads(response['SecretString'])
        print("Debug: Parsed secret data successfully.")
        return secret_data
    except Exception as e:
        print(f"Error: Failed to fetch secrets. Exception: {e}")
        return None

def fetch_access_token(api_key, api_secret):
    """
    Fetch access token from WaterInsights API.
    """
    try:
        # Base URL for the API
        base_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"

        # Prepare headers
        credentials = f"{api_key}:{api_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Authorization": f"Basic {encoded_credentials}"
        }

        # Prepare query parameters
        params = {
            "grant_type": "client_credentials"
        }

        # Make the GET request to fetch the access token
        response = requests.get(base_url, headers=headers, params=params)

        # Log request and response details for debugging
        print(f"Request Headers: {headers}")
        print(f"Request Parameters: {params}")
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")

        # Check the response
        if response.status_code == 200:
            response_data = response.json()
            access_token = response_data.get("access_token")
            print(f"Access Token retrieved successfully: {access_token}")
            return access_token
        else:
            print(f"Failed to fetch access token. Status Code: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        print(f"Error during access token retrieval: {e}")
        return None

def trigger_eventbridge_event():
    """
    Triggers an EventBridge event.
    """
    try:
        event_bus_name = os.getenv('EVENT_BUS_NAME', 'default')
        aws_region = os.getenv('CUSTOM_AWS_REGION', 'ap-southeast-2')
        eventbridge_client = boto3.client('events', region_name=aws_region)
        
        response = eventbridge_client.put_events(
            Entries=[
                {
                    'EventBusName': event_bus_name,
                    'Source': 'lambda_trigger',
                    'DetailType': 'SecretUpdated',
                    'Detail': json.dumps({'message': 'Secret updated successfully'})
                }
            ]
        )
        print(f"EventBridge response: {response}")
        if response['FailedEntryCount'] > 0:
            print("Error: Failed to put event to EventBridge.")
            return False
        else:
            print("EventBridge event triggered successfully.")
            return True
    except Exception as e:
        print(f"Error triggering EventBridge event: {e}")
        return False

def update_secret_with_access_token(secret_name, aws_region, secret_data, access_token):
    """
    Updates the secret in AWS Secrets Manager with the new access token
    and triggers EventBridge if successful.
    """
    secrets_client = boto3.client('secretsmanager', region_name=aws_region)

    # Add 'ACCESS_TOKEN' to the secret
    secret_data['ACCESS_TOKEN'] = access_token

    try:
        # Update the secret in Secrets Manager
        secrets_client.put_secret_value(
            SecretId=secret_name,
            SecretString=json.dumps(secret_data)
        )
        print("ACCESS_TOKEN added to the secret successfully.")

        # Trigger EventBridge event
        trigger_eventbridge_event()

        return {
            'statusCode': 200,
            'body': 'ACCESS_TOKEN added to the secret successfully and EventBridge event triggered.'
        }
    except Exception as e:
        print(f"Error updating secret: {e}")
        return {
            'statusCode': 500,
            'body': f"Error updating secret: {e}"
        }

def lambda_handler(event, context):
    print("Lambda function started.")
    print(f"Debug: Event received: {event}")

    # Fetch secrets
    secret_data = get_secrets()
    if secret_data:
        print("Debug: Successfully retrieved secrets.")
        # Extract API_KEY and API_SECRET
        api_key = secret_data.get("API_KEY")
        api_secret = secret_data.get("API_SECRET")
        print(f"Debug: Extracted API_KEY: {api_key}, API_SECRET: {api_secret}")

        # Fetch the access token from the API
        access_token = fetch_access_token(api_key, api_secret)
        if access_token:
            print("Successfully retrieved access token from WaterInsights API.")

            # Write the access token to Secrets Manager and trigger EventBridge
            secret_name = os.getenv('SECRET_NAME')
            aws_region = os.getenv('CUSTOM_AWS_REGION', 'ap-southeast-2')
            update_result = update_secret_with_access_token(secret_name, aws_region, secret_data, access_token)
            return update_result
        else:
            print("Failed to retrieve access token.")
            return {
                'statusCode': 500,
                'body': 'Failed to retrieve access token.'
            }
    else:
        print("Error: Failed to fetch secrets.")
        return {
            'statusCode': 500,
            'body': 'Failed to fetch secrets.'
        }
