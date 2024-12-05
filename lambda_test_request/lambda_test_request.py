# lambda_test_request/lambda_test_request.py

import requests
import json

def lambda_handler(event, context):
    try:
        response = requests.get("https://jsonplaceholder.typicode.com/posts/1")
        response_data = response.json()
        print("Response from JSONPlaceholder API:", json.dumps(response_data, indent=2))
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_data)
        }
    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            "statusCode": 500,
            "body": f"Error: {e}"
        }
