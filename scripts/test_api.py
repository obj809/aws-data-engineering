# scripts/test_api.py

from dotenv import load_dotenv
import os
import base64
import requests

# Load environment variables from the .env file at the project root
load_dotenv()

# Retrieve API_KEY and API_SECRET from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

def test_api():
    """
    Test the WaterInsights API by retrieving an access token using the API_KEY and API_SECRET.
    """
    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and/or API_SECRET not set in the .env file.")
        return

    try:
        # Base URL for the API
        base_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"

        # Prepare headers
        credentials = f"{API_KEY}:{API_SECRET}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Authorization": f"Basic {encoded_credentials}"
        }

        # Prepare query parameters
        params = {
            "grant_type": "client_credentials"
        }

        # Make the GET request to fetch the access token
        print("Sending request to API...")
        response = requests.get(base_url, headers=headers, params=params)

        # Log request and response details for debugging
        print(f"Request Headers: {headers}")
        print(f"Request Parameters: {params}")
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")

        # Handle response
        if response.status_code == 200:
            try:
                # Parse the response as JSON
                response_data = response.json()
                access_token = response_data.get("access_token")
                print(f"Access Token retrieved successfully: {access_token}")
            except ValueError:
                # Handle non-JSON response
                print("The API returned a non-JSON response. Raw response:")
                print(response.text)
        else:
            print(f"Failed to fetch access token. Status Code: {response.status_code}, Response: {response.text}")

    except Exception as e:
        print(f"Error during API call: {e}")

if __name__ == "__main__":
    test_api()
