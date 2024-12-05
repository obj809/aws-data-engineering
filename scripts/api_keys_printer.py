# scripts/api_keys_printer.py

from dotenv import load_dotenv
import os

# Load environment variables from .env file at the project root
load_dotenv()

# Fetch API_KEY and API_SECRET from the loaded environment variables
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

# Print API_KEY and API_SECRET
print("API_KEY:", API_KEY)
print("API_SECRET:", API_SECRET)
