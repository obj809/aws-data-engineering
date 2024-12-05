# /scripts/verify_database_updates.py

import pymysql
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_recent_data_from_table(connection, table_name, limit=10):
    """
    Query the most recent rows from a given table.
    """
    query = f"SELECT * FROM {table_name} ORDER BY date DESC LIMIT %s"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            return rows
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return []

def main():
    """
    Main function to connect to the database and fetch recent data from both tables.
    """
    try:
        # Connect to the database
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print(f"Connected to the database '{DB_NAME}'.")

        # Fetch recent data from 'latest_data' table
        print("\nFetching recent data from 'latest_data' table...")
        latest_data = get_recent_data_from_table(connection, "latest_data")
        if latest_data:
            for row in latest_data:
                print(row)
        else:
            print("No data found in 'latest_data' table.")

        # Fetch recent data from 'dam_resources' table
        print("\nFetching recent data from 'dam_resources' table...")
        dam_resources_data = get_recent_data_from_table(connection, "dam_resources")
        if dam_resources_data:
            for row in dam_resources_data:
                print(row)
        else:
            print("No data found in 'dam_resources' table.")

    except Exception as e:
        print(f"Failed to connect to the database or fetch data: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
