# /scripts/verify_database_updates.py

import pymysql
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            return rows
    except Exception as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        return []

def verify_specific_dam_analysis(connection, dam_id):
    """
    Verify the latest entry in the specific_dam_analysis table for a given dam_id.
    """
    query = f"""
        SELECT * FROM specific_dam_analysis
        WHERE dam_id = %s
        ORDER BY analysis_date DESC
        LIMIT 1
    """
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, (dam_id,))
            row = cursor.fetchone()
            if row:
                logger.info(f"Latest entry in 'specific_dam_analysis' for dam_id {dam_id}:")
                logger.info(row)
                return row
            else:
                logger.warning(f"No entries found in 'specific_dam_analysis' for dam_id {dam_id}.")
                return None
    except Exception as e:
        logger.error(f"Error verifying data in specific_dam_analysis: {e}")
        return None

def main():
    """
    Main function to connect to the database and fetch recent data from tables.
    """
    try:
        # Connect to the database
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info(f"Connected to the database '{DB_NAME}'.")

        # Define the dam_id to verify in specific_dam_analysis
        dam_id = '203042'  # Toonumbar Dam

        # Fetch recent data from 'latest_data' table
        logger.info("\nFetching recent data from 'latest_data' table...")
        latest_data = get_recent_data_from_table(connection, "latest_data")
        if latest_data:
            for row in latest_data:
                logger.info(row)
        else:
            logger.warning("No data found in 'latest_data' table.")

        # Fetch recent data from 'dam_resources' table
        logger.info("\nFetching recent data from 'dam_resources' table...")
        dam_resources_data = get_recent_data_from_table(connection, "dam_resources")
        if dam_resources_data:
            for row in dam_resources_data:
                logger.info(row)
        else:
            logger.warning("No data found in 'dam_resources' table.")

        # Verify data in 'specific_dam_analysis' table
        logger.info("\nVerifying data in 'specific_dam_analysis' table...")
        specific_dam_analysis = verify_specific_dam_analysis(connection, dam_id)
        if specific_dam_analysis:
            # Additional verification can be added here if needed
            logger.info("Verification of 'specific_dam_analysis' table completed successfully.")
        else:
            logger.warning("Verification of 'specific_dam_analysis' table failed or no data found.")

    except Exception as e:
        logger.error(f"Failed to connect to the database or fetch data: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
