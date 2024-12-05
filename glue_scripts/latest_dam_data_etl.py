# glue_scripts/latest_dam_data_etl.py

import sys
import logging

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    logger.info("AWS Glue job 'latest_dam_data_etl' has been triggered successfully.")

if __name__ == "__main__":
    main()
