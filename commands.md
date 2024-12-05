# COMMANDS

## VENV

python3 -m venv venv

source venv/bin/activate

pip freeze > requirements.txt

pip install -r requirements.txt


## Terraform

terraform -chdir=terraform plan

terraform -chdir=terraform apply

terraform -chdir=terraform destroy

## Zip Lambda functions


(cd lambda_test_request && zip -r ../zipped_lambda_functions/lambda_test_request.zip .)

(cd lambda_db_connection && zip -r ../zipped_lambda_functions/lambda_db_connection.zip .)



(cd lambda_trigger && zip -r ../zipped_lambda_functions/lambda_trigger.zip .)

(cd lambda_data_collection && zip -r ../zipped_lambda_functions/lambda_data_collection.zip .)

(cd lambda_load_rds_glue && zip -r ../zipped_lambda_functions/lambda_load_rds_glue.zip .)



## Trigger

python scripts/invoke_lambda_trigger.py

python scripts/invoke_lambda_test_request.py




# Dependency Installation

pip install requests -t lambda_test_request/


# List S3 Bucket Contents

python scripts/list_s3_contents.py


# Verify Database Updates

python scripts/verify_database_updates.py
