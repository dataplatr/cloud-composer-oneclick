# Execute the Python script
python3 src/generate_var_file.py

# Check if the Python script execution was successful
if [ $? -ne 0 ]; then
    echo "Failed to generate variable file. Exiting..."
    exit 1
fi

# Initialize Terraform
terraform init -backend=true

# Check if the Terraform initialization was successful
if [ $? -ne 0 ]; then
    echo "Failed to initialize Terraform. Exiting..."
    exit 1
fi

# Run Terraform plan with variable file
terraform plan -var-file=variables.tfvars

# Review the plan and confirm if desired changes
echo "Please review the Terraform plan above."

# Apply Terraform changes (manual confirmation)
terraform apply -var-file=variables.tfvars

# Capture the exit status of the terraform apply command
exit_status=$?

# Check if the terraform apply command was successful
if [ $exit_status -eq 0 ]; then
    echo -e "\n✅ Cloud Composer environment creation successful."
else
    echo -e "\n❌ Cloud Composer environment creation failed or was cancelled."
    exit 1
fi

# Extract configuration values from JSON
project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectId']))" 2>/dev/null || echo "")
location=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['location']))" 2>/dev/null || echo "")
cloud_composer_env=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['composerEnvName']))" 2>/dev/null || echo "")

# Get the Composer environment details
env_details=$(gcloud composer environments describe $cloud_composer_env \
  --location $location \
  --project $project \
  --format json)

# Check if the gcloud command was successful
if [ $? -ne 0 ]; then
    echo "Failed to describe Composer environment. Exiting..."
    exit 1
fi

# Parse the environment details to get the bucket name
bucket_name=$(echo $env_details | jq -r '.config.dagGcsPrefix' | sed 's/^gs:\/\///' | sed 's/\/dags$//')

# Check if the bucket name was successfully parsed
if [ -z "$bucket_name" ]; then
    echo "Failed to parse the bucket name from the Composer environment details. Exiting..."
    exit 1
fi

# Output the bucket name
echo "Composer Bucket Name: $bucket_name"

# Copy Oracle DAG files and JSON schema to respective buckets
gsutil -m cp -r src/Dags/* gs://$bucket_name/dags/Dev

# Check if the gsutil command was successful
if [ $? -ne 0 ]; then
    echo "Failed to copy DAG files to the bucket. Exiting..."
    exit 1
fi

echo "DAG files successfully copied to gs://$bucket_name/dags."
