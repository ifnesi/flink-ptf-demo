#!/bin/bash
set -e

echo "=========================================="
echo "Flink PTF Demo - Teardown Script"
echo "=========================================="
echo ""

# Check if terraform directory exists
if [ ! -d "terraform" ]; then
    echo "❌ Error: terraform directory not found!"
    exit 1
fi

# Check if .env exists
if [ ! -f "terraform/.env" ]; then
    echo "❌ Error: terraform/.env not found!"
    echo "Cannot destroy resources without Confluent Cloud credentials."
    exit 1
fi

echo "⚠️  This will destroy all Confluent Cloud resources created by this demo."
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Teardown cancelled."
    exit 0
fi

echo ""
echo "Destroying Confluent Cloud resources..."
cd terraform
source .env

terraform destroy -auto-approve

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ All resources destroyed successfully"
    echo "=========================================="
    echo ""
    echo "Note: The following local files remain:"
    echo "  - flink-ptf/target/ (JAR build artifacts)"
    echo "  - backend/.venv/ (Python virtual environment)"
    echo "  - backend/.env (generated credentials - now invalid)"
    echo "  - terraform/.terraform/ (Terraform state)"
    echo ""
    echo "To clean up local files, run:"
    echo "  rm -rf flink-ptf/target backend/.venv backend/.env terraform/.terraform terraform/.terraform.lock.hcl terraform/terraform.tfstate*"
else
    echo ""
    echo "❌ Terraform destroy failed!"
    echo "Please check the error messages above and try again."
    exit 1
fi
