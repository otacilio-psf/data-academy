# Terraform init

`init_project.sh` will create the necessary service principal for Terraform and for Data lake contributor

```
export TF_VAR_PROJECT_NAME="<project-name>"
export TF_VAR_LOCATION="West Europe"

az logout
az login --use-device-code

source init_project.sh
```

## Reload .env if already exist
```
source .env 
```

## Delete SP
```
az ad sp delete --id $TF_VAR_SP_TERRAFORM_ID
```

# Terraform commands

```
terraform init

terraform plan

terraform apply

terraform destroy
```
