# Terraform init

```
export TF_VAR_PROJECT_NAME="<project-name>"
export TF_VAR_LOCATION="West Europe"

az logout
az login --use-device-code
```

## Create .env
```
echo export TF_VAR_PROJECT_NAME=$TF_VAR_PROJECT_NAME > .env
echo export TF_VAR_LOCATION='"'${TF_VAR_LOCATION}'"' >> .env
```

## Reload .env if already exist
```
source .env 
```

# Terraform commands

```
terraform init

terraform plan

terraform apply

terraform destroy
```
