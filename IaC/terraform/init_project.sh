echo Terraform auth

export TF_VAR_SUBSCRIPTION_ID=$(az account list | jq -r '.[0].id')
export TF_VAR_TENANT_ID=$(az account list | jq -r '.[0].tenantId')

az account set --subscription $TF_VAR_SUBSCRIPTION_ID

SP_TERRAFORM_INFO=$(az ad sp create-for-rbac --name "${TF_VAR_PROJECT_NAME}-terraform-contributor" --role Owner --scopes /subscriptions/$TF_VAR_SUBSCRIPTION_ID)

export TF_VAR_SP_TERRAFORM_ID=$(echo $SP_TERRAFORM_INFO | jq -r ".appId")

export TF_VAR_SP_TERRAFORM_SECRET=$(echo $SP_TERRAFORM_INFO | jq -r ".password")

echo Create .env

echo export TF_VAR_PROJECT_NAME=$TF_VAR_PROJECT_NAME > .env
echo export TF_VAR_LOCATION='"'${TF_VAR_LOCATION}'"' >> .env
echo export TF_VAR_SUBSCRIPTION_ID=$TF_VAR_SUBSCRIPTION_ID >> .env
echo export TF_VAR_TENANT_ID=$TF_VAR_TENANT_ID >> .env
echo export TF_VAR_SP_TERRAFORM_ID=$TF_VAR_SP_TERRAFORM_ID >> .env
echo export TF_VAR_SP_TERRAFORM_SECRET=$TF_VAR_SP_TERRAFORM_SECRET >> .env