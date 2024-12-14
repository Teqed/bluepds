# Bluesky PDS
This is a barebones implementation of a ATProto PDS using [Axum](https://github.com/tokio-rs/axum) and [Azure app services](https://learn.microsoft.com/en-us/azure/app-service/overview).

## Quick Start
```
cargo run
```

## Quick Deployment (Azure CLI)
```
az group create --name "webapp" --location southcentralus
az deployment group create --resource-group "webapp" --template-file .\deployment.bicep --parameters webAppName=testapp

az acr login --name <insert name of ACR resource here>
docker build -t <ACR>.azurecr.io/testapp:latest .
docker push <ACR>.azurecr.io/testapp:latest
```
