# Bluesky PDS
This is a barebones implementation of a ATProto PDS using [Axum](https://github.com/tokio-rs/axum) and [Azure app services](https://learn.microsoft.com/en-us/azure/app-service/overview).

## To-do
- [ ] UG /xrpc/_health (undocumented, but impl by reference PDS)
- com.atproto.identity
    - [ ] UG /xrpc/com.atproto.identity.resolveHandle
    - [ ] UP /xrpc/com.atproto.identity.updateHandle
- com.atproto.server
    - [X] UG /xrpc/com.atproto.server.describeServer
    - [X] UP /xrpc/com.atproto.server.createAccount
    - [X] AP /xrpc/com.atproto.server.createInviteCode
    - [X] UP /xrpc/com.atproto.server.createSession
    - [X] AG /xrpc/com.atproto.server.getSession
- com.atproto.repo
    - [ ] AP /xrpc/com.atproto.repo.applyWrites
    - [X] AP /xrpc/com.atproto.repo.createRecord
    - [ ] AP /xrpc/com.atproto.repo.putRecord
    - [ ] AP /xrpc/com.atproto.repo.deleteRecord
    - [ ] UG /xrpc/com.atproto.repo.describeRepo
    - [ ] UG /xrpc/com.atproto.repo.getRecord
    - [ ] UG /xrpc/com.atproto.repo.listRecords
    - [ ] AP /xrpc/com.atproto.repo.uploadBlob
- com.atproto.sync
    - [X] UG /xrpc/com.atproto.sync.getBlob
    - [ ] UG /xrpc/com.atproto.sync.getBlocks
    - [X] UG /xrpc/com.atproto.sync.getLatestCommit
    - [X] UG /xrpc/com.atproto.sync.getRecord
    - [ ] UG /xrpc/com.atproto.sync.getRepoStatus
    - [ ] UG /xrpc/com.atproto.sync.getRepo
    - [ ] UG /xrpc/com.atproto.sync.listBlobs
    - [X] UG /xrpc/com.atproto.sync.listRepos
    - [X] UG /xrpc/com.atproto.sync.subscribeRepos

## Quick Start
```
# Install sqlx CLI at https://github.com/launchbadge/sqlx/tree/main/sqlx-cli
cargo sqlx database setup
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
