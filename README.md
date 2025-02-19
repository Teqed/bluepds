# Bluesky PDS
```
         __                         __
        /\ \__                     /\ \__
    __  \ \ ,_\  _____   _ __   ___\ \ ,_\   ___
  /'__'\ \ \ \/ /\ '__'\/\''__\/ __'\ \ \/  / __'\
 /\ \L\.\_\ \ \_\ \ \L\ \ \ \//\ \L\ \ \ \_/\ \L\ \
 \ \__/.\_\\ \__\\ \ ,__/\ \_\\ \____/\ \__\ \____/
  \/__/\/_/ \/__/ \ \ \/  \/_/ \/___/  \/__/\/___/
                   \ \_\
                    \/_/
```

This is an implementation of an ATProto PDS using [Axum](https://github.com/tokio-rs/axum) and [Azure app services](https://learn.microsoft.com/en-us/azure/app-service/overview).

This PDS implementation uses a SQLite database to store private account information and file storage to store canonical user data.

## Quick Start
```
# Install sqlx CLI at https://github.com/launchbadge/sqlx/tree/main/sqlx-cli
cargo sqlx database setup
cargo run
```

## Code map
```
* migrations/   - SQLite database migrations
* src/
  * endpoints/  - ATProto API endpoints
  * auth.rs     - Authentication primitives
  * config.rs   - Application configuration
  * did.rs      - Decentralized Identifier helpers
  * error.rs    - Axum error helpers
  * firehose.rs - ATProto firehose producer
  * main.rs     - Main entrypoint
  * plc.rs      - Functionality to access the Public Ledger of Credentials
  * storage.rs  - Helpers to access user repository storage
```

## To-do
- [X] [Service proxying](https://atproto.com/specs/xrpc#service-proxying)
- [X] UG /xrpc/_health (undocumented, but impl by reference PDS)
- com.atproto.identity
    - [X] UG /xrpc/com.atproto.identity.resolveHandle
    - [ ] AP /xrpc/com.atproto.identity.updateHandle
- com.atproto.server
    - [X] UG /xrpc/com.atproto.server.describeServer
    - [X] UP /xrpc/com.atproto.server.createAccount
    - [X] AP /xrpc/com.atproto.server.createInviteCode
    - [X] UP /xrpc/com.atproto.server.createSession
    - [X] AG /xrpc/com.atproto.server.getSession
- com.atproto.repo
    - [X] AP /xrpc/com.atproto.repo.applyWrites
    - [X] AP /xrpc/com.atproto.repo.createRecord
    - [X] AP /xrpc/com.atproto.repo.putRecord
    - [X] AP /xrpc/com.atproto.repo.deleteRecord
    - [X] UG /xrpc/com.atproto.repo.describeRepo
    - [X] UG /xrpc/com.atproto.repo.getRecord
    - [X] UG /xrpc/com.atproto.repo.listRecords
    - [ ] AP /xrpc/com.atproto.repo.uploadBlob
- com.atproto.sync
    - [ ] UG /xrpc/com.atproto.sync.getBlob
    - [X] UG /xrpc/com.atproto.sync.getBlocks
    - [X] UG /xrpc/com.atproto.sync.getLatestCommit
    - [X] UG /xrpc/com.atproto.sync.getRecord
    - [X] UG /xrpc/com.atproto.sync.getRepoStatus
    - [X] UG /xrpc/com.atproto.sync.getRepo
    - [ ] UG /xrpc/com.atproto.sync.listBlobs
    - [X] UG /xrpc/com.atproto.sync.listRepos
    - [X] UG /xrpc/com.atproto.sync.subscribeRepos

## Quick Deployment (Azure CLI)
```
az group create --name "webapp" --location southcentralus
az deployment group create --resource-group "webapp" --template-file .\deployment.bicep --parameters webAppName=testapp

az acr login --name <insert name of ACR resource here>
docker build -t <ACR>.azurecr.io/testapp:latest .
docker push <ACR>.azurecr.io/testapp:latest
```
