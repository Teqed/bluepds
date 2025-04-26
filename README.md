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

This is an implementation of an ATProto PDS, built with [Axum](https://github.com/tokio-rs/axum) and [Atrium](https://github.com/sugyan/atrium).
Heavily inspired by David Buchanan's [millipds](https://github.com/DavidBuchanan314/millipds).

This PDS implementation uses a SQLite database to store private account information and file storage to store canonical user data.

If you want to see this PDS in action, there is a live account hosted by this PDS at [@test.justinm.one](https://bsky.app/profile/test.justinm.one)!

> [!WARNING]
> This PDS is undergoing heavy development. Do _NOT_ use this to host your primary account or any important data!

## Quick Start
```
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
  * metrics.rs  - Definitions for telemetry instruments
  * plc.rs      - Functionality to access the Public Ledger of Credentials
  * storage.rs  - Helpers to access user repository storage
```

## To-do
### High-level features
- [ ] Authentication
  - [ ] [OAuth support](https://atproto.com/specs/oauth)
- [ ] Storage backend abstractions
  - [ ] Azure blob storage backend
  - [ ] Backblaze b2(?)
- [ ] Telemetry
  - [X] [Metrics](https://github.com/metrics-rs/metrics) (counters/gauges/etc)
  - [X] Exporters for common backends (Prometheus/etc)

### APIs
- [X] [Service proxying](https://atproto.com/specs/xrpc#service-proxying)
- [X] UG /xrpc/_health (undocumented, but impl by reference PDS)
- com.atproto.identity
    - [X] AP /xrpc/com.atproto.identity.updateHandle
    - [ ] AP /xrpc/com.atproto.identity.requestPlcOperationSignature
    - [ ] AP /xrpc/com.atproto.identity.signPlcOperation
    - [X] UG /xrpc/com.atproto.identity.resolveHandle
- com.atproto.server
    - [X] UG /xrpc/com.atproto.server.describeServer
    - [X] UP /xrpc/com.atproto.server.createAccount
    - [X] AP /xrpc/com.atproto.server.createInviteCode
    - [X] UP /xrpc/com.atproto.server.createSession
    - [X] AG /xrpc/com.atproto.server.getServiceAuth
    - [X] AG /xrpc/com.atproto.server.getSession
- com.atproto.repo
    - [X] AP /xrpc/com.atproto.repo.applyWrites
    - [X] AP /xrpc/com.atproto.repo.createRecord
    - [X] AP /xrpc/com.atproto.repo.putRecord
    - [X] AP /xrpc/com.atproto.repo.deleteRecord
    - [X] UG /xrpc/com.atproto.repo.describeRepo
    - [X] UG /xrpc/com.atproto.repo.getRecord
    - [X] UG /xrpc/com.atproto.repo.listRecords
    - [X] AP /xrpc/com.atproto.repo.uploadBlob
- com.atproto.sync
    - [X] UG /xrpc/com.atproto.sync.getBlob
    - [X] UG /xrpc/com.atproto.sync.getBlocks
    - [X] UG /xrpc/com.atproto.sync.getLatestCommit
    - [X] UG /xrpc/com.atproto.sync.getRecord
    - [X] UG /xrpc/com.atproto.sync.getRepoStatus
    - [X] UG /xrpc/com.atproto.sync.getRepo
    - [X] UG /xrpc/com.atproto.sync.listBlobs
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
