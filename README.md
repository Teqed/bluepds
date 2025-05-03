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
This implementation forked from the [azure-rust-app](https://github.com/DrChat/azure-rust-app) starter template.

This PDS implementation uses a SQLite database to store private account information and file storage to store canonical user data.

If you want to see this PDS in action, there is a live account hosted by this PDS at [@test.justinm.one](https://bsky.app/profile/test.justinm.one)!

> [!WARNING]
> This PDS is undergoing heavy development. Do _NOT_ use this to host your primary account or any important data!

## Quick Start
```
cargo run
```

## Cost breakdown (on Azure)
This is how much it costs to host the @test.justinm.one account:

- $20/mo
  - $13/mo: Azure Application Service
  - $5/mo: Azure Container Registry
  - $1/mo: Azure Storage Account

This is _without_ optimizing for costs. The PDS can likely be made much cheaper.

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
  * oauth.rs    - OAuth routes
  * plc.rs      - Functionality to access the Public Ledger of Credentials
  * storage.rs  - Helpers to access user repository storage
```

## To-do
### Teqed's
- [ ] OAuth
  - [ ] `/.well-known/oauth-protected-resource` - Authorization Server Metadata
  - [ ] `/.well-known/oauth-authorization-server`
  - [ ] `/par` - Pushed Authorization Request
  - [ ] `/client-metadata.json` - Client metadata discovery
  - [ ] `/oauth/authorize`
  - [ ] `/oauth/authorize/sign-in`
  - [ ] `/oauth/token`
- [ ] Email verification
- [ ] 2FA
- [ ] Admin endpoints
- [ ] App passwords
- [X] `listRecords` fixes
  - [X] Fix collection prefixing (terminate with `/`)
  - [X] Fix cursor handling (return `cid` instead of `key`)
- [X] Session management (JWT)
  - [X] Match token fields to reference implementation
  - [X] RefreshSession from Bluesky Client
    - [X] Respond with JSON error message `ExpiredToken`
- [X] Cursor handling
  - [X] Implement time-based unix microsecond sequences
  - [X] Startup with present cursor
- [X] Respond `RecordNotFound`, required for:
  - [X] app.bsky.feed.postgate
  - [X] app.bsky.feed.threadgate
  - [ ] app.bsky... (profile creation?)
- [X] Linting
  - [X] Rustfmt
    - [X] warnings
    - [X] deprecated-safe
    - [X] future-incompatible
    - [X] keyword-idents
    - [X] let-underscore
    - [X] nonstandard-style
    - [X] refining-impl-trait
    - [X] rust-2018-idioms
    - [X] rust-2018/2021/2024-compatibility
    - [X] ungrouped
  - [X] Clippy
    - [X] nursery
    - [X] correctness
    - [X] suspicious
    - [X] complexity
    - [X] perf
    - [X] style
    - [X] pedantic
    - [X] cargo
    - [X] ungrouped
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
<!-- - [ ] xx /xrpc/app.bsky.notification.registerPush
- app.bsky.actor
    - [X] AG /xrpc/app.bsky.actor.getPreferences
    - [ ] xx /xrpc/app.bsky.actor.getProfile
    - [ ] xx /xrpc/app.bsky.actor.getProfiles
    - [X] AP /xrpc/app.bsky.actor.putPreferences
- app.bsky.feed
    - [ ] xx /xrpc/app.bsky.feed.getActorLikes
    - [ ] xx /xrpc/app.bsky.feed.getAuthorFeed
    - [ ] xx /xrpc/app.bsky.feed.getFeed
    - [ ] xx /xrpc/app.bsky.feed.getPostThread
    - [ ] xx /xrpc/app.bsky.feed.getTimeline -->
- com.atproto.admin
    - [ ] xx /xrpc/com.atproto.admin.deleteAccount
    - [ ] xx /xrpc/com.atproto.admin.disableAccountInvites
    - [ ] xx /xrpc/com.atproto.admin.disableInviteCodes
    - [ ] xx /xrpc/com.atproto.admin.enableAccountInvites
    - [ ] xx /xrpc/com.atproto.admin.getAccountInfo
    - [ ] xx /xrpc/com.atproto.admin.getAccountInfos
    - [ ] xx /xrpc/com.atproto.admin.getInviteCodes
    - [ ] xx /xrpc/com.atproto.admin.getSubjectStatus
    - [ ] xx /xrpc/com.atproto.admin.sendEmail
    - [ ] xx /xrpc/com.atproto.admin.updateAccountEmail
    - [ ] xx /xrpc/com.atproto.admin.updateAccountHandle
    - [ ] xx /xrpc/com.atproto.admin.updateAccountPassword
    - [ ] xx /xrpc/com.atproto.admin.updateSubjectStatus
- com.atproto.identity
    - [ ] xx /xrpc/com.atproto.identity.getRecommendedDidCredentials
    - [ ] AP /xrpc/com.atproto.identity.requestPlcOperationSignature
    - [X] UG /xrpc/com.atproto.identity.resolveHandle
    - [ ] AP /xrpc/com.atproto.identity.signPlcOperation
    - [ ] xx /xrpc/com.atproto.identity.submitPlcOperation
    - [X] AP /xrpc/com.atproto.identity.updateHandle
<!-- - com.atproto.moderation
    - [ ] xx /xrpc/com.atproto.moderation.createReport -->
- com.atproto.repo
    - [X] AP /xrpc/com.atproto.repo.applyWrites
    - [X] AP /xrpc/com.atproto.repo.createRecord
    - [X] AP /xrpc/com.atproto.repo.deleteRecord
    - [X] UG /xrpc/com.atproto.repo.describeRepo
    - [X] UG /xrpc/com.atproto.repo.getRecord
    - [ ] xx /xrpc/com.atproto.repo.importRepo
    - [ ] xx /xrpc/com.atproto.repo.listMissingBlobs
    - [X] UG /xrpc/com.atproto.repo.listRecords
    - [X] AP /xrpc/com.atproto.repo.putRecord
    - [X] AP /xrpc/com.atproto.repo.uploadBlob
- com.atproto.server
    - [ ] xx /xrpc/com.atproto.server.activateAccount
    - [ ] xx /xrpc/com.atproto.server.checkAccountStatus
    - [ ] xx /xrpc/com.atproto.server.confirmEmail
    - [X] UP /xrpc/com.atproto.server.createAccount
    - [ ] xx /xrpc/com.atproto.server.createAppPassword
    - [X] AP /xrpc/com.atproto.server.createInviteCode
    - [ ] xx /xrpc/com.atproto.server.createInviteCodes
    - [X] UP /xrpc/com.atproto.server.createSession
    - [ ] xx /xrpc/com.atproto.server.deactivateAccount
    - [ ] xx /xrpc/com.atproto.server.deleteAccount
    - [ ] xx /xrpc/com.atproto.server.deleteSession
    - [X] UG /xrpc/com.atproto.server.describeServer
    - [ ] xx /xrpc/com.atproto.server.getAccountInviteCodes
    - [X] AG /xrpc/com.atproto.server.getServiceAuth
    - [X] AG /xrpc/com.atproto.server.getSession
    - [ ] xx /xrpc/com.atproto.server.listAppPasswords
    - [ ] xx /xrpc/com.atproto.server.refreshSession
    - [ ] xx /xrpc/com.atproto.server.requestAccountDelete
    - [ ] xx /xrpc/com.atproto.server.requestEmailConfirmation
    - [ ] xx /xrpc/com.atproto.server.requestEmailUpdate
    - [ ] xx /xrpc/com.atproto.server.requestPasswordReset
    - [ ] xx /xrpc/com.atproto.server.reserveSigningKey
    - [ ] xx /xrpc/com.atproto.server.resetPassword
    - [ ] xx /xrpc/com.atproto.server.revokeAppPassword
    - [ ] xx /xrpc/com.atproto.server.updateEmail
- com.atproto.sync
    - [X] UG /xrpc/com.atproto.sync.getBlob
    - [X] UG /xrpc/com.atproto.sync.getBlocks
    - [X] UG /xrpc/com.atproto.sync.getLatestCommit
    - [X] UG /xrpc/com.atproto.sync.getRecord
    - [X] UG /xrpc/com.atproto.sync.getRepo
    - [X] UG /xrpc/com.atproto.sync.getRepoStatus
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
## Quick Deployment (NixOS)
```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    bluepds = {
      url = "github:Teqed/bluesky-pds";
    };
  };
  outputs = {
    nixpkgs,
    bluepds,
    ...
  }: {
    nixosConfigurations.mysystem = nixpkgs.lib.nixosSystem {
      modules = [
        ({ pkgs, ... }: {
          config.services.bluepds = {
            enable = true;
            host_name = "pds.example.com";
            listen_address = "0.0.0.0:8000";
            test = "true"; # Set to false for production
          };
        })
      ];
    };
  };
}
```
