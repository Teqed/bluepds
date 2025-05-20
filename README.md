# ATProto PDS
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

This is an implementation of an ATProto PDS, built with [Axum](https://github.com/tokio-rs/axum), [rsky](https://github.com/blacksky-algorithms/rsky/) and [Atrium](https://github.com/sugyan/atrium).
This PDS implementation uses a SQLite database and [diesel.rs](https://diesel.rs/) ORM to store canonical user data, and file system storage to store user blobs.

Heavily inspired by David Buchanan's [millipds](https://github.com/DavidBuchanan314/millipds).
This implementation forked from [DrChat/bluepds](https://github.com/DrChat/bluepds), and now makes heavy use of the [rsky-repo](https://github.com/blacksky-algorithms/rsky/tree/main/rsky-repo) repository implementation.
The `actor_store` and `account_manager` modules have been reimplemented from [rsky-pds](https://github.com/blacksky-algorithms/rsky/tree/main/rsky-pds) to use a SQLite backend and file storage, which are themselves adapted from the [original Bluesky implementation](https://github.com/bluesky-social/atproto) using SQLite in Typescript.


If you want to see this fork in action, there is a live account hosted by this PDS at [@teq.shatteredsky.net](https://bsky.app/profile/teq.shatteredsky.net)!

> [!WARNING]
> This PDS is undergoing heavy development, and this branch is not at an operable release. Do _NOT_ use this to host your primary account or any important data!

## Quick Start
```
cargo run
```

## Cost breakdown (on Oracle Cloud Infrastructure)
This is how much it costs to host the @teq.shatteredsky.net account:

- $0/mo [Always Free Resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
  - $0/mo: VM.Standard.A1.Flex
    - OCPU count: 2
    - Network bandwidth: 2 Gbps
    - Memory: 12 GB
  - $0/mo: Virtual Cloud Network
    - IPv4 address
    - IPv6 address
  - $0/mo: Boot volume
    - Size: 47 GB
    - VPUs/GB: 10

This is about half of the 3,000 OCPU hours and 18,000 GB hours available per month for free on the VM.Standard.A1.Flex shape. This is _without_ optimizing for costs. The PDS can likely be made to run on much less resources.

## To-do
### APIs
- [ ] [Service proxying](https://atproto.com/specs/xrpc#service-proxying)
- [ ] UG /xrpc/_health (undocumented, but impl by reference PDS)
<!-- - [ ] xx /xrpc/app.bsky.notification.registerPush
- app.bsky.actor
    - [ ] AG /xrpc/app.bsky.actor.getPreferences
    - [ ] xx /xrpc/app.bsky.actor.getProfile
    - [ ] xx /xrpc/app.bsky.actor.getProfiles
    - [ ] AP /xrpc/app.bsky.actor.putPreferences
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
    - [ ] UG /xrpc/com.atproto.identity.resolveHandle
    - [ ] AP /xrpc/com.atproto.identity.signPlcOperation
    - [ ] xx /xrpc/com.atproto.identity.submitPlcOperation
    - [ ] AP /xrpc/com.atproto.identity.updateHandle
<!-- - com.atproto.moderation
    - [ ] xx /xrpc/com.atproto.moderation.createReport -->
- com.atproto.repo
    - [X] AP /xrpc/com.atproto.repo.applyWrites
    - [X] AP /xrpc/com.atproto.repo.createRecord
    - [X] AP /xrpc/com.atproto.repo.deleteRecord
    - [X] UG /xrpc/com.atproto.repo.describeRepo
    - [X] UG /xrpc/com.atproto.repo.getRecord
    - [X] xx /xrpc/com.atproto.repo.importRepo
    - [X] xx /xrpc/com.atproto.repo.listMissingBlobs
    - [X] UG /xrpc/com.atproto.repo.listRecords
    - [X] AP /xrpc/com.atproto.repo.putRecord
    - [X] AP /xrpc/com.atproto.repo.uploadBlob
- com.atproto.server
    - [ ] xx /xrpc/com.atproto.server.activateAccount
    - [ ] xx /xrpc/com.atproto.server.checkAccountStatus
    - [ ] xx /xrpc/com.atproto.server.confirmEmail
    - [ ] UP /xrpc/com.atproto.server.createAccount
    - [ ] xx /xrpc/com.atproto.server.createAppPassword
    - [ ] AP /xrpc/com.atproto.server.createInviteCode
    - [ ] xx /xrpc/com.atproto.server.createInviteCodes
    - [ ] UP /xrpc/com.atproto.server.createSession
    - [ ] xx /xrpc/com.atproto.server.deactivateAccount
    - [ ] xx /xrpc/com.atproto.server.deleteAccount
    - [ ] xx /xrpc/com.atproto.server.deleteSession
    - [ ] UG /xrpc/com.atproto.server.describeServer
    - [ ] xx /xrpc/com.atproto.server.getAccountInviteCodes
    - [ ] AG /xrpc/com.atproto.server.getServiceAuth
    - [ ] AG /xrpc/com.atproto.server.getSession
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
    - [ ] UG /xrpc/com.atproto.sync.getBlob
    - [ ] UG /xrpc/com.atproto.sync.getBlocks
    - [ ] UG /xrpc/com.atproto.sync.getLatestCommit
    - [ ] UG /xrpc/com.atproto.sync.getRecord
    - [ ] UG /xrpc/com.atproto.sync.getRepo
    - [ ] UG /xrpc/com.atproto.sync.getRepoStatus
    - [ ] UG /xrpc/com.atproto.sync.listBlobs
    - [ ] UG /xrpc/com.atproto.sync.listRepos
    - [ ] UG /xrpc/com.atproto.sync.subscribeRepos

## Deployment (NixOS)
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
