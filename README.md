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
- [ ] /xrpc/_health (undocumented, but impl by reference PDS)
<!-- - [ ] /xrpc/app.bsky.notification.registerPush
- app.bsky.actor
    - [ ] /xrpc/app.bsky.actor.getPreferences
    - [ ] /xrpc/app.bsky.actor.getProfile
    - [ ] /xrpc/app.bsky.actor.getProfiles
    - [ ] /xrpc/app.bsky.actor.putPreferences
- app.bsky.feed
    - [ ] /xrpc/app.bsky.feed.getActorLikes
    - [ ] /xrpc/app.bsky.feed.getAuthorFeed
    - [ ] /xrpc/app.bsky.feed.getFeed
    - [ ] /xrpc/app.bsky.feed.getPostThread
    - [ ] /xrpc/app.bsky.feed.getTimeline -->
- com.atproto.admin
    - [ ] /xrpc/com.atproto.admin.deleteAccount
    - [ ] /xrpc/com.atproto.admin.disableAccountInvites
    - [ ] /xrpc/com.atproto.admin.disableInviteCodes
    - [ ] /xrpc/com.atproto.admin.enableAccountInvites
    - [ ] /xrpc/com.atproto.admin.getAccountInfo
    - [ ] /xrpc/com.atproto.admin.getAccountInfos
    - [ ] /xrpc/com.atproto.admin.getInviteCodes
    - [ ] /xrpc/com.atproto.admin.getSubjectStatus
    - [ ] /xrpc/com.atproto.admin.sendEmail
    - [ ] /xrpc/com.atproto.admin.updateAccountEmail
    - [ ] /xrpc/com.atproto.admin.updateAccountHandle
    - [ ] /xrpc/com.atproto.admin.updateAccountPassword
    - [ ] /xrpc/com.atproto.admin.updateSubjectStatus
- com.atproto.identity
    - [ ] /xrpc/com.atproto.identity.getRecommendedDidCredentials
    - [ ] /xrpc/com.atproto.identity.requestPlcOperationSignature
    - [ ] /xrpc/com.atproto.identity.resolveHandle
    - [ ] /xrpc/com.atproto.identity.signPlcOperation
    - [ ] /xrpc/com.atproto.identity.submitPlcOperation
    - [ ] /xrpc/com.atproto.identity.updateHandle
<!-- - com.atproto.moderation
    - [ ] /xrpc/com.atproto.moderation.createReport -->
- com.atproto.repo
    - [X] /xrpc/com.atproto.repo.applyWrites
    - [X] /xrpc/com.atproto.repo.createRecord
    - [X] /xrpc/com.atproto.repo.deleteRecord
    - [X] /xrpc/com.atproto.repo.describeRepo
    - [X] /xrpc/com.atproto.repo.getRecord
    - [X] /xrpc/com.atproto.repo.importRepo
    - [X] /xrpc/com.atproto.repo.listMissingBlobs
    - [X] /xrpc/com.atproto.repo.listRecords
    - [X] /xrpc/com.atproto.repo.putRecord
    - [X] /xrpc/com.atproto.repo.uploadBlob
- com.atproto.server
    - [ ] /xrpc/com.atproto.server.activateAccount
    - [ ] /xrpc/com.atproto.server.checkAccountStatus
    - [ ] /xrpc/com.atproto.server.confirmEmail
    - [ ] /xrpc/com.atproto.server.createAccount
    - [ ] /xrpc/com.atproto.server.createAppPassword
    - [ ] /xrpc/com.atproto.server.createInviteCode
    - [ ] /xrpc/com.atproto.server.createInviteCodes
    - [ ] /xrpc/com.atproto.server.createSession
    - [ ] /xrpc/com.atproto.server.deactivateAccount
    - [ ] /xrpc/com.atproto.server.deleteAccount
    - [ ] /xrpc/com.atproto.server.deleteSession
    - [ ] /xrpc/com.atproto.server.describeServer
    - [ ] /xrpc/com.atproto.server.getAccountInviteCodes
    - [ ] /xrpc/com.atproto.server.getServiceAuth
    - [ ] /xrpc/com.atproto.server.getSession
    - [ ] /xrpc/com.atproto.server.listAppPasswords
    - [ ] /xrpc/com.atproto.server.refreshSession
    - [ ] /xrpc/com.atproto.server.requestAccountDelete
    - [ ] /xrpc/com.atproto.server.requestEmailConfirmation
    - [ ] /xrpc/com.atproto.server.requestEmailUpdate
    - [ ] /xrpc/com.atproto.server.requestPasswordReset
    - [ ] /xrpc/com.atproto.server.reserveSigningKey
    - [ ] /xrpc/com.atproto.server.resetPassword
    - [ ] /xrpc/com.atproto.server.revokeAppPassword
    - [ ] /xrpc/com.atproto.server.updateEmail
- com.atproto.sync
    - [ ] /xrpc/com.atproto.sync.getBlob
    - [ ] /xrpc/com.atproto.sync.getBlocks
    - [ ] /xrpc/com.atproto.sync.getLatestCommit
    - [ ] /xrpc/com.atproto.sync.getRecord
    - [ ] /xrpc/com.atproto.sync.getRepo
    - [ ] /xrpc/com.atproto.sync.getRepoStatus
    - [ ] /xrpc/com.atproto.sync.listBlobs
    - [ ] /xrpc/com.atproto.sync.listRepos
    - [ ] /xrpc/com.atproto.sync.subscribeRepos

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
