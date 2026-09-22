# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), the versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `CaptureChangeMongoDB` reads the change stream of a collection and writes insert, update, replace, delete
  and invalidate events as records through a Record Writer, one FlowFile per batch. Documents are written as
  Extended JSON strings.
- The resume token is stored in cluster state in the same transaction as the FlowFiles of the batch, so a
  failure replays events and never skips them. A batch that cannot be written or read to the end is
  discarded, and the stream is reopened from the last committed token.
- An idle stream stores the position the server reports, so a quiet collection does not fall behind the
  oplog while nothing changes.
- `Max Events Per FlowFile`, `Max Batch Duration` and `Max Await Time` bound how much a single trigger reads
  and how long it blocks.
- An `invalidate` event, which the server sends when the watched collection is dropped or renamed, is
  delivered like any other event, and the stream carries on after it with `startAfter`.
- `On History Lost` decides what happens when the server can no longer serve the stored position: `Fail`
  keeps the position and reports the problem, `Restart From Now` gives it up and continues with the changes
  made from then on.
- After a failed attempt the processor waits before trying again, doubling the wait up to a minute and
  giving it up as soon as a trigger succeeds, so an unreachable server is not asked on every trigger.
- `verify` checks the connection, that the server is a replica set or a sharded cluster, and that it is
  MongoDB 6.0 or later.
- Maven skeleton with the `nifi-cdc-mongodb-processors` and `nifi-cdc-mongodb-nar` modules, parented to
  `org.apache.nifi:nifi-cdc`, so the bundle can move into `apache/nifi` unchanged.
- GitHub Actions workflows for build and release, and the upstream RAT, checkstyle and PMD checks through
  the `contrib-check` profile.
- Development lab in `docker/compose.yml`: a single-node MongoDB replica set and NiFi 2.12.0 loading the
  built NAR.

### Known limitations

- Collection scope only. Database scope, the aggregation pipeline, the full document options and
  `Start Position` follow.
- Deployment scope is not possible through the current `MongoDBClientService`; see `docs/prior-art.md`.
