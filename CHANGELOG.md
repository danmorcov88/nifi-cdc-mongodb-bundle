# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), the versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Maven skeleton with the `nifi-cdc-mongodb-processors` and `nifi-cdc-mongodb-nar` modules, parented to
  `org.apache.nifi:nifi-cdc`, so the bundle can move into `apache/nifi` unchanged.
- `CaptureChangeMongoDB` registered as a processor, with the Client Service, Database Name, Collection Name
  and Record Writer properties. It does not read change events yet.
- GitHub Actions workflows for build and release, and the upstream RAT, checkstyle and PMD checks through
  the `contrib-check` profile.
- Development lab in `docker/compose.yml`: a single-node MongoDB replica set and NiFi 2.12.0 loading the
  built NAR.
