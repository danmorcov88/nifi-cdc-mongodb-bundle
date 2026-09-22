<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# NiFi MongoDB CDC Bundle

`CaptureChangeMongoDB` is an Apache NiFi 2.x processor that streams insert, update, replace, delete and
invalidate events from MongoDB using Change Streams, and writes them as records with a Record Writer. It
keeps the resume token in NiFi cluster state, so it continues where it stopped, and it never writes to the
source database. No Kafka, no Kafka Connect, no Debezium in between.

The module follows the layout and conventions of `nifi-extension-bundles/nifi-cdc` in Apache NiFi and is
intended as a contribution for [NIFI-7180](https://issues.apache.org/jira/browse/NIFI-7180).

## Status

**Work in progress.** The processor reads the change stream of a collection or of a whole database and
writes the events as records, resuming from the stored resume token after a restart. It supports a
server-side aggregation pipeline, the full document and pre-image options, a start position in the past and
both Extended JSON modes, and it survives a dropped collection, an unreachable server and a position the
server can no longer serve. It can take an initial snapshot of a collection first and carry on with the
changes without a gap. Covered by unit tests and by integration tests against MongoDB 7.0 and 8.0.

What is left for 0.1.0 is the documentation and the release.

## Requirements

- Apache NiFi 2.12.0, Java 21
- MongoDB 6.0 or later, running as a replica set or a sharded cluster (Change Streams are not available on a
  standalone server)
- A database user with `changeStream` and `find` on the collections to capture

## Build

```
./mvnw verify                        # compile, unit tests, NAR
./mvnw verify -P contrib-check       # same, plus the upstream RAT, checkstyle and PMD checks
./mvnw verify -P integration-tests   # also runs the Testcontainers suite (needs Docker)
```

The NAR is produced at `nifi-cdc-mongodb-nar/target/nifi-cdc-mongodb-nar-2.12.0.nar`.

## Development lab

`docker/compose.yml` starts a single-node MongoDB replica set and a NiFi 2.12.0 container that loads the
built NAR:

```
docker compose -f docker/compose.yml up -d mongo
./mvnw package
docker compose -f docker/compose.yml --profile nifi up -d    # https://localhost:8443/nifi
```

NiFi signs in with `admin` / `adminadminadmin`. From NiFi the connection URI is
`mongodb://mongo:27017/?replicaSet=rs0`; from the host machine use
`mongodb://localhost:27017/?directConnection=true`. The lab runs without authentication.

## License

Apache License 2.0.
