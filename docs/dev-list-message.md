# Draft message for dev@nifi.apache.org

Send by hand. Subject and body below.

---

**Subject:** MongoDB CDC processor (NIFI-7180) — working implementation, and a small API question

Hello,

NiFi has CDC for MySQL but nothing for MongoDB. NIFI-7180 asks for it and has been open since February 2020
with no pull request. Today the usual answer is to put Debezium or the MongoDB Kafka Connector plus Kafka
between MongoDB and NiFi, which is two extra systems for what one change stream cursor does on its own.

I have written a `CaptureChangeMongoDB` processor and would like to contribute it. It is laid out as
`nifi-extension-bundles/nifi-cdc/nifi-cdc-mongodb-bundle`, mirroring the MySQL CDC bundle:

  https://github.com/danmorcov88/nifi-cdc-mongodb-bundle

What it does:

- Reads a MongoDB change stream for a collection or a database and writes insert, update, replace, delete
  and invalidate events as records through a configured Record Writer, one FlowFile per batch.
- Stores the resume token in cluster state, and only after the FlowFiles of a batch are committed, so
  events are delivered at least once and never skipped.
- Optionally takes an initial snapshot first and hands over to the stream without a gap.
- Never writes to the source database. It needs only `changeStream` and `find` on the watched scope.
- Implements `VerifiableProcessor`: connectivity, server version, replica set or mongos, privileges,
  pre-image configuration.
- Uses the existing `MongoDBClientService`, so connection settings and credentials stay in one place.

The standalone build targets NiFi 2.12.0; a branch against current main builds and tests there as well. It
needs MongoDB 6.0 or later, and is covered by unit tests and Testcontainers integration tests against
MongoDB 7.0 and 8.0.

One question before I open the pull request. `MongoDBClientService` exposes `getDatabase(String)`,
`getURI()` and `getWriteConcern()`. That covers collection scope and database scope. It does not cover a
deployment-wide change stream, which needs `MongoClient.watch()` — the driver sends that as
`allChangesForCluster: true`, and `getDatabase("admin").watch()` is not equivalent, since it is tagged at
database level and reports changes to `admin` only.

So for now I have left deployment scope out. Would the project accept adding an accessor for the
`MongoClient` (or a `watch()` method) to `MongoDBClientService`? The field already exists on
`MongoDBControllerService`; it is only not exposed. If that sounds reasonable I will open a separate Jira
for it and keep it out of the processor pull request.

Happy to split the contribution differently if that is easier to review.

Thanks,
Dan
