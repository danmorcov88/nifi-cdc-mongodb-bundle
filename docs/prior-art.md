# Prior art

Checked on 22 September 2026.

**NIFI-7180 (MongoDB CDC Processor)** is still Open and Unresolved. It was created on 21 February 2020,
assigned to Gerdan Santos, priority Minor, and last touched eight minutes after it was created. It has one
vote, three watchers, no comments, no linked pull request and no fix version. The description asks for a
processor that connects to a replica set or a sharded cluster, keeps the state of the collections it watches,
and can snapshot existing collections. That is the scope this bundle implements.

**No code exists upstream.** A search of `apache/nifi` pull requests for `NIFI-7180`, for
`CaptureChangeMongoDB` and for any open pull request mentioning MongoDB returns nothing. The only NiFi CDC
processor in the tree is `CaptureChangeMySQL` in `nifi-extension-bundles/nifi-cdc/nifi-cdc-mysql-bundle`.
A GitHub-wide code search for `CaptureChangeMongoDB` finds only documentation in unrelated MCP tooling
(`cloudera/NiFi-MCP-Server` and forks) that tells users MongoDB CDC is available through a processor of that
name. It is not. People already expect this processor to exist.

**The public route today** is MongoDB to Kafka through Debezium or the MongoDB Kafka Connector, then Kafka
into NiFi. That is two extra systems to run for what a change stream cursor does on its own.

## What the NiFi API gives us

`MongoDBClientService` (module `nifi-mongodb-client-service-api`, NiFi 2.12.0) exposes exactly three methods:
`getDatabase(String)`, `getURI()` and `getWriteConcern()`. The `MongoClient` it builds is a protected field
with no accessor.

That is enough for collection scope (`getDatabase(db).getCollection(c).watch()`) and for database scope
(`getDatabase(db).watch()`), which is what this bundle ships.

It is not enough for a deployment-wide change stream. That needs `MongoClient.watch()`, which the driver tags
`ChangeStreamLevel.CLIENT` and sends as `allChangesForCluster: true`. `getDatabase("admin").watch()` is not a
substitute: the driver tags it `ChangeStreamLevel.DATABASE`, so it reports changes to the `admin` database
only. Deployment scope is therefore out of scope for 0.1.0, and a separate Jira asking for a `MongoClient`
accessor on `MongoDBClientService` is the clean way to unlock it.

The driver version that ships with NiFi 2.12.0 is `mongodb-driver-sync` 5.11.0, new enough for every change
stream option this bundle uses, including `fullDocumentBeforeChange`.
