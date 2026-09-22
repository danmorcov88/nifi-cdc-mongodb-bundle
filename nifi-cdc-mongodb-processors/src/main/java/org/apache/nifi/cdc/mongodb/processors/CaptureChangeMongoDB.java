/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.cdc.mongodb.processors;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"mongodb", "cdc", "change stream", "replication", "event"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from MongoDB using Change Streams. The processor reads insert, update, "
        + "replace, delete and invalidate events for a collection and writes them as records using the configured Record Writer, one "
        + "FlowFile per batch. Each record has the fields operation, database, collection, document_key, full_document, "
        + "full_document_before_change, updated_fields, removed_fields, cluster_time, wall_time, txn_number and resume_token. Documents "
        + "are written as Extended JSON strings. The resume token is stored in the same transaction as the FlowFiles of a batch, so a "
        + "failure replays events but never skips them. The processor never writes to the source database; it needs only the "
        + "changeStream and find privileges on the watched collection. MongoDB 6.0 or later, running as a replica set or a sharded "
        + "cluster, is required. Without stored state the stream starts at the moment the processor is started, so changes made while "
        + "it is stopped for the first time are not captured.")
@Stateful(scopes = Scope.CLUSTER, description = "The resume token of the last change event written to a FlowFile is stored so that the "
        + "processor resumes from the same position after a restart or a change of the primary node. Clear the state to start over; "
        + "the state is not valid for a different database or collection.")
@WritesAttributes({
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_DATABASE, description = "Database the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_COLLECTION, description = "Collection the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, description = "Resume token of the first change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, description = "Resume token of the last change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS,
                description = "Milliseconds between the wall clock time of the last change event in the FlowFile and the time the FlowFile was written"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, description = "Number of records written to the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the Record Writer")
})
public class CaptureChangeMongoDB extends AbstractProcessor implements VerifiableProcessor {

    public static final String ATTRIBUTE_DATABASE = "mongodb.database";
    public static final String ATTRIBUTE_COLLECTION = "mongodb.collection";
    public static final String ATTRIBUTE_FIRST_RESUME_TOKEN = "cdc.first.resume.token";
    public static final String ATTRIBUTE_LAST_RESUME_TOKEN = "cdc.last.resume.token";
    public static final String ATTRIBUTE_LAG_MILLIS = "cdc.lag.millis";
    public static final String ATTRIBUTE_RECORD_COUNT = "record.count";

    private static final int MINIMUM_MAJOR_VERSION = 6;
    private static final String SHARDED_CLUSTER_MESSAGE = "isdbgrid";

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Client Service")
            .description("The MongoDB client service that provides the connection to the replica set or the sharded cluster. "
                    + "Credentials and connection timeouts are configured there.")
            .identifiesControllerService(MongoDBClientService.class)
            .required(true)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("Name of the database that holds the collection to watch.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("Name of the collection to watch.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor MAX_EVENTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Max Events Per FlowFile")
            .description("The number of change events after which the FlowFile is completed and transferred, even if the stream has more "
                    + "events available.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_BATCH_DURATION = new PropertyDescriptor.Builder()
            .name("Max Batch Duration")
            .description("The time after which the FlowFile is completed and transferred, even if fewer events than Max Events Per "
                    + "FlowFile have been read. Keeps the delay of an event bounded while the collection changes slowly.")
            .required(true)
            .defaultValue("5 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_AWAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Await Time")
            .description("How long the server holds the request open while the stream has no new event. A trigger that finds nothing "
                    + "returns after this time, so this is the longest a single trigger blocks.")
            .required(true)
            .defaultValue("1 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer used to write the change events to a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing the change events read from the change stream")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            DATABASE_NAME,
            COLLECTION_NAME,
            MAX_EVENTS_PER_FLOWFILE,
            MAX_BATCH_DURATION,
            MAX_AWAIT_TIME,
            RECORD_WRITER);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile StreamOpener streamOpener;
    private volatile EventMapper eventMapper;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile Map<String, String> namespaceAttributes;
    private volatile String transitUri;
    private volatile int maxEventsPerFlowFile;
    private volatile long maxBatchDurationMillis;

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;

    /**
     * The position the stream is reopened from after a failure: the last committed resume token, or, before the
     * first commit, the position the cursor was opened at, so that reopening after a failed batch does not step
     * over the events of that batch.
     */
    private volatile String resumeFrom;

    /** The resume token currently held in processor state. */
    private volatile String committedToken;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final MongoDBClientService clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue();
        final long maxAwaitTimeMillis = context.getProperty(MAX_AWAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        streamOpener = new StreamOpener(clientService, databaseName, collectionName, maxAwaitTimeMillis);
        eventMapper = new EventMapper(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        namespaceAttributes = Map.of(ATTRIBUTE_DATABASE, databaseName, ATTRIBUTE_COLLECTION, collectionName);
        transitUri = buildTransitUri(clientService.getURI(), databaseName, collectionName);
        maxEventsPerFlowFile = context.getProperty(MAX_EVENTS_PER_FLOWFILE).asInteger();
        maxBatchDurationMillis = context.getProperty(MAX_BATCH_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);

        committedToken = context.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN);
        resumeFrom = committedToken;
    }

    @OnStopped
    public void stop() {
        closeCursor();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor;
        try {
            currentCursor = getOrOpenCursor();
        } catch (final Exception e) {
            closeCursor();
            getLogger().error("Opening the change stream of {} failed; retrying", transitUri, e);
            context.yield();
            return;
        }

        // Events taken from the cursor are either committed to a FlowFile together with their resume token, or the
        // cursor is discarded and reopened from the last committed token, so that no event is passed over.
        final EventBatch batch = new EventBatch(session, writerFactory, eventMapper, getLogger(), transitUri, namespaceAttributes);
        boolean completed = false;
        try {
            read(currentCursor, batch);

            if (batch.isEmpty()) {
                confirmIdleProgress(session, currentCursor);
                context.yield();
            } else {
                batch.transfer(REL_SUCCESS);
                commit(session, batch.getLastResumeToken());
                getLogger().debug("Wrote {} change events from {}", batch.getEventCount(), transitUri);
            }
            completed = true;
        } catch (final Exception e) {
            getLogger().error("Reading changes from {} failed; the change stream will be reopened from the last committed resume token",
                    transitUri, e);
            context.yield();
        } finally {
            if (!completed) {
                batch.rollback();
                closeCursor();
            }
        }
    }

    private void read(final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor, final EventBatch batch) throws IOException {
        final long deadline = System.currentTimeMillis() + maxBatchDurationMillis;
        while (batch.getEventCount() < maxEventsPerFlowFile && System.currentTimeMillis() < deadline) {
            final ChangeStreamDocument<BsonDocument> event = currentCursor.tryNext();
            if (event == null) {
                break;
            }
            batch.write(event);
        }
    }

    /**
     * An idle stream still moves forward: the server reports a resume token for the point it has read up to. Storing
     * it keeps the position of a quiet collection close to the end of the oplog, so that a long pause does not leave
     * the processor with a token the server has already discarded.
     */
    private void confirmIdleProgress(final ProcessSession session, final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor)
            throws IOException {
        final String resumeToken = EventMapper.resumeTokenData(currentCursor.getResumeToken());
        if (resumeToken != null && !resumeToken.equals(committedToken)) {
            commit(session, resumeToken);
        }
    }

    private void commit(final ProcessSession session, final String resumeToken) throws IOException {
        session.setState(Map.of(StateKeys.RESUME_TOKEN, resumeToken), Scope.CLUSTER);
        session.commitAsync(() -> {
            committedToken = resumeToken;
            resumeFrom = resumeToken;
        }, this::onCommitFailure);
    }

    private void onCommitFailure(final Throwable failure) {
        getLogger().error("Committing change events from {} failed; the change stream will be reopened from the last committed resume token",
                transitUri, failure);
        closeCursor();
    }

    /**
     * Factory method for the change stream cursor, overridable for tests.
     */
    protected MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openCursor(final String resumeTokenData) {
        return streamOpener.open(resumeTokenData);
    }

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> getOrOpenCursor() {
        if (cursor == null) {
            cursor = openCursor(resumeFrom);
            if (resumeFrom == null) {
                // Remember where the stream begins before any event is read, so that a batch that fails to commit
                // is read again instead of being skipped.
                resumeFrom = EventMapper.resumeTokenData(cursor.getResumeToken());
            }
        }
        return cursor;
    }

    private void closeCursor() {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (final Exception e) {
                getLogger().debug("Closing the change stream of {} failed", transitUri, e);
            }
            cursor = null;
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final MongoDBClientService clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();

        final Document hello;
        try {
            hello = clientService.getDatabase(databaseName).runCommand(new Document("hello", 1));
            results.add(result("Connect to MongoDB", ConfigVerificationResult.Outcome.SUCCESSFUL,
                    String.format("Connected to %s", hello.get("me", "the server"))));
        } catch (final Exception e) {
            verificationLogger.error("Connecting to MongoDB failed", e);
            results.add(result("Connect to MongoDB", ConfigVerificationResult.Outcome.FAILED,
                    String.format("Could not connect: %s", e.getMessage())));
            return results;
        }

        results.add(verifyDeployment(hello));
        results.add(verifyServerVersion(clientService, databaseName, verificationLogger));
        return results;
    }

    private ConfigVerificationResult verifyDeployment(final Document hello) {
        final String replicaSetName = hello.getString("setName");
        if (replicaSetName != null) {
            return result("Change Streams available", ConfigVerificationResult.Outcome.SUCCESSFUL,
                    String.format("Connected to the replica set %s", replicaSetName));
        }
        if (SHARDED_CLUSTER_MESSAGE.equals(hello.getString("msg"))) {
            return result("Change Streams available", ConfigVerificationResult.Outcome.SUCCESSFUL, "Connected to a sharded cluster");
        }
        return result("Change Streams available", ConfigVerificationResult.Outcome.FAILED,
                "The server is a standalone server. Change Streams need a replica set or a sharded cluster.");
    }

    private ConfigVerificationResult verifyServerVersion(final MongoDBClientService clientService, final String databaseName,
                                                         final ComponentLog verificationLogger) {
        final String version;
        final int majorVersion;
        try {
            final Document buildInfo = clientService.getDatabase(databaseName).runCommand(new Document("buildInfo", 1));
            version = buildInfo.getString("version");
            majorVersion = buildInfo.getList("versionArray", Number.class).get(0).intValue();
        } catch (final Exception e) {
            verificationLogger.warn("Reading the server version failed", e);
            return result("Server version supported", ConfigVerificationResult.Outcome.SKIPPED,
                    String.format("Could not read the server version, the user may not be allowed to run buildInfo: %s", e.getMessage()));
        }

        if (majorVersion < MINIMUM_MAJOR_VERSION) {
            return result("Server version supported", ConfigVerificationResult.Outcome.FAILED,
                    String.format("MongoDB %s is older than the required version %d.0", version, MINIMUM_MAJOR_VERSION));
        }
        return result("Server version supported", ConfigVerificationResult.Outcome.SUCCESSFUL, String.format("MongoDB %s", version));
    }

    private static ConfigVerificationResult result(final String step, final ConfigVerificationResult.Outcome outcome, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(step)
                .outcome(outcome)
                .explanation(explanation)
                .build();
    }

    /**
     * The connection string without any credentials that may be embedded in it, so that provenance never carries a password.
     */
    private static String buildTransitUri(final String uri, final String databaseName, final String collectionName) {
        final String withoutCredentials = uri == null ? "" : uri.replaceAll("://[^@/]*@", "://");
        final String withoutTrailingSlash = withoutCredentials.endsWith("/")
                ? withoutCredentials.substring(0, withoutCredentials.length() - 1)
                : withoutCredentials;
        return String.format("%s/%s.%s", withoutTrailingSlash, databaseName, collectionName);
    }
}
