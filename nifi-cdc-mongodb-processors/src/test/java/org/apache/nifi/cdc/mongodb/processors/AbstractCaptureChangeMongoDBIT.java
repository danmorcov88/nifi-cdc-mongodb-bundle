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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the processor against a real single node replica set. The subclasses pick the server version.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractCaptureChangeMongoDBIT {

    private static final String DATABASE = "lab";
    private static final String COLLECTION = "orders";
    private static final int MAX_TRIGGERS = 40;

    private MongoDBContainer container;
    private MongoClient client;
    private MongoCollection<Document> collection;
    private TestRunner runner;

    protected abstract String getImage();

    @BeforeAll
    void startServer() {
        container = new MongoDBContainer(DockerImageName.parse(getImage())).withReplicaSet();
        container.start();
        client = MongoClients.create(container.getReplicaSetUrl());
    }

    @AfterAll
    void stopServer() {
        if (client != null) {
            client.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void createRunner() throws Exception {
        client.getDatabase(DATABASE).drop();
        collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
        // A change stream can only be opened on a collection that exists.
        collection.insertOne(new Document("_id", "seed"));

        runner = TestRunners.newTestRunner(new CaptureChangeMongoDB());

        final MongoDBControllerService clientService = new MongoDBControllerService();
        runner.addControllerService("client-service", clientService);
        runner.setProperty(clientService, MongoDBClientService.URI, container.getReplicaSetUrl());
        runner.enableControllerService(clientService);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, JsonRecordSetWriter.OUTPUT_ONELINE.getValue());
        runner.enableControllerService(writer);

        runner.setProperty(CaptureChangeMongoDB.CLIENT_SERVICE, "client-service");
        runner.setProperty(CaptureChangeMongoDB.RECORD_WRITER, "record-writer");
        runner.setProperty(CaptureChangeMongoDB.DATABASE_NAME, DATABASE);
        runner.setProperty(CaptureChangeMongoDB.COLLECTION_NAME, COLLECTION);
        runner.setProperty(CaptureChangeMongoDB.MAX_AWAIT_TIME, "250 ms");
        runner.setProperty(CaptureChangeMongoDB.MAX_BATCH_DURATION, "1 s");
    }

    @Test
    void everyOperationIsCapturedInOrder() {
        startStream();

        collection.insertOne(new Document("_id", 1).append("total", 10));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 20));
        collection.replaceOne(Filters.eq("_id", 1), new Document("_id", 1).append("total", 30));
        collection.deleteOne(Filters.eq("_id", 1));

        final List<Document> records = readRecords(4);

        assertEquals(List.of("insert", "update", "replace", "delete"),
                records.stream().map(record -> record.getString("operation")).toList());
        records.forEach(record -> {
            assertEquals(DATABASE, record.getString("database"));
            assertEquals(COLLECTION, record.getString("collection"));
            assertEquals("{\"_id\": 1}", record.getString("document_key"));
            assertNotNull(record.getString("resume_token"));
            assertTrue(record.get("cluster_time", Number.class).longValue() > 0L);
        });

        assertEquals("{\"_id\": 1, \"total\": 10}", records.get(0).getString("full_document"));
        assertEquals("{\"total\": 20}", records.get(1).getString("updated_fields"));
        assertEquals("{\"_id\": 1, \"total\": 30}", records.get(2).getString("full_document"));
        assertNull(records.get(3).getString("full_document"));
    }

    @Test
    void changesMadeWhileTheProcessorIsStoppedAreReadAfterTheRestart() throws Exception {
        startStream();
        collection.insertOne(new Document("_id", 0));
        assertEquals(1, readRecords(1).size());
        assertNotNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN));

        runner.run(1, true, false);
        runner.clearTransferState();

        for (int id = 1; id <= 100; id++) {
            collection.insertOne(new Document("_id", id));
        }

        runner.run(1, false, true);
        final List<Document> records = readRecords(100);

        assertEquals(100, records.size());
        assertEquals(List.of(), records.stream().filter(record -> !"insert".equals(record.getString("operation"))).toList());
        assertEquals(IntStream.rangeClosed(1, 100).boxed().toList(),
                records.stream().map(record -> Document.parse(record.getString("document_key")).getInteger("_id")).toList());
    }

    /**
     * The processor must be usable with a read-only user, so none of what it does may be a write. The counters are
     * compared around the reading of the events.
     */
    @Test
    void readingChangesWritesNothingToTheServer() {
        startStream();
        collection.insertOne(new Document("_id", 1));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 5));
        collection.deleteOne(Filters.eq("_id", 1));

        final Document before = opcounters();
        assertEquals(3, readRecords(3).size());
        final Document after = opcounters();

        for (final String counter : List.of("insert", "update", "delete")) {
            assertEquals(before.get(counter, Number.class).longValue(), after.get(counter, Number.class).longValue(),
                    String.format("the processor performed a %s on the server", counter));
        }
    }

    @Test
    void verifyReportsTheConnectionTheDeploymentAndTheVersion() {
        final List<ConfigVerificationResult> results =
                ((CaptureChangeMongoDB) runner.getProcessor()).verify(runner.getProcessContext(), runner.getLogger(), Map.of());

        assertEquals(3, results.size());
        results.forEach(result -> assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(),
                result.getVerificationStepName() + ": " + result.getExplanation()));
    }

    /**
     * Opens the change stream, so that the changes made afterwards fall inside the window the processor watches.
     */
    private void startStream() {
        runner.run(1, false, true);
        runner.clearTransferState();
    }

    private List<Document> readRecords(final int expected) {
        final List<Document> records = new ArrayList<>();
        for (int trigger = 0; trigger < MAX_TRIGGERS && records.size() < expected; trigger++) {
            runner.run(1, false, false);
            for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS)) {
                flowFile.getContent().lines().filter(line -> !line.isBlank()).map(Document::parse).forEach(records::add);
            }
            runner.clearTransferState();
        }
        return records;
    }

    private Document opcounters() {
        return client.getDatabase("admin").runCommand(new Document("serverStatus", 1)).get("opcounters", Document.class);
    }
}
