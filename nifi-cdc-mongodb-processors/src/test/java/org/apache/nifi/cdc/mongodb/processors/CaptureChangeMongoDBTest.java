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
import org.apache.nifi.cdc.mongodb.event.ChangeEvents;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the processor with a fake cursor, so the batch loop, the stored resume token and the behaviour after a
 * failure are covered without a server.
 */
class CaptureChangeMongoDBTest {

    private static final String INITIAL_TOKEN = "token-at-open";

    private final TestableProcessor processor = new TestableProcessor();

    @Test
    void eventsAreWrittenAsOneFlowFileAndTheLastTokenIsStored() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1", "t2", "t3")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, "t1");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_DATABASE, "lab");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_COLLECTION, "orders");
        flowFile.assertAttributeExists(CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS);

        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t3", Scope.CLUSTER);
        assertEquals(Collections.singletonList(null), processor.openedFrom);
    }

    @Test
    void maxEventsPerFlowFileEndsTheBatch() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1", "t2", "t3", "t4")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.setProperty(CaptureChangeMongoDB.MAX_EVENTS_PER_FLOWFILE, "2");

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "2");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t2");
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t2", Scope.CLUSTER);
    }

    @Test
    void anIdleStreamStoresThePositionTheServerReports() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, List.of()));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, INITIAL_TOKEN, Scope.CLUSTER);
    }

    @Test
    void aStoredTokenIsUsedToReopenTheStreamAfterARestart() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor("ignored", events("t9")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.getStateManager().setState(java.util.Map.of(StateKeys.RESUME_TOKEN, "stored-token"), Scope.CLUSTER);

        runner.run();

        assertEquals(List.of("stored-token"), processor.openedFrom);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t9", Scope.CLUSTER);
    }

    /**
     * A stream that breaks in the middle of a batch must leave nothing behind: no FlowFile, no stored token, and
     * the reopened stream starts where the failed batch started, so the events come again.
     */
    @Test
    void aFailedBatchIsDiscardedAndItsEventsAreReadAgain() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1", "t2", "t3")).failAfter(2));
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1", "t2", "t3")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run(2, true, true);

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, "t1");

        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t3", Scope.CLUSTER);
        assertEquals(Arrays.asList(null, INITIAL_TOKEN), processor.openedFrom);
        assertTrue(processor.cursors.getFirst().isClosed(), "the broken cursor must be closed");
    }

    /**
     * The same guarantee when the Record Writer is what fails.
     */
    @Test
    void aBatchThatCannotBeWrittenLeavesNoTokenBehind() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1", "t2", "t3")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false, 2));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        runner.getStateManager().assertStateNotSet(Scope.CLUSTER);
        assertTrue(processor.cursors.getFirst().isClosed(), "the cursor must be closed so that it is reopened");
    }

    @Test
    void theTransitUriDoesNotRepeatThePasswordOfTheClientService() throws Exception {
        processor.cursors.add(new FakeChangeStreamCursor(INITIAL_TOKEN, events("t1")));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final String transitUri = provenanceEvents.getFirst().getTransitUri();
        assertFalse(transitUri.contains("s3cret"), "transit URI must not carry the password: " + transitUri);
        assertEquals("mongodb://mongo.example:27017/lab.orders", transitUri);
    }

    private TestRunner createRunner(final MockRecordWriter writer) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final FakeMongoDBClientService clientService = new FakeMongoDBClientService();
        runner.addControllerService("client-service", clientService);
        runner.enableControllerService(clientService);

        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(CaptureChangeMongoDB.CLIENT_SERVICE, "client-service");
        runner.setProperty(CaptureChangeMongoDB.RECORD_WRITER, "record-writer");
        runner.setProperty(CaptureChangeMongoDB.DATABASE_NAME, "lab");
        runner.setProperty(CaptureChangeMongoDB.COLLECTION_NAME, "orders");
        return runner;
    }

    private static List<ChangeStreamDocument<BsonDocument>> events(final String... resumeTokens) {
        return Stream.of(resumeTokens).map(ChangeEvents::insert).toList();
    }


    private static class TestableProcessor extends CaptureChangeMongoDB {

        private final List<FakeChangeStreamCursor> cursors = new ArrayList<>();
        private final List<String> openedFrom = new ArrayList<>();
        private int opened;

        @Override
        protected MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openCursor(final String resumeTokenData) {
            openedFrom.add(resumeTokenData);
            return cursors.get(opened++);
        }
    }
}
