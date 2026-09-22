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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.List;
import java.util.Set;

@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"mongodb", "cdc", "change stream", "replication", "event"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from MongoDB using Change Streams. The processor reads insert, update, "
        + "replace, delete and invalidate events for a collection or a database and writes them as records using the configured Record "
        + "Writer, one FlowFile per batch. Each record has the fields operation, database, collection, document_key, full_document, "
        + "full_document_before_change, updated_fields, removed_fields, cluster_time, wall_time, txn_number and resume_token. Documents "
        + "are written as Extended JSON strings. The resume token is stored only after the FlowFiles of a batch have been committed, so "
        + "every change is delivered at least once. The processor never writes to the source database; it needs only the changeStream "
        + "and find privileges on the watched scope. MongoDB 6.0 or later, running as a replica set or a sharded cluster, is required.")
@Stateful(scopes = Scope.CLUSTER, description = "The resume token of the last change event written to a FlowFile is stored so that the "
        + "processor resumes from the same position after a restart or a change of the primary node.")
@WritesAttributes({
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_DATABASE, description = "Database the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_COLLECTION, description = "Collection the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, description = "Resume token of the first change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, description = "Resume token of the last change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS,
                description = "Milliseconds between the cluster time of the last change event in the FlowFile and the time the FlowFile was written"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, description = "Number of records written to the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the Record Writer")
})
public class CaptureChangeMongoDB extends AbstractProcessor {

    public static final String ATTRIBUTE_DATABASE = "mongodb.database";
    public static final String ATTRIBUTE_COLLECTION = "mongodb.collection";
    public static final String ATTRIBUTE_FIRST_RESUME_TOKEN = "cdc.first.resume.token";
    public static final String ATTRIBUTE_LAST_RESUME_TOKEN = "cdc.last.resume.token";
    public static final String ATTRIBUTE_LAG_MILLIS = "cdc.lag.millis";
    public static final String ATTRIBUTE_RECORD_COUNT = "record.count";

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Client Service")
            .description("The MongoDB client service that provides the connection to the replica set or the sharded cluster. "
                    + "Credentials and connection timeouts are configured there.")
            .identifiesControllerService(MongoDBClientService.class)
            .required(true)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("Name of the database to watch.")
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
            RECORD_WRITER);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        throw new ProcessException("CaptureChangeMongoDB does not read change events yet. This build contains the module skeleton only.");
    }
}
