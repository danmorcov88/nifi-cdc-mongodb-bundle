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

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.bson.BsonDocument;

import java.util.concurrent.TimeUnit;

/**
 * Opens the change stream cursor for the watched collection. Without a resume token the stream starts at the
 * current moment; with one it continues after the event the token belongs to.
 */
class StreamOpener {

    private final MongoDBClientService clientService;
    private final String databaseName;
    private final String collectionName;
    private final long maxAwaitTimeMillis;

    StreamOpener(final MongoDBClientService clientService, final String databaseName, final String collectionName, final long maxAwaitTimeMillis) {
        this.clientService = clientService;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.maxAwaitTimeMillis = maxAwaitTimeMillis;
    }

    MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> open(final String resumeTokenData) {
        ChangeStreamIterable<BsonDocument> stream = clientService.getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class)
                .watch();

        final BsonDocument resumeToken = EventMapper.resumeToken(resumeTokenData);
        if (resumeToken != null) {
            stream = stream.resumeAfter(resumeToken);
        }

        // Bounds how long tryNext() waits on an idle stream, so that a trigger returns promptly.
        return stream.maxAwaitTime(maxAwaitTimeMillis, TimeUnit.MILLISECONDS).cursor();
    }
}
