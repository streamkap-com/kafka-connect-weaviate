/*
 * Copyright © 2025 Weaviate
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.weaviate.connector;

import com.streamkap.common.util.RecordUtils;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.connector.converter.DataConverter;
import io.weaviate.connector.idstrategy.IDStrategy;
import io.weaviate.connector.vectorstrategy.VectorStrategy;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Handles the processing of individual SinkRecords with retry logic.
 * Supports record transformations, tombstone handling, and batching.
 */
public class RecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

    private final WeaviateClient client;
    private final ObjectsBatcher objectsBatcher;
    private final WeaviateSinkConfig config;
    private final IDStrategy idStrategy;
    private final VectorStrategy vectorStrategy;
    private final String collectionMappingRule;
    private final int maxRetries;
    private final long retryBackoffMs;

    public RecordProcessor(
            WeaviateClient client,
            ObjectsBatcher objectsBatcher,
            WeaviateSinkConfig config,
            IDStrategy idStrategy,
            VectorStrategy vectorStrategy,
            String collectionMappingRule,
            int maxRetries,
            long retryBackoffMs) {
        this.client = client;
        this.objectsBatcher = objectsBatcher;
        this.config = config;
        this.idStrategy = idStrategy;
        this.vectorStrategy = vectorStrategy;
        this.collectionMappingRule = collectionMappingRule;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Processes a single SinkRecord with retry logic.
     * Retries up to maxRetries times with exponential backoff for retriable exceptions.
     *
     * @param record the SinkRecord to process
     * @param dataConverter the converter to transform Kafka data to Weaviate format
     * @throws RetriableException if all retries are exhausted for a retriable error
     * @throws RuntimeException for non-retriable errors
     */
    public void processRecordWithRetries(SinkRecord record, DataConverter dataConverter) throws RetriableException {
        int remainingRetries = maxRetries;

        while (remainingRetries > 0) {
            try {
                log.debug("Processing record from topic: {}. Remaining retries: {}", record.topic(), remainingRetries);
                processRecord(record, dataConverter);
                return; // Exit if successful
            } catch (RetriableException e) {
                remainingRetries--;
                log.warn("Retriable exception occurred for record. Attempt {}/{}. Remaining retries: {}",
                         maxRetries - remainingRetries, maxRetries, remainingRetries, e);

                if (remainingRetries <= 0) {
                    log.error("Exhausted retries for record: {}", record.topic());
                    throw e; // Rethrow if no retries left
                }

                try {
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry backoff interrupted", interruptedException);
                }
            }
        }
    }

    /**
     * Processes a single record, handling either data insertion or tombstone deletion.
     *
     * @param record the SinkRecord to process
     * @param dataConverter the converter for data transformation
     * @throws RetriableException for retriable errors during processing
     */
    private void processRecord(SinkRecord record, DataConverter dataConverter) throws RetriableException {
        if (record.value() == null) {
            handleTombstone(record);
            return;
        } else if (config.getDeleteEnabled() && isDeleteOperation(record)) {
            handleTombstone(record);
            return;
        }

        Map<String, Object> properties = dataConverter.convertToWeaviateProperties(record.valueSchema(), record.value());
        String collectionName = getCollectionName(record.topic());
        log.debug("Processing record for collection: {} with properties: {}", collectionName, properties);

        objectsBatcher.withObject(WeaviateObject.builder()
                .className(collectionName)
                .properties(properties)
                .id(idStrategy.getDocumentId(record, properties))
                .vector(vectorStrategy.getDocumentVector(record, properties))
                .build());
    }

    private boolean isDeleteOperation(SinkRecord record) {
        var valueStruct = RecordUtils.requireStruct(record.value());
        var __deleted = valueStruct.schema().field("__deleted") != null ? valueStruct.get("__deleted") : "false";
        return __deleted.toString().equalsIgnoreCase("true");
    }

    /**
     * Handles deletion of a record (tombstone).
     * Only processes if delete operations are enabled in configuration.
     *
     * @param record the tombstone SinkRecord
     */
    private void handleTombstone(SinkRecord record) {
        if (config.getDeleteEnabled()) {
            String collectionName = getCollectionName(record.topic());
            client.data().deleter()
                    .withClassName(collectionName)
                    .withID(idStrategy.getDocumentId(record, null))
                    .withConsistencyLevel(config.getConsistencyLevel().name())
                    .run();

            log.info("Tombstone processed for collection: {} with ID: {}", collectionName,
                    idStrategy.getDocumentId(record, null));
        }
    }

    /**
     * Converts a topic name to a collection name using the mapping rule.
     *
     * @param topic the Kafka topic name
     * @return the mapped collection name
     */
    public String getCollectionName(String topic) {
        return collectionMappingRule.replace("${topic}", topic);
    }
}

