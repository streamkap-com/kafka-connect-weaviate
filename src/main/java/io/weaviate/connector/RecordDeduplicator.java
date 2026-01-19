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

import io.weaviate.connector.converter.DataConverter;
import io.weaviate.connector.idstrategy.IDStrategy;
import io.weaviate.connector.idstrategy.NoIdStrategy;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplicates SinkRecords by document ID to handle out-of-order deletes and inserts.
 * Ensures only the latest version of each record is processed, preventing inconsistent data
 * when delete operations arrive before insert operations due to batching.
 *
 * Deduplication is skipped if NoIdStrategy is used, as records don't have stable identifiers.
 */
public class RecordDeduplicator {
    private static final Logger log = LoggerFactory.getLogger(RecordDeduplicator.class);

    private final IDStrategy documentIdStrategy;
    private final DataConverter dataConverter;

    public RecordDeduplicator(IDStrategy documentIdStrategy, DataConverter dataConverter) {
        this.documentIdStrategy = documentIdStrategy;
        this.dataConverter = dataConverter;
    }

    /**
     * Deduplicates records by document ID, keeping only the latest version of each record.
     * Skips deduplication if using NoIdStrategy since records don't have stable identifiers.
     * Uses insertion order preservation to maintain Kafka ordering semantics.
     *
     * @param records list of SinkRecords to deduplicate
     * @return map of deduplicated records keyed by document ID, or original list if NoIdStrategy
     */
    public Map<String, SinkRecord> deduplicate(List<SinkRecord> records) {
        // Skip deduplication if using NoIdStrategy - records don't have stable identifiers
        if (documentIdStrategy instanceof NoIdStrategy) {
            log.debug("NoIdStrategy detected - skipping deduplication");
            Map<String, SinkRecord> noDedup = new LinkedHashMap<>();
            for (int i = 0; i < records.size(); i++) {
                // Use index as key to preserve order without actual deduplication
                noDedup.put(String.valueOf(i), records.get(i));
            }
            return noDedup;
        }

        Map<String, SinkRecord> deduplicatedRecords = new LinkedHashMap<>();

        for (SinkRecord record : records) {
            String recordKey = getRecordKey(record);
            if (recordKey != null) {
                deduplicatedRecords.put(recordKey, record);
            } else {
                // If unable to extract key, include the record as-is
                deduplicatedRecords.put(record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset(), record);
            }
        }

        if (deduplicatedRecords.size() < records.size()) {
            log.debug("Deduplicated {} records to {} unique keys",
                    records.size(), deduplicatedRecords.size());
        }

        return deduplicatedRecords;
    }

    /**
     * Extracts the unique key for a record using the document ID strategy.
     * Returns null if no valid ID can be extracted.
     *
     * @param record the SinkRecord to extract key from
     * @return string representation of the document ID, or null if unable to extract
     */
    private String getRecordKey(SinkRecord record) {
        try {
            // Convert record value to properties map for ID strategy
            Map<String, Object> valueProperties = dataConverter.convertToWeaviateProperties(
                    record.valueSchema(),
                    record.value()
            );

            // Get document ID using the strategy
            String documentId = documentIdStrategy.getDocumentId(record, valueProperties);

            if (documentId != null && !documentId.isEmpty()) {
                return documentId;
            }

            log.debug("Document ID is null/empty for record at offset {}", record.kafkaOffset());
            return null;
        } catch (Exception e) {
            log.warn("Failed to extract document ID for record at offset {}: {}",
                    record.kafkaOffset(), e.getMessage());
            return null;
        }
    }
}

