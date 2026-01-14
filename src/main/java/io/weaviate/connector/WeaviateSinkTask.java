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

import com.streamkap.common.util.AutoMagicSchemaMaintenance;
import io.grpc.NameResolverRegistry;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.connector.converter.DataConverter;
import io.weaviate.connector.idstrategy.IDStrategy;
import io.weaviate.connector.vectorstrategy.VectorStrategy;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WeaviateSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(WeaviateSinkTask.class);
    private static final DataConverter DATA_CONVERTER = new DataConverter();

    private WeaviateClient client;
    private WeaviateSinkConfig config;
    private ObjectsBatcher objectsBatcher;

    private IDStrategy documentIdStrategy;
    private VectorStrategy vectorStrategy;
    private String collectionMappingRule;

    private WeaviateSchemaManager schemaManager;
    private RecordProcessor recordProcessor;
    private AutoMagicSchemaMaintenance autoMagicSchemaMaintenance;

    /** Captures async errors from the batch callback for propagation to Kafka Connect */
    private final AtomicReference<Throwable> batchError = new AtomicReference<>();

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new WeaviateSinkConfig(WeaviateSinkConfig.CONFIG_DEF, map);
        this.collectionMappingRule = config.getCollectionMapping();

        autoMagicSchemaMaintenance = new AutoMagicSchemaMaintenance(
                !config.schemaEvolutionMode.equals(WeaviateSinkConfig.SchemaEvolutionMode.NONE),
                !config.schemaEvolutionMode.equals(WeaviateSinkConfig.SchemaEvolutionMode.NONE));

        // Build Weaviate client using builder
        this.client = WeaviateClientBuilder.buildClient(config);

        // Initialize schema manager
        this.schemaManager = new WeaviateSchemaManager(client, config, autoMagicSchemaMaintenance);

        // Initialize ID and vector strategies
        try {
            this.documentIdStrategy = (IDStrategy) config.getDocumentIdStrategy().getDeclaredConstructor().newInstance();
            this.documentIdStrategy.configure(config);
        } catch (Exception e) {
            throw new RuntimeException("Can not instantiate DocumentIDStrategy class", e);
        }

        try {
            this.vectorStrategy = (VectorStrategy) config.getVectorStrategy().getDeclaredConstructor().newInstance();
            this.vectorStrategy.configure(config);
        } catch (Exception e) {
            throw new RuntimeException("Can not instantiate VectorStrategy class", e);
        }

        // Trigger classloader to detect missing GRPC packages early
        NameResolverRegistry.getDefaultRegistry().getDefaultScheme();

        // Initialize batcher and record processor
        initializeObjectsBatcher();
        this.recordProcessor = new RecordProcessor(
                client,
                objectsBatcher,
                config,
                documentIdStrategy,
                vectorStrategy,
                collectionMappingRule,
                config.getRetryMax(),
                config.getRetryBackoffMs()
        );
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        checkBatchError();

        if (collection.isEmpty()) {
            return;
        }

        log.debug("Received {} records", collection.size());

        final Map<String, List<SinkRecord>> recordsByCollection = new HashMap<>();
        for (SinkRecord record : collection) {
            final String collectionId = recordProcessor.getCollectionName(record.topic());
            recordsByCollection.computeIfAbsent(collectionId, k -> new java.util.ArrayList<>())
                    .add(record);
        }

        for (Map.Entry<String, List<SinkRecord>> entry : recordsByCollection.entrySet()) {
            String collectionId = entry.getKey();
            List<SinkRecord> records = entry.getValue();
            schemaManager.validateAndCreateCollectionIfNeeded(collectionId);

            List<SinkRecord> updatedRecords;
            if (config.getApplyAutomagicSchemaMaintenanceOnTopOfDbSchema()) {
                log.debug("Applying automagic schema maintenance for collection: {}", collectionId);
                updatedRecords = schemaManager.applyAutomagicSchemaMaintenance(collectionId, records);
            } else {
                updatedRecords = records;
            }

            log.info("Processing {} records for collection: {}", updatedRecords.size(), collectionId);
            for (SinkRecord record : updatedRecords) {
                try {
                    recordProcessor.processRecordWithRetries(record, DATA_CONVERTER);
                } catch (RetriableException e) {
                    log.error("Retriable exception after max retries for record: {}", record, e);
                    throw e;
                } catch (Exception e) {
                    log.error("Non-retriable exception for record: {}", record, e);
                    throw new ConnectException("Failed to process record", e);
                }
            }
        }

        flushBatch();
        checkBatchError();
    }

    private void flushBatch() {
        log.debug("Flushing batch");
        try {
            objectsBatcher.flush();
        } catch (Exception e) {
            log.error("Flush failed - collection may not exist or be inaccessible", e);
            throw new ConnectException("Batch flush failed", e);
        }
    }

    private void checkBatchError() {
        Throwable error = batchError.getAndSet(null);
        if (error != null) {
            throw new ConnectException("Async batch operation failed", error);
        }
    }


    private void initializeObjectsBatcher() {
        objectsBatcher = client.batch().objectsAutoBatcher(
                ObjectsBatcher.BatchRetriesConfig.builder()
                        .maxConnectionRetries(config.getMaxConnectionRetries())
                        .maxTimeoutRetries(config.getMaxTimeoutRetries())
                        .retriesIntervalMs(config.getRetryInterval())
                        .build(),
                ObjectsBatcher.AutoBatchConfig.builder()
                        .batchSize(config.getBatchSize())
                        .poolSize(config.getPoolSize())
                        .awaitTerminationMs(config.getAwaitTerminationMs())
                        .callback(this::handleBatchResult)
                        .build()
        );
        objectsBatcher.withConsistencyLevel(config.getConsistencyLevel().name());
    }

    private void handleBatchResult(Result<ObjectGetResponse[]> result) {
        try {
            if (result == null) {
                batchError.compareAndSet(null, new RuntimeException("Weaviate batch result is null"));
                return;
            }

            if (result.hasErrors()) {
                batchError.compareAndSet(null, new RuntimeException(
                        "Weaviate batch ingestion failed: " + result.getError().getMessages()));
                return;
            }

            ObjectGetResponse[] responses = result.getResult();
            if (responses != null) {
                for (ObjectGetResponse r : responses) {
                    if (r.getResult() != null && "FAILED".equalsIgnoreCase(r.getResult().getStatus())) {
                        batchError.compareAndSet(null, new RuntimeException(
                                "Weaviate object failed: id=" + r.getId()
                                        + ", errors=" + r.getResult().getErrors().getError()));
                        return;
                    }
                }
            }
        } catch (Exception e) {
            batchError.compareAndSet(null, e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        super.flush(currentOffsets);
        flushBatch();
        checkBatchError();
    }

    @Override
    public void stop() {
        if (objectsBatcher != null) {
            try {
                objectsBatcher.close();
            } catch (Exception e) {
                log.warn("Error closing objectsBatcher", e);
            }
        }
        // WeaviateClient doesn't implement AutoCloseable; no explicit cleanup required
        client = null;
    }
}
