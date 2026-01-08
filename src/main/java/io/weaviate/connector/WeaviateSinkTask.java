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
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.api.ObjectsBatcher.AutoBatchConfig;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.connector.converter.DataConverter;
import io.weaviate.connector.idstrategy.IDStrategy;
import io.weaviate.connector.vectorstrategy.VectorStrategy;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WeaviateSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(WeaviateSinkTask.class);

    private WeaviateClient client;
    private WeaviateSinkConfig config;
    private ObjectsBatcher objectsBatcher;

    private IDStrategy documentIdStrategy;
    private VectorStrategy vectorStrategy;
    private String collectionMappingRule;

    private WeaviateSchemaManager schemaManager;
    private RecordProcessor recordProcessor;
    AutoMagicSchemaMaintenance autoMagicSchemaMaintenance;

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

        // Getting GRPC default registry to trigger Classloader issues if there
        // are missing GRPC packages
        NameResolverRegistry defaultRegistry = NameResolverRegistry.getDefaultRegistry();
        defaultRegistry.getDefaultScheme();
    }


    @Override
    public void put(Collection<SinkRecord> collection) {
        if (objectsBatcher == null) {
            initializeObjectsBatcher();
            // Initialize record processor after objectsBatcher is ready
            recordProcessor = new RecordProcessor(
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

        final Map<String, List<SinkRecord>> recordsByCollection = new HashMap<>();
        for (SinkRecord record : collection) {
            final String collectionId = recordProcessor.getCollectionName(record.topic());
            recordsByCollection.computeIfAbsent(collectionId, k -> new java.util.ArrayList<>())
                    .add(record);
        }

        // Process all records with retry logic
        DataConverter dataConverter = new DataConverter();

        for (Map.Entry<String, List<SinkRecord>> entry : recordsByCollection.entrySet()) {
            // Validate collections exist
            String collectionId = entry.getKey();
            List<SinkRecord> records = entry.getValue();
            schemaManager.validateAndCreateCollectionIfNeeded(collectionId);

            List<SinkRecord> updatedRecords;
            if (config.getApplyAutomagicSchemaMaintenanceOnTopOfDbSchema()) {
                log.info("Applying automagic schema maintenance for collection: {}", collectionId);
                updatedRecords = schemaManager.applyAutomagicSchemaMaintenance(
                        collectionId,
                        records);
            } else {
                updatedRecords = records;
            }

            for (SinkRecord record : updatedRecords) {
                try {
                    recordProcessor.processRecordWithRetries(record, dataConverter);
                } catch (RetriableException e) {
                    log.error("Retriable exception occurred after max retries for record: {}", record, e);
                    throw e;
                } catch (Exception e) {
                    log.error("Non-retriable exception occurred for record: {}", record, e);
                    throw new RuntimeException("Failed to process record", e);
                }
            }

            log.info("Flushing records: {}", collection.size());
            try {
                objectsBatcher.flush();
            } catch (Exception e) {
                log.error("Flush failed - collection may not exist or be inaccessible", e);
                throw new RuntimeException("Batch flush failed", e);
            }
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
                        .callback(result -> {
                            try {
                                if (result == null) {
                                    throw new RuntimeException("Weaviate batch result is null");
                                }

                                if (result.hasErrors()) {
                                    throw new RuntimeException(
                                            "Weaviate batch ingestion failed: "
                                                    + result.getError().getMessages()
                                    );
                                }

                                ObjectGetResponse[] responses = result.getResult();
                                if (responses != null) {
                                    for (ObjectGetResponse r : responses) {
                                        if (r.getResult() != null &&
                                                "FAILED".equalsIgnoreCase(r.getResult().getStatus())) {

                                            throw new RuntimeException(
                                                    "Weaviate object failed: id=" + r.getId()
                                                            + ", errors=" + r.getResult().getErrors().getError()
                                            );
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                // Ensure Connect sees this as fatal
                                throw new RuntimeException("Weaviate batch callback failed", e);
                            }
                        })
                        .build()
        );
        objectsBatcher.withConsistencyLevel(config.getConsistencyLevel().name());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        super.flush(currentOffsets);
        if (objectsBatcher != null) {
            objectsBatcher.flush();
        }
    }

    @Override
    public void stop() {
        if (objectsBatcher != null) {
            objectsBatcher.close();
        }
    }
}
