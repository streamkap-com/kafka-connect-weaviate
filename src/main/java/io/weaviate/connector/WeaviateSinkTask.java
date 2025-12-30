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

import io.grpc.NameResolverRegistry;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.auth.exception.AuthException;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.data.model.WeaviateObject;
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
import java.util.Map;

public class WeaviateSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(WeaviateSinkTask.class);
    WeaviateClient client;
    private String collectionMappingRule;
    private IDStrategy documentIdStrategy;
    private VectorStrategy vectorStrategy;
    private ObjectsBatcher objectsBatcher;
    private WeaviateSinkConfig config;

    private int maxRetries;
    private long retryBackoffMs;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new WeaviateSinkConfig(WeaviateSinkConfig.CONFIG_DEF, map);
        this.maxRetries = config.getRetryMax();
        this.retryBackoffMs = config.getRetryBackoffMs();
        this.collectionMappingRule = config.getCollectionMapping();
        buildWeaviateClient(config);
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

    private void buildWeaviateClient(WeaviateSinkConfig config) {
        Config weaviateConfig = getConfig(config);
        if (config.getAuthMechanism() == WeaviateSinkConfig.AuthMechanism.NONE) {
            client = new WeaviateClient(weaviateConfig);
        } else if (config.getAuthMechanism() == WeaviateSinkConfig.AuthMechanism.API_KEY) {
            try {
                client = WeaviateAuthClient.apiKey(weaviateConfig, config.getApiKey());
            } catch (AuthException e) {
                throw new RuntimeException(e);
            }
        } else if (config.getAuthMechanism() == WeaviateSinkConfig.AuthMechanism.OIDC_CLIENT_CREDENTIALS) {
            try {
                client = WeaviateAuthClient.clientCredentials(weaviateConfig, config.getOidcClientSecret(), config.getOidcScopes());
            } catch (AuthException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Unknown authentication mechanism");
        }
    }

    private static Config getConfig(WeaviateSinkConfig config) {
        String scheme = config.getConnectionUrl().split("://")[0];
        String hostAndPort = config.getConnectionUrl().split("://")[1];
        Map<String, String> headers = config.getHeaders();
        Config weaviateConfig = new Config(scheme, hostAndPort, headers);
        if (config.getGrpcUrl() != null && !config.getGrpcUrl().isEmpty()) {
            weaviateConfig.setGRPCHost(config.getGrpcUrl());
            weaviateConfig.setGRPCSecured(config.getGrpcSecured());
        }
        return weaviateConfig;
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (objectsBatcher == null) {
            initializeObjectsBatcher();
        }
        DataConverter dataConverter = new DataConverter();
        for (SinkRecord record : collection) {
            try {
                processRecordWithRetries(record, dataConverter);
            } catch (RetriableException e) {
                log.error("Retriable exception occurred after max retries for record: {}", record, e);
                // Throw RetriableException to let Kafka Connect handle retry and DLQ logic
                throw e;
            } catch (Exception e) {
                log.error("Non-retriable exception occurred for record: {}", record, e);
                // Throw RuntimeException for non-retriable errors so Kafka Connect sends to DLQ
                throw new RuntimeException("Failed to process record", e);
            }
        }
        objectsBatcher.flush(); // Flushing to ease error handling
    }


    private void processRecordWithRetries(SinkRecord record, DataConverter dataConverter) throws RetriableException {
        int retries = maxRetries;
        while (retries > 0) {
            try {
                log.info("Processing record: {}. Remaining retries: {}", record, retries);
                processRecord(record, dataConverter);
                return; // Exit if successful
            } catch (RetriableException e) {
                retries--;
                log.warn("Retriable exception occurred for record: {}. Attempt {}/{}", record, retries, maxRetries, e);
                if (retries <= 0) {
                    log.error("Exhausted retries for record: {}", record);
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
    private void processRecord(SinkRecord record, DataConverter dataConverter) throws RetriableException {
        if (record.value() == null) {
            handleTombstone(record);
            return;
        }
        Map<String, Object> properties = dataConverter.convertToWeaviateProperties(record.valueSchema(), record.value());
        objectsBatcher.withObject(WeaviateObject.builder()
                .className(getCollectionName(record.topic()))
                .properties(properties)
                .id(documentIdStrategy.getDocumentId(record, properties))
                .vector(vectorStrategy.getDocumentVector(record, properties))
                .build());
    }

    private void handleTombstone(SinkRecord record) {
        if (config.getDeleteEnabled()) {
            client.data().deleter()
                    .withClassName(getCollectionName(record.topic()))
                    .withID(documentIdStrategy.getDocumentId(record, null))
                    .withConsistencyLevel(config.getConsistencyLevel().name())
                    .run();
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
                        .build()
        );
        objectsBatcher.withConsistencyLevel(config.getConsistencyLevel().name());
    }
    public String getCollectionName(String topic) {
        return collectionMappingRule.replace("${topic}", topic);
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
