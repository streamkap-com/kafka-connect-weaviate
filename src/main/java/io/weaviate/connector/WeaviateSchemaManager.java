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
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.misc.model.InvertedIndexConfig;
import io.weaviate.client.v1.misc.model.VectorIndexConfig;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.WeaviateClass;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.weaviate.connector.WeaviateSinkConfig.SchemaEvolutionMode.NONE;

/**
 * Manages Weaviate schema operations including validation and creation of collections.
 */
public class WeaviateSchemaManager {
    private static final Logger log = LoggerFactory.getLogger(WeaviateSchemaManager.class);

    private final WeaviateClient client;
    private final WeaviateSinkConfig config;
    private final AutoMagicSchemaMaintenance autoMagicSchemaMaintenance;

    public WeaviateSchemaManager(WeaviateClient client, WeaviateSinkConfig config, AutoMagicSchemaMaintenance autoMagicSchemaMaintenance) {
        this.client = client;
        this.config = config;
        this.autoMagicSchemaMaintenance = autoMagicSchemaMaintenance;
    }

    /**
     * Validates that a collection exists in Weaviate. If it doesn't exist, creates it automatically.
     *
     * @param collectionName the name of the collection to validate/create
     * @throws RuntimeException if validation or creation fails
     */
    public void validateAndCreateCollectionIfNeeded(String collectionName) {
        try {
            if (!collectionExists(collectionName)) {
                if (NONE.equals(config.getSchemaEvolutionMode())){
                    throw new RuntimeException("Collection '" + collectionName + "' does not exist in Weaviate. Please create the collection before starting the connector.");
                } else {
                    log.info("Collection '{}' does not exist. Creating new collection...", collectionName);
                    createCollection(collectionName);
                }
            }
        } catch (Exception e) {
            log.error("Failed to validate or create collection: {}", collectionName, e);
            throw new RuntimeException("Collection validation/creation failed for: " + collectionName, e);
        }
    }

    /**
     * Checks if a collection exists in Weaviate schema.
     *
     * @param collectionName the name of the collection to check
     * @return true if collection exists, false otherwise
     */
    private boolean collectionExists(String collectionName) {
        return getCollection(collectionName) != null;
    }

    /**
     * Get details if a collection exists in Weaviate schema.
     *
     * @param collectionName the name of the collection to check
     * @return collection details
     */
    private WeaviateClass getCollection(String collectionName) {
        try {
            return client.schema().classGetter()
                    .withClassName(collectionName)
                    .run()
                    .getResult();
        } catch (Exception e) {
            log.warn("Error checking collection existence for '{}': {}", collectionName, e.getMessage());
            return null;
        }
    }

    /**
     * Creates a new Weaviate collection with default configuration.
     * Uses text2vec-weaviate vectorizer with Snowflake Arctic Embed model.
     *
     * @param collectionName the name of the collection to create
     * @throws RuntimeException if collection creation fails
     */
    private void createCollection(String collectionName) {
        try {
            WeaviateClass weaviateClass = buildDefaultWeaviateClass(collectionName);

            client.schema().classCreator()
                    .withClass(weaviateClass)
                    .run();

            log.info("Successfully created collection: {}", collectionName);
        } catch (Exception e) {
            log.error("Failed to create collection: {}", collectionName, e);
            throw new RuntimeException("Collection creation failed for: " + collectionName, e);
        }
    }

    /**
     * Builds a default WeaviateClass configuration for auto-created collections.
     *
     * @param collectionName the name of the collection
     * @return configured WeaviateClass
     */
    private WeaviateClass buildDefaultWeaviateClass(String collectionName) {
        return WeaviateClass.builder()
                .className(collectionName)
                .description("Auto-created collection from Streamkap for topic: " + collectionName)
                .vectorizer("text2vec-weaviate")
                .vectorIndexType("hnsw")
                .vectorIndexConfig(VectorIndexConfig.builder().distance("cosine").build())
                .invertedIndexConfig(InvertedIndexConfig.builder().build())
                .moduleConfig(Map.of(
                        "text2vec-weaviate", Map.of(
                                "model", "Snowflake/snowflake-arctic-embed-l-v2.0",
                                "dimensions", 1024,
                                "vectorizeClassName", false
                        )
                ))
                .properties(List.of(
                        Property.builder()
                                .name("")  // Dummy property like UI
                                .description("")
                                .dataType(List.of("string"))
                                .indexFilterable(true)
                                .build()
                ))
                .build();
    }

    public List<SinkRecord> applyAutomagicSchemaMaintenance(String collectionId, List<SinkRecord> records) {

        ImmutablePair<Schema, Schema> previousKeyValueSchema = new ImmutablePair<>(null, null);
        final WeaviateClass collection = getCollection(collectionId);
        if(collection!= null) {
            previousKeyValueSchema = getKCSchemaFromWeaviateCollection(collection);
        }

        return autoMagicSchemaMaintenance.normalizeRecordsOptInferSchema(
                records,
                previousKeyValueSchema.left,
                previousKeyValueSchema.right);
    }

    public ImmutablePair<Schema, Schema> getKCSchemaFromWeaviateCollection(WeaviateClass collection) {

        SchemaBuilder valueSchema = SchemaBuilder.struct()
                .name(collection.getClassName() + ".Value");

        SchemaBuilder keySchema = SchemaBuilder.struct()
                .name(collection.getClassName() + ".Key");

        if (collection.getProperties() == null) {
            return new ImmutablePair<>(keySchema.build(), valueSchema.build());
        }

        for (Property property : collection.getProperties()) {

            // Skip invalid or dummy properties
            if (property.getName() == null || property.getName().isBlank()) {
                continue;
            }

            // Never map reserved Weaviate fields
            if ("id".equals(property.getName())) {
                continue;
            }

            Schema fieldSchema = buildFieldSchema(property);
            valueSchema.field(property.getName(), fieldSchema);
        }

        return new ImmutablePair<>(keySchema.build(), valueSchema.build());
    }


    private Schema buildFieldSchema(Property property) {

        List<String> dataTypes = property.getDataType();
        if (dataTypes == null || dataTypes.isEmpty()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        String type = dataTypes.get(0).toLowerCase();

        boolean isArray = type.endsWith("[]");
        String baseType = isArray ? type.substring(0, type.length() - 2) : type;

        Schema baseSchema;

        if ("string".equals(baseType) ||
                "text".equals(baseType) ||
                "uuid".equals(baseType) ||
                "phonenumber".equalsIgnoreCase(baseType)) {

            baseSchema = Schema.OPTIONAL_STRING_SCHEMA;

        } else if ("int".equals(baseType)) {

            baseSchema = Schema.OPTIONAL_INT64_SCHEMA;

        } else if ("number".equals(baseType)) {

            baseSchema = Schema.OPTIONAL_FLOAT64_SCHEMA;

        } else if ("boolean".equals(baseType)) {

            baseSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;

        } else if ("date".equals(baseType)) {

            baseSchema = Timestamp.builder().optional().build();

        } else if ("geocoordinates".equalsIgnoreCase(baseType)) {

            baseSchema = SchemaBuilder.struct()
                    .optional()
                    .field("latitude", Schema.OPTIONAL_FLOAT64_SCHEMA)
                    .field("longitude", Schema.OPTIONAL_FLOAT64_SCHEMA)
                    .build();

        } else {

            // Fallback for unknown types
            baseSchema = Schema.OPTIONAL_STRING_SCHEMA;
        }

        if (isArray) {
            return SchemaBuilder.array(baseSchema).optional().build();
        }

        return baseSchema;
    }

}

