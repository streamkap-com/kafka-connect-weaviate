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
package io.weaviate.connector.converter;

import com.streamkap.common.util.AutoMagicSchemaMaintenance;
import io.weaviate.connector.WeaviateSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.plexus.util.IOUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataConverterTest {

    @Test
    void structNoSchemaConvertToWeaviateProperties() throws IOException {
        DataConverter converter = new DataConverter();
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(new HashMap<>() {{
            put(JsonConverterConfig.TYPE_CONFIG, "value");
            put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        }});

        String jsonContent = IOUtil.toString(converter.getClass().getResourceAsStream("/jsonData.json"));
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test", jsonContent.getBytes(Charset.defaultCharset()));

        Map<String, Object> properties = converter.convertToWeaviateProperties(schemaAndValue.schema(), schemaAndValue.value());

        assertEquals("hello world", properties.get("text"));
        assertEquals(123L, properties.get("int"));
        assertEquals(1.23, properties.get("float"));
        assertEquals(true, properties.get("boolean"));
        assertEquals("[1.0, 2.0]", properties.get("vector").toString());
    }


    @Test
    void structWithSchemaConvertToWeaviateProperties() throws IOException {
        DataConverter converter = new DataConverter();
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(new HashMap<>() {{
            put(JsonConverterConfig.TYPE_CONFIG, "value");
            put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        }});

        String jsonContent = IOUtil.toString(converter.getClass().getResourceAsStream("/jsonDataSchema.json"));
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test", jsonContent.getBytes(Charset.defaultCharset()));

        Map<String, Object> properties = converter.convertToWeaviateProperties(schemaAndValue.schema(), schemaAndValue.value());

        assertEquals("hello world", properties.get("text"));
        assertEquals(123L, properties.get("int"));
        assertEquals(1.23, properties.get("float"));
        assertEquals(true, properties.get("boolean"));
    }

    @Test
    void structWithSchemaConvertToWeaviateProperties_int8Repro() {
        DataConverter converter = new DataConverter();

        // 1. Build a realistic Connect schema
        Schema valueSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("tinyint_col", Schema.OPTIONAL_INT8_SCHEMA)
                .field("int_col", Schema.OPTIONAL_INT32_SCHEMA)
                .field("float_col", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("bool_col", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();

        // 2. Populate Struct with real Java types
        Struct struct = new Struct(valueSchema)
                .put("tinyint_col", (byte) 1)   // <- THIS is the problematic field
                .put("int_col", 123)
                .put("float_col", 1.23d)
                .put("bool_col", true);

        // 3. Create a proper SinkRecord
        SinkRecord sinkRecord = new SinkRecord(
                "test-topic",   // topic (important for automagic)
                0,              // partition
                null,            // key schema
                null,            // key
                valueSchema,     // value schema
                struct,          // value
                0L               // offset
        );

        AutoMagicSchemaMaintenance autoMagicSchemaMaintenance = new AutoMagicSchemaMaintenance(
                true,
                true);
        SinkRecord updatedRecord = autoMagicSchemaMaintenance.normalizeRecordsOptInferSchema((Collections.singleton(sinkRecord))).get(0);
        // 4. Convert using the same entry point as the sink
        Map<String, Object> properties =
                converter.convertToWeaviateProperties(
                        updatedRecord.valueSchema(),
                        updatedRecord.value()
                );

        // 5. Assertions
        assertEquals(1L, properties.get("tinyint_col"));  // expect DOUBLE
        assertEquals(123L, properties.get("int_col"));
        assertEquals(1.23d, properties.get("float_col"));
        assertEquals(true, properties.get("bool_col"));
    }
}