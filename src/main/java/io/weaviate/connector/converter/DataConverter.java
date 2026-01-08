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

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DataConverter {
    private static final Logger log = LoggerFactory.getLogger(DataConverter.class);

    @SuppressWarnings("unchecked")
    public Map<String, Object> convertToWeaviateProperties(Schema schema, Object value) {
        Object object = convertToJava(schema, value);
        if (!(object instanceof Map)) {
            throw new DataException(String.format("Cannot convert " + schema.name() + " to Java object, %s is not a Map", value));
        }
        return (Map<String, Object>) object;
    }

    private Object convertToJava(Schema schema, Object value) {
        if (value == null) {
            if (schema == null)
                return null;
            if (schema.defaultValue() != null)
                return convertToJava(schema, schema.defaultValue());
            if (schema.isOptional())
                return null;
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null) {
                    // Handle special cases where schema type detection fails
                    if (value instanceof BigDecimal) {
                        return ((BigDecimal) value).doubleValue();
                    }
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }
            if (schema!=null) log.info("Schema name: {}, type: {}", schema.name(), schema.type());
            switch (schemaType) {
                case INT8:
                    if (value instanceof Byte) {
                        return ((Byte) value).longValue();
                    }
                    ByteBuffer bb = ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, (byte) value});
                    return bb.getLong();
                case INT16:
                    return ((Number) value).longValue();
                case INT32:
                    return ((Number) value).longValue();
                case INT64:
                    if (schema != null && schema.name() != null) {

                        String logicalName = schema.name();

                        // === Kafka Connect logical timestamps ===
                        if ("org.apache.kafka.connect.data.Timestamp".equals(logicalName) ||
                                "org.apache.kafka.connect.data.Date".equals(logicalName) ||
                                "org.apache.kafka.connect.data.Time".equals(logicalName)) {

                            if (value instanceof java.util.Date) {
                                return ((java.util.Date) value).toInstant().toString();
                            }
                            if (value instanceof Number) {
                                return Instant.ofEpochMilli(((Number) value).longValue()).toString();
                            }
                        }

                        // === Debezium timestamps ===
                        if ("io.debezium.time.Timestamp".equals(logicalName)) {
                            return Instant.ofEpochMilli(((Number) value).longValue()).toString();
                        }

                        if ("io.debezium.time.MicroTimestamp".equals(logicalName)) {
                            return Instant.ofEpochMilli(((Number) value).longValue() / 1_000L).toString();
                        }

                        if ("io.debezium.time.NanoTimestamp".equals(logicalName)) {
                            return Instant.ofEpochMilli(((Number) value).longValue() / 1_000_000L).toString();
                        }
                    }

                    // Non-date INT64
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }

                    throw new DataException("Invalid INT64 value type: " + value.getClass());

                case FLOAT32:
                    return ((Number) value).doubleValue();
                case FLOAT64:
                    return ((Number) value).doubleValue();
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    }
                    // Handle string representations of boolean
                    if (value instanceof String) {
                        String strVal = ((String) value).toLowerCase();
                        return strVal.equals("true") || strVal.equals("1");
                    }
                    return Boolean.valueOf(value.toString());
                case STRING:
                    if (value instanceof String) {
                        return value;
                    }
                    return value.toString();
                case BYTES:
                    if (value instanceof BigDecimal)
                        return ((BigDecimal) value).doubleValue();
                    else if (value instanceof byte[])
                        return value;
                    else if (value instanceof ByteBuffer)
                        return ((ByteBuffer) value).array();
                    else
                        // For any other type, convert to string then to bytes
                        return value.toString().getBytes();
                case ARRAY: {
                    Collection<?> collection = (Collection<?>) value;
                    ArrayList<Object> list = new ArrayList<>(collection.size());
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object fieldValue = convertToJava(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    HashMap<String, Object> object = new HashMap<>(map.size());
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object mapKey = convertToJava(keySchema, entry.getKey());
                        Object mapValue = convertToJava(valueSchema, entry.getValue());

                        object.put(String.valueOf(mapKey), mapValue);
                    }
                    return object;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");

                    HashMap<String, Object> object = new HashMap<>();
                    for (Field field : schema.fields()) {
                        object.put(field.name(), convertToJava(field.schema(), struct.get(field)));
                    }
                    return object;
                }
            }

            // Handle logical types (e.g., Decimal)
            if (schema != null && schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                    if (value instanceof BigDecimal) {
                        return ((BigDecimal) value).doubleValue();
                    }
                    if (value instanceof byte[]) {
                        return new BigDecimal(new java.math.BigInteger((byte[]) value)).doubleValue();
                    }
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                }
            }

            throw new DataException("Couldn't convert " + value + " to Java object.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass(), e);
        } catch (Exception e) {
            String schemaTypeStr = (schema != null && schema.type() != null) ? schema.type().toString() : "unknown schema";
            log.warn("Error converting value of type {} to schema type {}: {}", value.getClass().getName(), schemaTypeStr, e.getMessage());
            throw new DataException("Conversion error for " + schemaTypeStr + " with value " + value.getClass().getName(), e);
        }
    }

}
