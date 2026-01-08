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
package io.weaviate.connector.idstrategy;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class KafkaIdStrategy implements IDStrategy {
    public KafkaIdStrategy() {
    }

    @Override
    public String getDocumentId(SinkRecord record, Map<String, Object> valueProperties) {
        Object key = record.key();

        if (key == null) {
            throw new IllegalArgumentException(
                    "KafkaIdStrategy requires record key, but key is null. "
                            + "Topic=" + record.topic()
            );
        }

        // Convert key to a stable byte representation
        byte[] keyBytes = serializeKey(key);

        Object id = valueProperties.remove("id");
        if (id != null) {
            valueProperties.put(INTERNAL_ID_FIELD, id);
        }
        
        // Deterministic UUID (v5-style)
        return UUID.nameUUIDFromBytes(keyBytes).toString();
    }

    private byte[] serializeKey(Object key) {
        if (key instanceof String) {
            return ((String) key).getBytes(StandardCharsets.UTF_8);
        }

        if (key instanceof Number || key instanceof Boolean) {
            return String.valueOf(key).getBytes(StandardCharsets.UTF_8);
        }

        if (key instanceof Struct) {
            return serializeStruct((Struct) key);
        }

        // Fallback (still deterministic)
        return key.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serializeStruct(Struct struct) {
        StringBuilder sb = new StringBuilder();

        struct.schema().fields().forEach(field -> {
            Object value = struct.get(field);
            sb.append(field.name())
                    .append('=')
                    .append(value)
                    .append('|');
        });

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
