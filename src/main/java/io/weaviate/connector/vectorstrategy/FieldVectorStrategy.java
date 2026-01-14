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
package io.weaviate.connector.vectorstrategy;

import io.weaviate.connector.WeaviateSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldVectorStrategy implements VectorStrategy {
    private String fieldName;

    public FieldVectorStrategy() {
    }

    @Override
    public void configure(WeaviateSinkConfig config) {
        fieldName = config.getVectorFieldName();
    }

    @Override
    public Float[] getDocumentVector(SinkRecord record, Map<String, Object> valueProperties) {
        if (valueProperties.get(fieldName) == null) {
            return null;
        }

        Object object = valueProperties.get(fieldName);

        // Case 1: Already Float[]
        if (object instanceof Float[]) {
            valueProperties.remove(fieldName);
            return (Float[]) object;
        }

        // Case 2: Iterable of numbers
        if (object instanceof Iterable) {
            Iterable<?> iterable = (Iterable<?>) object;
            List<Float> floatList = new ArrayList<>();
            for (Object o : iterable) {
                if (o instanceof Float) {
                    floatList.add((Float) o);
                } else if (o instanceof Double) {
                    floatList.add(((Double) o).floatValue());
                } else if (o instanceof Number) {
                    floatList.add(((Number) o).floatValue());
                } else {
                    throw new UnsupportedOperationException("Can't convert element " + o + " to Float");
                }
            }
            valueProperties.remove(fieldName);
            return floatList.toArray(new Float[0]);
        }

        // Case 3: JSON string "[0.1, 0.2, 0.3]"
        if (object instanceof String) {
            String str = ((String) object).trim();
            if (str.startsWith("[") && str.endsWith("]")) {
                str = str.substring(1, str.length() - 1); // remove brackets
                String[] parts = str.split(",");
                List<Float> floatList = new ArrayList<>();
                for (String part : parts) {
                    try {
                        floatList.add(Float.parseFloat(part.trim()));
                    } catch (NumberFormatException e) {
                        throw new UnsupportedOperationException("Can't convert '" + part + "' to Float", e);
                    }
                }
                valueProperties.remove(fieldName);
                return floatList.toArray(new Float[0]);
            }
        }

        throw new UnsupportedOperationException("Can't convert " + object + " to Float[]");
    }

}
