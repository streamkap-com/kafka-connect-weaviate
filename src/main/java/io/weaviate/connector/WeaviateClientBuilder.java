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

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.auth.exception.AuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builds and configures WeaviateClient instances based on connector configuration.
 */
public class WeaviateClientBuilder {
    private static final Logger log = LoggerFactory.getLogger(WeaviateClientBuilder.class);

    /**
     * Builds a WeaviateClient based on the provided configuration.
     * Supports three authentication mechanisms: NONE, API_KEY, and OIDC_CLIENT_CREDENTIALS.
     *
     * @param config the WeaviateSinkConfig containing connection and auth details
     * @return configured WeaviateClient
     * @throws RuntimeException if client creation fails
     */
    public static WeaviateClient buildClient(WeaviateSinkConfig config) {
        Config weaviateConfig = buildWeaviateConfig(config);

        try {
            switch (config.getAuthMechanism()) {
                case NONE:
                    return new WeaviateClient(weaviateConfig);
                case API_KEY:
                    return WeaviateAuthClient.apiKey(weaviateConfig, config.getApiKey());
                case OIDC_CLIENT_CREDENTIALS:
                    return WeaviateAuthClient.clientCredentials(
                            weaviateConfig,
                            config.getOidcClientSecret(),
                            config.getOidcScopes()
                    );
                default:
                    throw new RuntimeException("Unknown authentication mechanism: " + config.getAuthMechanism());
            }
        } catch (AuthException e) {
            log.error("Failed to create WeaviateClient with {} authentication", config.getAuthMechanism(), e);
            throw new RuntimeException("WeaviateClient creation failed", e);
        }
    }

    /**
     * Builds the Weaviate Config from connector configuration.
     * Extracts host, port, scheme, and configures gRPC settings if provided.
     *
     * @param config the WeaviateSinkConfig
     * @return configured Weaviate Config
     * @throws RuntimeException if URL parsing fails
     */
    private static Config buildWeaviateConfig(WeaviateSinkConfig config) {
        String connectionUrl = config.getConnectionUrl();
        String[] urlParts = connectionUrl.split("://");

        if (urlParts.length != 2) {
            throw new RuntimeException("Invalid Weaviate connection URL format: " + connectionUrl);
        }

        String scheme = urlParts[0];
        String hostAndPort = urlParts[1];
        Map<String, String> headers = config.getHeaders();

        Config weaviateConfig = new Config(scheme, hostAndPort, headers);

        // Configure gRPC settings if provided
        String grpcUrl = config.getGrpcUrl();
        if (grpcUrl != null && !grpcUrl.isEmpty()) {
            weaviateConfig.setGRPCHost(grpcUrl);
            weaviateConfig.setGRPCSecured(config.getGrpcSecured());
            log.info("gRPC configured with host: {} (secured: {})", grpcUrl, config.getGrpcSecured());
        }

        return weaviateConfig;
    }
}

