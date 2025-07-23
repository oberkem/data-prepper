/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.client;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;

/**
 * Builder for creating HTTP requests for Prometheus remote write operations
 */
public class RemoteWriteRequestBuilder {
    
    private final RemoteWriteClient.RemoteWriteClientConfig config;
    
    public RemoteWriteRequestBuilder(RemoteWriteClient.RemoteWriteClientConfig config) {
        this.config = config;
    }
    
    /**
     * Build an HTTP POST request for remote write with proper headers and compressed data
     * 
     * @param compressedData the snappy-compressed protobuf data
     * @return ClassicHttpRequest ready to be executed
     */
    public org.apache.hc.core5.http.ClassicHttpRequest buildRequest(byte[] compressedData) {
        HttpEntity entity = new ByteArrayEntity(
            compressedData,
            ContentType.create(config.getContentType()),
            config.getEncoding()
        );
        
        return ClassicRequestBuilder.post()
            .setUri(config.getUrl())
            .setEntity(entity)
            .addHeader("Content-Encoding", config.getEncoding())
            .addHeader("Content-Type", config.getContentType())
            .addHeader("X-Prometheus-Remote-Write-Version", config.getRemoteWriteVersion())
            .addHeader("User-Agent", "DataPrepper-PrometheusRemoteWrite/1.0")
            .build();
    }
}
