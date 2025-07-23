/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.client;

import com.arpnetworking.metrics.prometheus.Remote;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.prometheus.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Dedicated RemoteWriteClient with configurable timeouts, exponential back-off, 
 * HTTP status inspection and protobuf snappy compression per protocol.
 */
public class RemoteWriteClient {
    
    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteClient.class);
    
    // Default configuration values
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(60);  
    private static final Duration DEFAULT_SOCKET_TIMEOUT = Duration.ofSeconds(90);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofMillis(500);
    private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(30);
    private static final double DEFAULT_JITTER_FACTOR = 0.1;
    
    // HTTP status codes that should trigger retries
    private static final int[] RETRYABLE_STATUS_CODES = {
        HttpStatus.SC_REQUEST_TIMEOUT,      // 408
        HttpStatus.SC_TOO_MANY_REQUESTS,    // 429  
        HttpStatus.SC_INTERNAL_SERVER_ERROR, // 500
        HttpStatus.SC_BAD_GATEWAY,          // 502
        HttpStatus.SC_SERVICE_UNAVAILABLE,  // 503
        HttpStatus.SC_GATEWAY_TIMEOUT       // 504
    };
    
    private final RemoteWriteClientConfig config;
    private final CloseableHttpClient httpClient;
    private final RemoteWriteRequestBuilder requestBuilder;
    
    // Metrics
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter retryCounter;
    private final Timer requestTimer;
    private final Timer compressionTimer;
    
    public RemoteWriteClient(RemoteWriteClientConfig config, PluginMetrics pluginMetrics) {
        this.config = config;
        this.httpClient = createHttpClient();
        this.requestBuilder = new RemoteWriteRequestBuilder(config);
        
        // Initialize metrics
        this.successCounter = pluginMetrics.counter("remote_write_requests_success");
        this.failureCounter = pluginMetrics.counter("remote_write_requests_failed");
        this.retryCounter = pluginMetrics.counter("remote_write_requests_retried");
        this.requestTimer = pluginMetrics.timer("remote_write_request_duration");
        this.compressionTimer = pluginMetrics.timer("remote_write_compression_duration");
    }
    
    /**
     * Send a WriteRequest to the remote Prometheus endpoint with retry logic and error handling
     * 
     * @param writeRequest the protobuf WriteRequest to send
     * @return RemoteWriteResponse containing success status and error details if any
     */
    public RemoteWriteResponse send(Remote.WriteRequest writeRequest) {
        Timer.Sample requestSample = Timer.start();
        
        try {
            byte[] serializedData = writeRequest.toByteArray();
            byte[] compressedData = compressData(serializedData);
            
            RemoteWriteResponse response = sendWithRetry(compressedData);
            
            if (response.isSuccess()) {
                successCounter.increment();
                LOG.debug("Successfully sent remote write request to {}", config.getUrl());
            } else {
                failureCounter.increment();
                LOG.error("Failed to send remote write request to {}: {}", 
                    config.getUrl(), response.getErrorMessage());
            }
            
            return response;
            
        } catch (Exception e) {
            failureCounter.increment();
            LOG.error("Exception occurred while sending remote write request to {}", config.getUrl(), e);
            return RemoteWriteResponse.failure(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                "Client exception: " + e.getMessage());
        } finally {
            requestSample.stop(requestTimer);
        }
    }
    
    /**
     * Send compressed data with exponential backoff retry logic
     */
    private RemoteWriteResponse sendWithRetry(byte[] compressedData) {
        int attempt = 0;
        Duration backoffDelay = config.getInitialBackoff();
        
        while (attempt <= config.getMaxRetries()) {
            try {
                RemoteWriteResponse response = performRequest(compressedData);
                
                if (response.isSuccess() || !isRetryableError(response.getStatusCode())) {
                    return response;
                }
                
                if (attempt < config.getMaxRetries()) {
                    retryCounter.increment();
                    LOG.warn("Request failed with status {} (attempt {}/{}), retrying in {}ms", 
                        response.getStatusCode(), attempt + 1, config.getMaxRetries() + 1, 
                        backoffDelay.toMillis());
                    
                    sleep(backoffDelay);
                    backoffDelay = calculateNextBackoff(backoffDelay);
                }
                
                attempt++;
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return RemoteWriteResponse.failure(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                    "Request interrupted");
            } catch (Exception e) {
                if (attempt >= config.getMaxRetries()) {
                    return RemoteWriteResponse.failure(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                        "Max retries exceeded: " + e.getMessage());
                }
                
                retryCounter.increment();
                LOG.warn("Request failed with exception (attempt {}/{}), retrying in {}ms", 
                    attempt + 1, config.getMaxRetries() + 1, backoffDelay.toMillis(), e);
                
                try {
                    sleep(backoffDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return RemoteWriteResponse.failure(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                        "Request interrupted");
                }
                
                backoffDelay = calculateNextBackoff(backoffDelay);
                attempt++;
            }
        }
        
        return RemoteWriteResponse.failure(HttpStatus.SC_INTERNAL_SERVER_ERROR, 
            "Max retries exceeded");
    }
    
    /**
     * Perform the actual HTTP request
     */
    private RemoteWriteResponse performRequest(byte[] compressedData) throws Exception {
        var request = requestBuilder.buildRequest(compressedData);
        var context = HttpClientContext.create();
        
        var response = httpClient.execute(request, context);
        
        try {
            int statusCode = response.getCode();
            String statusReason = response.getReasonPhrase();
            
            if (isSuccessStatus(statusCode)) {
                return RemoteWriteResponse.success();
            } else {
                String errorMessage = String.format("HTTP %d: %s", statusCode, statusReason);
                return RemoteWriteResponse.failure(statusCode, errorMessage);
            }
        } finally {
            response.close();
        }
    }
    
    /**
     * Compress data using Snappy compression as per Prometheus remote write protocol
     */
    private byte[] compressData(byte[] data) throws IOException {
        Timer.Sample compressionSample = Timer.start();
        try {
            return Snappy.compress(data);
        } finally {
            compressionSample.stop(compressionTimer);
        }
    }
    
    /**
     * Create HTTP client with configured timeouts and connection management
     */
    private CloseableHttpClient createHttpClient() {
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(Timeout.of(config.getConnectTimeout()))
            .setConnectionRequestTimeout(Timeout.of(config.getRequestTimeout()))
            .setResponseTimeout(Timeout.of(config.getSocketTimeout()))
            .build();
            
        return HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setConnectionManager(config.getConnectionManager())
            .build();
    }
    
    /**
     * Calculate next backoff delay with exponential backoff and jitter
     */
    private Duration calculateNextBackoff(Duration currentBackoff) {
        // Apply exponential backoff
        Duration nextBackoff = Duration.ofMillis(
            (long) (currentBackoff.toMillis() * config.getBackoffMultiplier())
        );
        
        // Cap at max backoff
        if (nextBackoff.compareTo(config.getMaxBackoff()) > 0) {
            nextBackoff = config.getMaxBackoff();
        }
        
        // Apply jitter to avoid thundering herd
        if (config.getJitterFactor() > 0) {
            long jitterRange = (long) (nextBackoff.toMillis() * config.getJitterFactor());
            long jitter = ThreadLocalRandom.current().nextLong(-jitterRange, jitterRange + 1);
            nextBackoff = Duration.ofMillis(Math.max(0, nextBackoff.toMillis() + jitter));
        }
        
        return nextBackoff;
    }
    
    /**
     * Check if HTTP status code indicates success
     */
    private boolean isSuccessStatus(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
    
    /**
     * Check if error is retryable based on status code
     */
    private boolean isRetryableError(int statusCode) {
        for (int retryableCode : RETRYABLE_STATUS_CODES) {
            if (statusCode == retryableCode) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Sleep with interruption handling
     */
    private void sleep(Duration duration) throws InterruptedException {
        Thread.sleep(duration.toMillis());
    }
    
    /**
     * Close the HTTP client and clean up resources
     */
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
    
    /**
     * Configuration class for RemoteWriteClient
     */
    public static class RemoteWriteClientConfig {
        private final String url;
        private final Duration connectTimeout;
        private final Duration requestTimeout;
        private final Duration socketTimeout;
        private final int maxRetries;
        private final Duration initialBackoff;
        private final double backoffMultiplier;
        private final Duration maxBackoff;
        private final double jitterFactor;
        private final org.apache.hc.client5.http.io.HttpClientConnectionManager connectionManager;
        private final String contentType;
        private final String encoding;
        private final String remoteWriteVersion;
        
        private RemoteWriteClientConfig(Builder builder) {
            this.url = builder.url;
            this.connectTimeout = builder.connectTimeout;
            this.requestTimeout = builder.requestTimeout;
            this.socketTimeout = builder.socketTimeout;
            this.maxRetries = builder.maxRetries;
            this.initialBackoff = builder.initialBackoff;
            this.backoffMultiplier = builder.backoffMultiplier;
            this.maxBackoff = builder.maxBackoff;
            this.jitterFactor = builder.jitterFactor;
            this.connectionManager = builder.connectionManager;
            this.contentType = builder.contentType;
            this.encoding = builder.encoding;
            this.remoteWriteVersion = builder.remoteWriteVersion;
        }
        
        public static Builder builder(String url) {
            return new Builder(url);
        }
        
        public static RemoteWriteClientConfig fromPrometheusSinkConfiguration(
                PrometheusSinkConfiguration config, 
                org.apache.hc.client5.http.io.HttpClientConnectionManager connectionManager) {
            return builder(config.getUrl())
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .requestTimeout(config.getRequestTimout() != null ? config.getRequestTimout() : DEFAULT_REQUEST_TIMEOUT)
                .socketTimeout(DEFAULT_SOCKET_TIMEOUT)
                .maxRetries(config.getMaxUploadRetries())
                .initialBackoff(DEFAULT_INITIAL_BACKOFF)
                .backoffMultiplier(DEFAULT_BACKOFF_MULTIPLIER)
                .maxBackoff(DEFAULT_MAX_BACKOFF)
                .jitterFactor(DEFAULT_JITTER_FACTOR)
                .connectionManager(connectionManager)
                .contentType(config.getContentType())
                .encoding(config.getEncoding())
                .remoteWriteVersion(config.getRemoteWriteVersion())
                .build();
        }
        
        // Getters
        public String getUrl() { return url; }
        public Duration getConnectTimeout() { return connectTimeout; }
        public Duration getRequestTimeout() { return requestTimeout; }
        public Duration getSocketTimeout() { return socketTimeout; }
        public int getMaxRetries() { return maxRetries; }
        public Duration getInitialBackoff() { return initialBackoff; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public Duration getMaxBackoff() { return maxBackoff; }
        public double getJitterFactor() { return jitterFactor; }
        public org.apache.hc.client5.http.io.HttpClientConnectionManager getConnectionManager() { return connectionManager; }
        public String getContentType() { return contentType; }
        public String getEncoding() { return encoding; }
        public String getRemoteWriteVersion() { return remoteWriteVersion; }
        
        public static class Builder {
            private final String url;
            private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
            private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
            private Duration socketTimeout = DEFAULT_SOCKET_TIMEOUT;
            private int maxRetries = DEFAULT_MAX_RETRIES;
            private Duration initialBackoff = DEFAULT_INITIAL_BACKOFF;
            private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
            private Duration maxBackoff = DEFAULT_MAX_BACKOFF;
            private double jitterFactor = DEFAULT_JITTER_FACTOR;
            private org.apache.hc.client5.http.io.HttpClientConnectionManager connectionManager;
            private String contentType = "application/x-protobuf";
            private String encoding = "snappy";
            private String remoteWriteVersion = "0.1.0";
            
            private Builder(String url) {
                this.url = url;
            }
            
            public Builder connectTimeout(Duration connectTimeout) {
                this.connectTimeout = connectTimeout;
                return this;
            }
            
            public Builder requestTimeout(Duration requestTimeout) {
                this.requestTimeout = requestTimeout;
                return this;
            }
            
            public Builder socketTimeout(Duration socketTimeout) {
                this.socketTimeout = socketTimeout;
                return this;
            }
            
            public Builder maxRetries(int maxRetries) {
                this.maxRetries = maxRetries;
                return this;
            }
            
            public Builder initialBackoff(Duration initialBackoff) {
                this.initialBackoff = initialBackoff;
                return this;
            }
            
            public Builder backoffMultiplier(double backoffMultiplier) {
                this.backoffMultiplier = backoffMultiplier;
                return this;
            }
            
            public Builder maxBackoff(Duration maxBackoff) {
                this.maxBackoff = maxBackoff;
                return this;
            }
            
            public Builder jitterFactor(double jitterFactor) {
                this.jitterFactor = jitterFactor;
                return this;
            }
            
            public Builder connectionManager(org.apache.hc.client5.http.io.HttpClientConnectionManager connectionManager) {
                this.connectionManager = connectionManager;
                return this;
            }
            
            public Builder contentType(String contentType) {
                this.contentType = contentType;
                return this;
            }
            
            public Builder encoding(String encoding) {
                this.encoding = encoding;
                return this;
            }
            
            public Builder remoteWriteVersion(String remoteWriteVersion) {
                this.remoteWriteVersion = remoteWriteVersion;
                return this;
            }
            
            public RemoteWriteClientConfig build() {
                return new RemoteWriteClientConfig(this);
            }
        }
    }
}
