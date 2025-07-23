/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.client;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;

/**
 * Factory for creating RemoteWriteClient instances with appropriate configuration
 */
public class RemoteWriteClientFactory {
    
    /**
     * Create a RemoteWriteClient from PrometheusSinkConfiguration
     * 
     * @param config the Prometheus sink configuration
     * @param connectionManager the HTTP connection manager to use
     * @param pluginMetrics metrics for monitoring the client
     * @return configured RemoteWriteClient instance
     */
    public static RemoteWriteClient createClient(
            PrometheusSinkConfiguration config,
            HttpClientConnectionManager connectionManager,
            PluginMetrics pluginMetrics) {
        
        RemoteWriteClient.RemoteWriteClientConfig clientConfig = 
            RemoteWriteClient.RemoteWriteClientConfig.fromPrometheusSinkConfiguration(config, connectionManager);
        
        return new RemoteWriteClient(clientConfig, pluginMetrics);
    }
    
    /**
     * Create a RemoteWriteClient with custom configuration
     * 
     * @param config the remote write client configuration
     * @param pluginMetrics metrics for monitoring the client
     * @return configured RemoteWriteClient instance
     */
    public static RemoteWriteClient createClient(
            RemoteWriteClient.RemoteWriteClientConfig config,
            PluginMetrics pluginMetrics) {
        
        return new RemoteWriteClient(config, pluginMetrics);
    }
}
