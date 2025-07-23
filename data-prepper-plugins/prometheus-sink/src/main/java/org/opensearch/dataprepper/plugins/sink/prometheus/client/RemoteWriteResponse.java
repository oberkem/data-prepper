/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.client;

import java.util.Objects;

/**
 * Response object for remote write operations containing status and error information
 */
public class RemoteWriteResponse {
    
    private final boolean success;
    private final int statusCode;
    private final String errorMessage;
    
    private RemoteWriteResponse(boolean success, int statusCode, String errorMessage) {
        this.success = success;
        this.statusCode = statusCode;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Create a successful response
     */
    public static RemoteWriteResponse success() {
        return new RemoteWriteResponse(true, 200, null);
    }
    
    /**
     * Create a failure response with status code and error message
     */
    public static RemoteWriteResponse failure(int statusCode, String errorMessage) {
        return new RemoteWriteResponse(false, statusCode, errorMessage);
    }
    
    /**
     * Check if the operation was successful
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * Get the HTTP status code
     */
    public int getStatusCode() {
        return statusCode;
    }
    
    /**
     * Get the error message (null for successful responses)
     */
    public String getErrorMessage() {
        return errorMessage;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteWriteResponse that = (RemoteWriteResponse) o;
        return success == that.success &&
               statusCode == that.statusCode &&
               Objects.equals(errorMessage, that.errorMessage);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(success, statusCode, errorMessage);
    }
    
    @Override
    public String toString() {
        return "RemoteWriteResponse{" +
               "success=" + success +
               ", statusCode=" + statusCode +
               ", errorMessage='" + errorMessage + '\'' +
               '}';
    }
}
