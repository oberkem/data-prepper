/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.util;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Utility class for sanitizing Prometheus metric names and labels according to Prometheus naming conventions.
 * 
 * Prometheus naming rules:
 * - Metric names must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
 * - Label names must match the regex [a-zA-Z_][a-zA-Z0-9_]*
 * - Label names starting with __ are reserved for internal use
 * - Label values can contain any Unicode characters
 * - Both metric names and label names should be limited in length (recommended max 200 chars)
 */
public class PrometheusSanitizer {
    
    // Maximum recommended length for metric names and label names
    private static final int MAX_NAME_LENGTH = 200;
    
    // Maximum recommended length for label values
    private static final int MAX_LABEL_VALUE_LENGTH = 1000;
    
    // Pattern for valid metric name characters (allows colons for namespace separation)
    private static final Pattern METRIC_NAME_PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_:]");
    private static final Pattern METRIC_NAME_BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_:]");
    
    // Pattern for valid label name characters (no colons allowed)
    private static final Pattern LABEL_NAME_PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_]");
    private static final Pattern LABEL_NAME_BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_]");
    
    // Pattern to detect reserved label names (starting with __)
    private static final Pattern RESERVED_LABEL_PATTERN = Pattern.compile("^__.*");
    
    // Replacement character for invalid characters
    private static final String REPLACEMENT_CHAR = "_";
    
    /**
     * Sanitizes a Prometheus metric name according to naming conventions.
     * 
     * @param metricName the metric name to sanitize
     * @return sanitized metric name, or null if input is null
     */
    public static String sanitizeMetricName(final String metricName) {
        if (metricName == null) {
            return null;
        }
        
        if (metricName.isEmpty()) {
            return REPLACEMENT_CHAR;
        }
        
        // Replace invalid prefix characters
        String sanitized = METRIC_NAME_PREFIX_PATTERN.matcher(metricName).replaceFirst(REPLACEMENT_CHAR);
        
        // Replace invalid body characters
        sanitized = METRIC_NAME_BODY_PATTERN.matcher(sanitized).replaceAll(REPLACEMENT_CHAR);
        
        // Truncate if too long
        sanitized = truncateToMaxLength(sanitized, MAX_NAME_LENGTH);
        
        return sanitized;
    }
    
    /**
     * Sanitizes a Prometheus label name according to naming conventions.
     * 
     * @param labelName the label name to sanitize
     * @return sanitized label name, or null if input is null
     */
    public static String sanitizeLabelName(final String labelName) {
        if (labelName == null) {
            return null;
        }
        
        if (labelName.isEmpty()) {
            return REPLACEMENT_CHAR;
        }
        
        // Handle reserved names (starting with __)
        String sanitized = labelName;
        if (RESERVED_LABEL_PATTERN.matcher(labelName).matches()) {
            sanitized = "reserved" + REPLACEMENT_CHAR + labelName.substring(2);
        }
        
        // Replace invalid prefix characters
        sanitized = LABEL_NAME_PREFIX_PATTERN.matcher(sanitized).replaceFirst(REPLACEMENT_CHAR);
        
        // Replace invalid body characters
        sanitized = LABEL_NAME_BODY_PATTERN.matcher(sanitized).replaceAll(REPLACEMENT_CHAR);
        
        // Truncate if too long
        sanitized = truncateToMaxLength(sanitized, MAX_NAME_LENGTH);
        
        return sanitized;
    }
    
    /**
     * Sanitizes a Prometheus label value by truncating oversized values.
     * Label values can contain any Unicode characters, but should be limited in length.
     * 
     * @param labelValue the label value to sanitize
     * @return sanitized label value, or null if input is null
     */
    public static String sanitizeLabelValue(final String labelValue) {
        if (labelValue == null) {
            return null;
        }
        
        // Label values can contain any Unicode characters, just truncate if too long
        return truncateToMaxLength(labelValue, MAX_LABEL_VALUE_LENGTH);
    }
    
    /**
     * Validates if a metric name conforms to Prometheus naming conventions.
     * 
     * @param metricName the metric name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidMetricName(final String metricName) {
        if (metricName == null || metricName.isEmpty()) {
            return false;
        }
        
        if (metricName.length() > MAX_NAME_LENGTH) {
            return false;
        }
        
        // Check first character
        if (METRIC_NAME_PREFIX_PATTERN.matcher(metricName.substring(0, 1)).matches()) {
            return false;
        }
        
        // Check all characters
        return !METRIC_NAME_BODY_PATTERN.matcher(metricName).find();
    }
    
    /**
     * Validates if a label name conforms to Prometheus naming conventions.
     * 
     * @param labelName the label name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidLabelName(final String labelName) {
        if (labelName == null || labelName.isEmpty()) {
            return false;
        }
        
        if (labelName.length() > MAX_NAME_LENGTH) {
            return false;
        }
        
        // Reserved names are not allowed
        if (RESERVED_LABEL_PATTERN.matcher(labelName).matches()) {
            return false;
        }
        
        // Check first character
        if (LABEL_NAME_PREFIX_PATTERN.matcher(labelName.substring(0, 1)).matches()) {
            return false;
        }
        
        // Check all characters
        return !LABEL_NAME_BODY_PATTERN.matcher(labelName).find();
    }
    
    /**
     * Validates if a label value is within acceptable limits.
     * 
     * @param labelValue the label value to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidLabelValue(final String labelValue) {
        if (labelValue == null) {
            return true; // null values are acceptable
        }
        
        return labelValue.length() <= MAX_LABEL_VALUE_LENGTH;
    }
    
    /**
     * Escapes special characters in a string that might cause issues in Prometheus contexts.
     * This is primarily for logging and debugging purposes.
     * 
     * @param input the string to escape
     * @return escaped string, or null if input is null
     */
    public static String escapeSpecialCharacters(final String input) {
        if (input == null) {
            return null;
        }
        
        return input
            .replace("\\", "\\\\")  // Escape backslashes
            .replace("\"", "\\\"")  // Escape quotes
            .replace("\n", "\\n")   // Escape newlines
            .replace("\r", "\\r")   // Escape carriage returns
            .replace("\t", "\\t");  // Escape tabs
    }
    
    /**
     * Creates a safe metric name by combining namespace, subsystem, and name components.
     * This follows Prometheus best practices for metric naming.
     * 
     * @param namespace the metric namespace (can be null)
     * @param subsystem the metric subsystem (can be null)
     * @param name the metric name (required)
     * @return sanitized full metric name
     * @throws IllegalArgumentException if name is null or empty
     */
    public static String buildMetricName(final String namespace, final String subsystem, final String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Metric name cannot be null or empty");
        }
        
        StringBuilder builder = new StringBuilder();
        
        if (namespace != null && !namespace.isEmpty()) {
            builder.append(sanitizeMetricName(namespace)).append("_");
        }
        
        if (subsystem != null && !subsystem.isEmpty()) {
            builder.append(sanitizeMetricName(subsystem)).append("_");
        }
        
        builder.append(sanitizeMetricName(name));
        
        return builder.toString();
    }
    
    /**
     * Truncates a string to the specified maximum length.
     * If truncation occurs, adds a suffix to indicate truncation.
     * 
     * @param input the string to truncate
     * @param maxLength the maximum allowed length
     * @return truncated string
     */
    private static String truncateToMaxLength(final String input, final int maxLength) {
        if (input == null || input.length() <= maxLength) {
            return input;
        }
        
        // Reserve space for truncation suffix
        final String suffix = "_trunc";
        final int availableLength = maxLength - suffix.length();
        
        if (availableLength <= 0) {
            // If maxLength is too small, just return the suffix
            return suffix.substring(0, Math.min(suffix.length(), maxLength));
        }
        
        return input.substring(0, availableLength) + suffix;
    }
    
    /**
     * Gets the maximum allowed length for metric names and label names.
     * 
     * @return maximum name length
     */
    public static int getMaxNameLength() {
        return MAX_NAME_LENGTH;
    }
    
    /**
     * Gets the maximum allowed length for label values.
     * 
     * @return maximum label value length
     */
    public static int getMaxLabelValueLength() {
        return MAX_LABEL_VALUE_LENGTH;
    }
}
