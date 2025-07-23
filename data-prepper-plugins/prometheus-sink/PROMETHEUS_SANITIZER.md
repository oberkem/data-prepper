# Prometheus Sanitizer Utility

## Overview

The `PrometheusSanitizer` utility class provides reusable sanitization functions for Prometheus metric names, label names, and label values. It enforces Prometheus naming conventions and prevents issues with invalid characters and oversized values.

## Features

### Metric Name Sanitization
- Sanitizes metric names according to Prometheus naming rules: `[a-zA-Z_:][a-zA-Z0-9_:]*`
- Allows colons (`:`) for namespace separation
- Replaces invalid characters with underscores (`_`)
- Truncates overly long names (max 200 characters)

### Label Name Sanitization  
- Sanitizes label names according to Prometheus rules: `[a-zA-Z_][a-zA-Z0-9_]*`
- Does not allow colons in label names (unlike metric names)
- Handles reserved label names (starting with `__`) by prefixing with "reserved_"
- Replaces invalid characters with underscores
- Truncates overly long names (max 200 characters)

### Label Value Sanitization
- Truncates oversized label values (max 1000 characters)
- Preserves Unicode characters (Prometheus allows any Unicode in label values)
- Adds truncation suffix when values are shortened

## API Reference

### Core Sanitization Methods

```java
// Sanitize a metric name
String sanitized = PrometheusSanitizer.sanitizeMetricName("my-metric.name");
// Result: "my_metric_name"

// Sanitize a label name  
String sanitized = PrometheusSanitizer.sanitizeLabelName("__reserved_label");
// Result: "reserved_reserved_label"

// Sanitize a label value
String sanitized = PrometheusSanitizer.sanitizeLabelValue("very long value...");
// Result: truncated if over 1000 chars
```

### Validation Methods

```java
// Validate metric name
boolean isValid = PrometheusSanitizer.isValidMetricName("valid_metric_name");

// Validate label name
boolean isValid = PrometheusSanitizer.isValidLabelName("valid_label");

// Validate label value length
boolean isValid = PrometheusSanitizer.isValidLabelValue("short_value");
```

### Utility Methods

```java
// Build compound metric names safely
String metricName = PrometheusSanitizer.buildMetricName("namespace", "subsystem", "name");
// Result: "namespace_subsystem_name"

// Escape special characters for logging
String escaped = PrometheusSanitizer.escapeSpecialCharacters("text\nwith\tspecial\rchars");
// Result: "text\\nwith\\tspecial\\rchars"
```

## Integration

The sanitizer has been integrated into the existing `PrometheusSinkService`:

- **Metric names**: All metric names are sanitized before being sent to Prometheus
- **Label names**: All attribute keys are sanitized as label names  
- **Label values**: All attribute values are sanitized as label values

## Examples

### Before Sanitization
```java
Map<String, Object> attributes = Map.of(
    "123invalid-label", "some value",
    "__reserved", "reserved value",
    "valid_label", "normal value"
);
String metricName = "123invalid.metric-name";
```

### After Sanitization
```java
// Metric name: "123invalid.metric-name" → "_invalid_metric_name"
// Labels:
//   "123invalid-label" → "_invalid_label" 
//   "__reserved" → "reserved_reserved"
//   "valid_label" → "valid_label" (unchanged)
```

## Constants

- `MAX_NAME_LENGTH`: 200 characters (metric and label names)
- `MAX_LABEL_VALUE_LENGTH`: 1000 characters (label values)
- `REPLACEMENT_CHAR`: "_" (used to replace invalid characters)

## Thread Safety

The `PrometheusSanitizer` class is thread-safe as it only contains static methods and immutable fields.

## Testing

The utility includes comprehensive unit tests covering:
- Valid and invalid input scenarios
- Edge cases (empty strings, null values)
- Length truncation behavior
- Unicode character handling
- Reserved name handling

See `PrometheusSanitizerTest.java` for detailed test cases.
