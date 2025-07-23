package org.opensearch.dataprepper.plugins.sink.prometheus.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusSanitizerTest {

    @Test
    void testSanitizeMetricName() {
        assertEquals("valid_metric_name", PrometheusSanitizer.sanitizeMetricName("valid_metric_name"));
        assertEquals("metric_name", PrometheusSanitizer.sanitizeMetricName(".metric_name"));
        assertEquals("metric_name", PrometheusSanitizer.sanitizeMetricName("123metric_name."));
        assertEquals("very_very_long_metric_na_trunc", PrometheusSanitizer.sanitizeMetricName("very_very_long_metric_name_that_exceeds_the_length_limit_1234567890"));
        assertEquals("_", PrometheusSanitizer.sanitizeMetricName(""));
        assertNull(PrometheusSanitizer.sanitizeMetricName(null));
    }

    @Test
    void testSanitizeLabelName() {
        assertEquals("valid_label", PrometheusSanitizer.sanitizeLabelName("valid_label"));
        assertEquals("label", PrometheusSanitizer.sanitizeLabelName("123label."));
        assertEquals("reserved_label", PrometheusSanitizer.sanitizeLabelName("__reserved_label"));
        assertEquals("very_very_long_label_trunc", PrometheusSanitizer.sanitizeLabelName("very_very_long_label_that_exceeds_the_length_limit_1234567890"));
        assertEquals("_", PrometheusSanitizer.sanitizeLabelName(""));
        assertNull(PrometheusSanitizer.sanitizeLabelName(null));
    }

    @Test
    void testSanitizeLabelValue() {
        assertEquals("valid_value", PrometheusSanitizer.sanitizeLabelValue("valid_value"));
        assertEquals("very_very_long_value_that_exceeds_the_length_trunc", PrometheusSanitizer.sanitizeLabelValue("very_very_long_value_that_exceeds_the_length_limit_and_needs_to_be_truncated"));
        assertNull(PrometheusSanitizer.sanitizeLabelValue(null));
    }

    @Test
    void testIsValidMetricName() {
        assertTrue(PrometheusSanitizer.isValidMetricName("valid_metric_name"));
        assertFalse(PrometheusSanitizer.isValidMetricName("123metric_name"));
        assertFalse(PrometheusSanitizer.isValidMetricName("") );
        assertFalse(PrometheusSanitizer.isValidMetricName(null));
    }

    @Test
    void testIsValidLabelName() {
        assertTrue(PrometheusSanitizer.isValidLabelName("valid_label"));
        assertFalse(PrometheusSanitizer.isValidLabelName("__reserved_label"));
        assertFalse(PrometheusSanitizer.isValidLabelName("123label"));
        assertFalse(PrometheusSanitizer.isValidLabelName("") );
        assertFalse(PrometheusSanitizer.isValidLabelName(null));
    }

    @Test
    void testIsValidLabelValue() {
        assertTrue(PrometheusSanitizer.isValidLabelValue("valid_value"));
        assertTrue(PrometheusSanitizer.isValidLabelValue(null));
        assertFalse(PrometheusSanitizer.isValidLabelValue(new String(new char[2000]).replace('\0', 'a')));
    }

    @Test
    void testEscapeSpecialCharacters() {
        assertEquals("Some\\nNew\\tLine\\rCarriage\\\\Backslash\\\"Quote", PrometheusSanitizer.escapeSpecialCharacters("Some\nNew\tLine\rCarriage\\Backslash\"Quote"));
        assertNull(PrometheusSanitizer.escapeSpecialCharacters(null));
    }

    @Test
    void testBuildMetricName() {
        assertEquals("namespace_subsystem_metric", PrometheusSanitizer.buildMetricName("namespace", "subsystem", "metric"));
        assertEquals("namespace_metric", PrometheusSanitizer.buildMetricName("namespace", null, "metric"));
        assertEquals("subsystem_metric", PrometheusSanitizer.buildMetricName(null, "subsystem", "metric"));
        assertEquals("metric", PrometheusSanitizer.buildMetricName(null, null, "metric"));
        assertThrows(IllegalArgumentException.class, () -> PrometheusSanitizer.buildMetricName(null, null, ""));
    }
}
