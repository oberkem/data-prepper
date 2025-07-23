/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.record.Record;

import org.opensearch.dataprepper.plugins.certificate.s3.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.prometheus.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.prometheus.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.client.RemoteWriteClient;
import org.opensearch.dataprepper.plugins.sink.prometheus.client.RemoteWriteClientFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.client.RemoteWriteResponse;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.BasicAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.BearerTokenAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.MultiAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.util.PrometheusSinkUtil;
import org.opensearch.dataprepper.plugins.sink.prometheus.util.PrometheusSanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.sink.prometheus.handler.BearerTokenAuthPrometheusSinkHandler.AUTHORIZATION;

/**
 * This service class contains logic for sending data to Http Endpoints
 */
public class PrometheusSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);

    private final Collection<EventHandle> bufferedEventHandles;

    private final PrometheusSinkConfiguration prometheusSinkConfiguration;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private DlqPushHandler dlqPushHandler;

    private final Lock reentrantLock;

    private final HttpClientBuilder httpClientBuilder;

    private final OAuthAccessTokenManager oAuthAccessTokenManager;

    private CertificateProviderFactory certificateProviderFactory;

    private HttpClientConnectionManager httpClientConnectionManager;

    private final PluginSetting httpPluginSetting;

    private MultiAuthPrometheusSinkHandler multiAuthPrometheusSinkHandler;

    private final RemoteWriteClient remoteWriteClient;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Pattern PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_:]");
    private static final Pattern BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_:]");

    private final Counter prometheusSinkRecordsSuccessCounter;

    private final Counter prometheusSinkRecordsFailedCounter;

    public static final String PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER = "prometheusSinkRecordsSuccessPushToEndPoint";

    public static final String PROMETHEUS_SINK_RECORDS_FAILED_COUNTER = "prometheusSinkRecordsFailedToPushEndPoint";

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final DlqPushHandler dlqPushHandler,
                                 final HttpClientBuilder httpClientBuilder,
                                 final PluginMetrics pluginMetrics,
                                 final PluginSetting httpPluginSetting){
        this.prometheusSinkConfiguration = prometheusSinkConfiguration;
        this.dlqPushHandler = dlqPushHandler;
        this.reentrantLock = new ReentrantLock();
        this.bufferedEventHandles = new LinkedList<>();
        this.httpClientBuilder = httpClientBuilder;
        this.httpPluginSetting = httpPluginSetting;
        this.oAuthAccessTokenManager = new OAuthAccessTokenManager();
        if ((!prometheusSinkConfiguration.isInsecureSkipVerify()) || (prometheusSinkConfiguration.useAcmCertForSSL())) {
            this.certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration.useAcmCertForSSL(),
                    prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion(), prometheusSinkConfiguration.getAcmCertificateArn(),
                    prometheusSinkConfiguration.getAcmCertIssueTimeOutMillis(), prometheusSinkConfiguration.getAcmPrivateKeyPassword(),
                    prometheusSinkConfiguration.isSslCertAndKeyFileInS3(), prometheusSinkConfiguration.getSslCertificateFile(),
                    prometheusSinkConfiguration.getSslKeyFile());
            this.httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(prometheusSinkConfiguration, certificateProviderFactory);
        }
        else{
            try {
                this.httpClientConnectionManager = new HttpClientSSLConnectionManager().createHttpClientConnectionManagerWithoutValidation();
            }catch(NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex){
                LOG.error("Exception while insecure_skip_verify is true ",ex);
            }
        }
        this.prometheusSinkRecordsSuccessCounter = pluginMetrics.counter(PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER);
        this.prometheusSinkRecordsFailedCounter = pluginMetrics.counter(PROMETHEUS_SINK_RECORDS_FAILED_COUNTER);
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(prometheusSinkConfiguration);
        
        // Initialize the dedicated RemoteWriteClient with proper configuration
        this.remoteWriteClient = RemoteWriteClientFactory.createClient(
            prometheusSinkConfiguration, 
            httpClientConnectionManager, 
            pluginMetrics
        );
    }

    /**
     * This method process buffer records and send to Http End points based on configured codec
     * @param records Collection of Event
     */
    public void output(final Collection<Record<Event>> records) {
        reentrantLock.lock();
        try {
            records.forEach(record -> {
                final Event event = record.getData();
                byte[] bytes = null;
                if (event.getMetadata().getEventType().equals("METRIC")) {
                    Remote.WriteRequest message = null;
                    if (event instanceof JacksonGauge) {
                        final JacksonGauge jacksonGauge = (JacksonGauge) event;
                        message = buildGaugeWriteRequest(jacksonGauge);
                    } else if (event instanceof JacksonSum) {
                        final JacksonSum jacksonSum = (JacksonSum) event;
                        message = buildSumWriteRequest(jacksonSum);
                    } else if (event instanceof JacksonSummary) {
                        final JacksonSummary jacksonSummary = (JacksonSummary) event;
                        message = buildSummaryWriteRequest(jacksonSummary);
                    } else if (event instanceof JacksonHistogram) {
                        final JacksonHistogram jacksonHistogram = (JacksonHistogram) event;
                        message = buildHistogramWriteRequest(jacksonHistogram);
                    } else if (event instanceof JacksonExponentialHistogram) {
                        final JacksonExponentialHistogram jacksonExpHistogram = (JacksonExponentialHistogram) event;
                        message = buildExponentialHistogramWriteRequest(jacksonExpHistogram);
                    } else {
                        LOG.error("No valid Event type found");
                    }
                    if( message != null && message.toByteArray() != null)
                        bytes = message.toByteArray();
                }
                if (event.getEventHandle() != null) {
                    this.bufferedEventHandles.add(event.getEventHandle());
                }
                if(message != null){
                    // Use the dedicated RemoteWriteClient with built-in retry logic and error handling
                    RemoteWriteResponse response = remoteWriteClient.send(message);
                    
                    if (response.isSuccess()) {
                        LOG.debug("Remote write request sent successfully");
                        releaseEventHandles(Boolean.TRUE);
                    } else {
                        LOG.error("Failed to send remote write request: {}", response.getErrorMessage());
                        
                        // Convert RemoteWriteResponse to HttpEndPointResponse for DLQ compatibility
                        HttpEndPointResponse failedResponse = new HttpEndPointResponse(
                            prometheusSinkConfiguration.getUrl(), 
                            response.getStatusCode(), 
                            response.getErrorMessage()
                        );
                        
                        logFailedData(failedResponse);
                        releaseEventHandles(Boolean.FALSE);
                    }
                }});

        }finally {
            reentrantLock.unlock();
        }
    }

    /**
     * * This method build Remote.WriteRequest
     *  @param time time
     *  @param startTime start time
     *  @param value value
     *  @param attributeMap attributes
     *  @param metricName metricName
     */
    private static Remote.WriteRequest buildRemoteWriteRequest(final String time, final String startTime,
                                                               final Double value, final Map<String, Object> attributeMap, final String metricName) {
final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
writeRequestBuilder.addAllMetadata(buildMetadata(event));

        final Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();

        final List<Types.Label> arrayList = new ArrayList<>();

        setMetricName(metricName, arrayList);
        prepareLabelList(attributeMap, arrayList);

        final Types.Sample.Builder prometheusSampleBuilder = Types.Sample.newBuilder();
        long timeStampVal;
        if (time != null) {
            timeStampVal = getTimeStampVal(time);
        } else {
            timeStampVal = getTimeStampVal(startTime);
        }

        prometheusSampleBuilder.setValue(value).setTimestamp(timeStampVal);
        final Types.Sample prometheusSample = prometheusSampleBuilder.build();

        timeSeriesBuilder.addAllLabels(arrayList);
        timeSeriesBuilder.addAllSamples(Arrays.asList(prometheusSample));

// Add exemplars to timeSeriesBuilder
addExemplarsToTimeSeries(event, timeSeriesBuilder);
writeRequestBuilder.addAllTimeseries(Arrays.asList(timeSeriesBuilder.build()));

        return writeRequestBuilder.build();
    }

    /**
     * Build WriteRequest for Gauge metrics
     * @param gauge JacksonGauge metric
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest buildGaugeWriteRequest(final JacksonGauge gauge) {
        return buildRemoteWriteRequest(gauge.getTime(), gauge.getStartTime(), 
                gauge.getValue(), gauge.getAttributes(), PrometheusSanitizer.sanitizeMetricName(gauge.getName()));
    }

    /**
     * Build WriteRequest for Sum metrics with proper cumulative/delta handling
     * @param sum JacksonSum metric
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest buildSumWriteRequest(final JacksonSum sum) {
        String metricName = sum.getName();
        
        // For monotonic counters, ensure _total suffix for Prometheus compatibility
        if (sum.isMonotonic() && !metricName.endsWith("_total")) {
            metricName = metricName + "_total";
        }
        
        return buildRemoteWriteRequest(sum.getTime(), sum.getStartTime(), 
                sum.getValue(), sum.getAttributes(), PrometheusSanitizer.sanitizeMetricName(metricName));
    }

    /**
     * Build WriteRequest for Summary metrics with quantiles handling
     * @param summary JacksonSummary metric
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest buildSummaryWriteRequest(final JacksonSummary summary) {
        final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        final List<Types.TimeSeries> timeSeriesList = new ArrayList<>();
        
        final String baseName = summary.getName();
        final long timestamp = getTimeStampVal(summary.getTime() != null ? summary.getTime() : summary.getStartTime());
        
        // Create quantile time series
        if (summary.getQuantiles() != null) {
            for (final var quantile : summary.getQuantiles()) {
                final List<Types.Label> labels = new ArrayList<>();
                setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName), labels);
                prepareLabelList(summary.getAttributes(), labels);
                
                // Add quantile label
                labels.add(Types.Label.newBuilder()
                        .setName("quantile")
                        .setValue(String.valueOf(quantile.getQuantile()))
                        .build());
                
                final Types.Sample sample = Types.Sample.newBuilder()
                        .setValue(quantile.getValue())
                        .setTimestamp(timestamp)
                        .build();
                
                timeSeriesList.add(Types.TimeSeries.newBuilder()
                        .addAllLabels(labels)
                        .addSamples(sample)
                        .build());
            }
        }
        
        // Create _sum time series
        if (summary.getSum() != null) {
            final List<Types.Label> sumLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_sum"), sumLabels);
            prepareLabelList(summary.getAttributes(), sumLabels);
            
            final Types.Sample sumSample = Types.Sample.newBuilder()
                    .setValue(summary.getSum())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(sumLabels)
                    .addSamples(sumSample)
                    .build());
        }
        
        // Create _count time series
        if (summary.getCount() != null) {
            final List<Types.Label> countLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_count"), countLabels);
            prepareLabelList(summary.getAttributes(), countLabels);
            
            final Types.Sample countSample = Types.Sample.newBuilder()
                    .setValue(summary.getCount().doubleValue())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(countLabels)
                    .addSamples(countSample)
                    .build());
        }
        
        writeRequestBuilder.addAllTimeseries(timeSeriesList);
        return writeRequestBuilder.build();
    }

    /**
     * Build WriteRequest for Histogram metrics with buckets handling
     * @param histogram JacksonHistogram metric
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest buildHistogramWriteRequest(final JacksonHistogram histogram) {
        final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        final List<Types.TimeSeries> timeSeriesList = new ArrayList<>();
        
        final String baseName = histogram.getName();
        final long timestamp = getTimeStampVal(histogram.getTime() != null ? histogram.getTime() : histogram.getStartTime());
        
        // Create bucket time series
        if (histogram.getBuckets() != null && histogram.getExplicitBoundsList() != null) {
            final List<Double> bounds = histogram.getExplicitBoundsList();
            final List<? extends org.opensearch.dataprepper.model.metric.Bucket> buckets = histogram.getBuckets();
            
            for (int i = 0; i < bounds.size() && i < buckets.size(); i++) {
                final List<Types.Label> bucketLabels = new ArrayList<>();
                setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_bucket"), bucketLabels);
                prepareLabelList(histogram.getAttributes(), bucketLabels);
                
                // Add 'le' (less than or equal) label for bucket boundary
                final String leValue = bounds.get(i).equals(Double.POSITIVE_INFINITY) ? "+Inf" : String.valueOf(bounds.get(i));
                bucketLabels.add(Types.Label.newBuilder()
                        .setName("le")
                        .setValue(leValue)
                        .build());
                
                final Types.Sample bucketSample = Types.Sample.newBuilder()
                        .setValue(buckets.get(i).getCount().doubleValue())
                        .setTimestamp(timestamp)
                        .build();
                
                timeSeriesList.add(Types.TimeSeries.newBuilder()
                        .addAllLabels(bucketLabels)
                        .addSamples(bucketSample)
                        .build());
            }
            
            // Always add +Inf bucket if not present
            boolean hasInfBucket = bounds.stream().anyMatch(bound -> bound.equals(Double.POSITIVE_INFINITY));
            if (!hasInfBucket) {
                final List<Types.Label> infBucketLabels = new ArrayList<>();
                setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_bucket"), infBucketLabels);
                prepareLabelList(histogram.getAttributes(), infBucketLabels);
                
                infBucketLabels.add(Types.Label.newBuilder()
                        .setName("le")
                        .setValue("+Inf")
                        .build());
                
                final Types.Sample infBucketSample = Types.Sample.newBuilder()
                        .setValue(histogram.getCount() != null ? histogram.getCount().doubleValue() : 0.0)
                        .setTimestamp(timestamp)
                        .build();
                
                timeSeriesList.add(Types.TimeSeries.newBuilder()
                        .addAllLabels(infBucketLabels)
                        .addSamples(infBucketSample)
                        .build());
            }
        }
        
        // Create _sum time series
        if (histogram.getSum() != null) {
            final List<Types.Label> sumLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_sum"), sumLabels);
            prepareLabelList(histogram.getAttributes(), sumLabels);
            
            final Types.Sample sumSample = Types.Sample.newBuilder()
                    .setValue(histogram.getSum())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(sumLabels)
                    .addSamples(sumSample)
                    .build());
        }
        
        // Create _count time series
        if (histogram.getCount() != null) {
            final List<Types.Label> countLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_count"), countLabels);
            prepareLabelList(histogram.getAttributes(), countLabels);
            
            final Types.Sample countSample = Types.Sample.newBuilder()
                    .setValue(histogram.getCount().doubleValue())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(countLabels)
                    .addSamples(countSample)
                    .build());
        }
        
        writeRequestBuilder.addAllTimeseries(timeSeriesList);
        return writeRequestBuilder.build();
    }

    /**
     * Build WriteRequest for ExponentialHistogram metrics with conversion to classical buckets
     * @param expHistogram JacksonExponentialHistogram metric
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest buildExponentialHistogramWriteRequest(final JacksonExponentialHistogram expHistogram) {
        final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        final List<Types.TimeSeries> timeSeriesList = new ArrayList<>();
        
        final String baseName = expHistogram.getName();
        final long timestamp = getTimeStampVal(expHistogram.getTime() != null ? expHistogram.getTime() : expHistogram.getStartTime());
        
        // Convert exponential histogram to classical histogram buckets
        final List<ConvertedBucket> convertedBuckets = convertExponentialToBuckets(expHistogram);
        
        // Create bucket time series
        for (final ConvertedBucket bucket : convertedBuckets) {
            final List<Types.Label> bucketLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_bucket"), bucketLabels);
            prepareLabelList(expHistogram.getAttributes(), bucketLabels);
            
            // Add 'le' label for bucket boundary
            bucketLabels.add(Types.Label.newBuilder()
                    .setName("le")
                    .setValue(bucket.upperBound)
                    .build());
            
            final Types.Sample bucketSample = Types.Sample.newBuilder()
                    .setValue(bucket.cumulativeCount)
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(bucketLabels)
                    .addSamples(bucketSample)
                    .build());
        }
        
        // Create _sum time series
        if (expHistogram.getSum() != null) {
            final List<Types.Label> sumLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_sum"), sumLabels);
            prepareLabelList(expHistogram.getAttributes(), sumLabels);
            
            final Types.Sample sumSample = Types.Sample.newBuilder()
                    .setValue(expHistogram.getSum())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(sumLabels)
                    .addSamples(sumSample)
                    .build());
        }
        
        // Create _count time series
        if (expHistogram.getCount() != null) {
            final List<Types.Label> countLabels = new ArrayList<>();
            setMetricName(PrometheusSanitizer.sanitizeMetricName(baseName + "_count"), countLabels);
            prepareLabelList(expHistogram.getAttributes(), countLabels);
            
            final Types.Sample countSample = Types.Sample.newBuilder()
                    .setValue(expHistogram.getCount().doubleValue())
                    .setTimestamp(timestamp)
                    .build();
            
            timeSeriesList.add(Types.TimeSeries.newBuilder()
                    .addAllLabels(countLabels)
                    .addSamples(countSample)
                    .build());
        }
        
        writeRequestBuilder.addAllTimeseries(timeSeriesList);
        return writeRequestBuilder.build();
    }

    /**
     * Convert exponential histogram to classical histogram buckets
     * @param expHistogram exponential histogram to convert
     * @return list of converted buckets
     */
    private List<ConvertedBucket> convertExponentialToBuckets(final JacksonExponentialHistogram expHistogram) {
        final List<ConvertedBucket> buckets = new ArrayList<>();
        final Integer scale = expHistogram.getScale();
        final Double base = scale != null ? Math.pow(2.0, Math.pow(2.0, -scale)) : 2.0;
        
        long cumulativeCount = 0L;
        
        // Add zero bucket if present
        if (expHistogram.getZeroCount() != null && expHistogram.getZeroCount() > 0) {
            final Double zeroThreshold = expHistogram.getZeroThreshold() != null ? expHistogram.getZeroThreshold() : 0.0;
            cumulativeCount += expHistogram.getZeroCount();
            buckets.add(new ConvertedBucket(String.valueOf(zeroThreshold), cumulativeCount));
        }
        
        // Convert negative buckets
        if (expHistogram.getNegative() != null && expHistogram.getNegativeOffset() != null) {
            final List<Long> negativeCounts = expHistogram.getNegative();
            final int negativeOffset = expHistogram.getNegativeOffset();
            
            for (int i = 0; i < negativeCounts.size(); i++) {
                final int bucketIndex = negativeOffset + i;
                final double upperBound = -Math.pow(base, bucketIndex);
                cumulativeCount += negativeCounts.get(i);
                buckets.add(new ConvertedBucket(String.valueOf(upperBound), cumulativeCount));
            }
        }
        
        // Convert positive buckets
        if (expHistogram.getPositive() != null && expHistogram.getPositiveOffset() != null) {
            final List<Long> positiveCounts = expHistogram.getPositive();
            final int positiveOffset = expHistogram.getPositiveOffset();
            
            for (int i = 0; i < positiveCounts.size(); i++) {
                final int bucketIndex = positiveOffset + i;
                final double upperBound = Math.pow(base, bucketIndex);
                cumulativeCount += positiveCounts.get(i);
                buckets.add(new ConvertedBucket(String.valueOf(upperBound), cumulativeCount));
            }
        }
        
        // Always add +Inf bucket with total count
        final Long totalCount = expHistogram.getCount();
        if (totalCount != null) {
            buckets.add(new ConvertedBucket("+Inf", totalCount));
        }
        
        return buckets;
    }

    /**
     * Helper class for converted exponential histogram buckets
     */
    private static class ConvertedBucket {
        final String upperBound;
        final long cumulativeCount;
        
        ConvertedBucket(String upperBound, long cumulativeCount) {
            this.upperBound = upperBound;
            this.cumulativeCount = cumulativeCount;
        }
    }

    private static void prepareLabelList(final Map<String, Object> hashMap, final List<Types.Label> arrayList) {
        for (final Map.Entry<String, Object> entry : hashMap.entrySet()) {
            final String key = PrometheusSanitizer.sanitizeLabelName(entry.getKey());
            final Object value = entry.getValue();
            if (entry.getValue() instanceof Map) {
                final Object innerMap = entry.getValue();
                prepareLabelList(objectMapper.convertValue(innerMap, Map.class), arrayList);
                continue;
            }
            final Types.Label.Builder labelBuilder = Types.Label.newBuilder();
            labelBuilder.setName(key).setValue(PrometheusSanitizer.sanitizeLabelValue(value.toString()));
            final Types.Label label = labelBuilder.build();
            arrayList.add(label);
        }
    }

    private static String sanitizeName(final String name) {
        return BODY_PATTERN
                .matcher(PREFIX_PATTERN.matcher(name).replaceFirst("_"))
                .replaceAll("_");
    }

    private static long getTimeStampVal(final String time) {
        if (time == null) {
            return System.currentTimeMillis();
        }
        long timeStampVal = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        try {
            Date date = sdf.parse(time);
            timeStampVal = date.getTime();
        } catch (ParseException e) {
            LOG.warn("Failed to parse timestamp '{}', using current time", time, e);
            timeStampVal = System.currentTimeMillis();
        }
        return timeStampVal;
    }

    /**
     * * This method logs Failed Data to DLQ and Webhook
     *  @param endPointResponses HttpEndPointResponses.
     */
    private void logFailedData(final HttpEndPointResponse endPointResponses) {
        final FailedDlqData failedDlqData =
                FailedDlqData.builder()
                        .withUrl(endPointResponses.getUrl())
                        .withMessage(endPointResponses.getErrorMessage())
                        .withStatus(endPointResponses.getStatusCode()).build();
        LOG.info("Failed to push the data. Failed DLQ Data: {}",failedDlqData);

        logFailureForDlqObjects(failedDlqData);
    }

    private void releaseEventHandles(final boolean result) {
        for (final EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    /**
     * * This method pushes bufferData to configured HttpEndPoints
     *  @param data byte[] data.
     */
    private HttpEndPointResponse pushToEndPoint(final byte[] data) {
        HttpEndPointResponse httpEndPointResponses = null;
        final ClassicRequestBuilder classicHttpRequestBuilder =
                httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getClassicHttpRequestBuilder();

        classicHttpRequestBuilder.addHeader("Content-Encoding", prometheusSinkConfiguration.getEncoding());
        classicHttpRequestBuilder.addHeader("Content-Type", prometheusSinkConfiguration.getContentType());
        classicHttpRequestBuilder.addHeader("X-Prometheus-Remote-Write-Version", prometheusSinkConfiguration.getRemoteWriteVersion());

        try {
            final byte[] compressedBufferData = Snappy.compress(data);
            final HttpEntity entity = new ByteArrayEntity(compressedBufferData,
                    ContentType.create(prometheusSinkConfiguration.getContentType()), prometheusSinkConfiguration.getEncoding());

            classicHttpRequestBuilder.setEntity(entity);
            if(AuthTypeOptions.BEARER_TOKEN.equals(prometheusSinkConfiguration.getAuthType()))
                accessTokenIfExpired(prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions().getTokenExpired(),prometheusSinkConfiguration.getUrl());

            httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getHttpClientBuilder().build()
                    .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
            LOG.info("Records successfully pushed to endpoint {}", prometheusSinkConfiguration.getUrl());
            prometheusSinkRecordsSuccessCounter.increment();
        } catch (IOException e) {
            prometheusSinkRecordsFailedCounter.increment();
            LOG.info("Records failed to push endpoint {}");
            LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ", prometheusSinkConfiguration.getUrl(), e);
            httpEndPointResponses = new HttpEndPointResponse(prometheusSinkConfiguration.getUrl(), HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return httpEndPointResponses;
    }

    /**
     * * This method sends Failed objects to DLQ
     *  @param failedDlqData FailedDlqData.
     */
    private void logFailureForDlqObjects(final FailedDlqData failedDlqData){
        dlqPushHandler.perform(httpPluginSetting, failedDlqData);
    }


    /**
     * * This method gets Auth Handler classes based on configuration
     *  @param authType AuthTypeOptions.
     *  @param authOptions HttpAuthOptions.Builder.
     */
    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){
        switch(authType) {
            case HTTP_BASIC:
                multiAuthPrometheusSinkHandler = new BasicAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getUsername(),
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getPassword(),
                        httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthPrometheusSinkHandler = new BearerTokenAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions(),
                        httpClientConnectionManager, oAuthAccessTokenManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setHttpClientBuilder(httpClientBuilder
                        .setConnectionManager(httpClientConnectionManager)
                        .addResponseInterceptorLast(new FailedHttpResponseInterceptor(authOptions.getUrl()))).build();
        }
        return multiAuthPrometheusSinkHandler.authenticate(authOptions);
    }

    /**
     * * This method build HttpAuthOptions class based on configurations
     *  @param prometheusSinkConfiguration PrometheusSinkConfiguration.
     */
    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final PrometheusSinkConfiguration prometheusSinkConfiguration){
        final Map<String,HttpAuthOptions> authMap = new HashMap<>();

        final HTTPMethodOptions httpMethod = prometheusSinkConfiguration.getHttpMethod();
        final AuthTypeOptions authType =  prometheusSinkConfiguration.getAuthType();
        final String proxyUrlString =  prometheusSinkConfiguration.getProxy();
        final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(prometheusSinkConfiguration.getUrl());



        if(Objects.nonNull(prometheusSinkConfiguration.getCustomHeaderOptions()))
            addCustomHeaders(classicRequestBuilder,prometheusSinkConfiguration.getCustomHeaderOptions());

        if(prometheusSinkConfiguration.getAwsAuthenticationOptions().isAwsSigv4() && prometheusSinkConfiguration.isValidAWSUrl()){
            classicRequestBuilder.addHeader("x-amz-content-sha256","required");
        }

        if(Objects.nonNull(proxyUrlString)) {
            httpClientBuilder.setProxy(PrometheusSinkUtil.getHttpHostByURL(PrometheusSinkUtil.getURLByUrlString(proxyUrlString)));
            LOG.info("sending data via proxy {}",proxyUrlString);
        }

        if(prometheusSinkConfiguration.getRequestTimout() != null) {
            httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofMilliseconds(prometheusSinkConfiguration.getRequestTimout().toMillis())).build());
        }

        final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                .setUrl(prometheusSinkConfiguration.getUrl())
                .setClassicHttpRequestBuilder(classicRequestBuilder)
                .setHttpClientBuilder(httpClientBuilder);

        authMap.put(prometheusSinkConfiguration.getUrl(),getAuthHandlerByConfig(authType,authOptions));
        return authMap;
    }

    /**
     * * This method adds SageMakerHeaders as custom Header in the request
     *  @param classicRequestBuilder ClassicRequestBuilder.
     *  @param customHeaderOptions CustomHeaderOptions .
     */
    private void addCustomHeaders(final ClassicRequestBuilder classicRequestBuilder,
                                  final Map<String, List<String>> customHeaderOptions) {

        customHeaderOptions.forEach((k, v) -> classicRequestBuilder.addHeader(k,v.toString()));
    }

    /**
     * * builds ClassicRequestBuilder based on configured HttpMethod
     *  @param httpMethodOptions Http Method.
     */
    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch (httpMethodOptions) {
            case PUT:
                classicRequestBuilder = ClassicRequestBuilder.put();
                break;
            case POST:
            default:
                classicRequestBuilder = ClassicRequestBuilder.post();
                break;
        }
        return classicRequestBuilder;
    }

    private void accessTokenIfExpired(final Integer tokenExpired,final String url){
        if(oAuthAccessTokenManager.isTokenExpired(tokenExpired)) {
            httpAuthOptions.get(url).getClassicHttpRequestBuilder()
                    .setHeader(AUTHORIZATION, oAuthAccessTokenManager.getAccessToken(prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions()));
        }
    }

    private static void setMetricName(final String metricName, final List<Types.Label> arrayList) {
        final Types.Label.Builder labelBuilder = Types.Label.newBuilder();
        labelBuilder.setName("__name__").setValue(metricName);
        final Types.Label label = labelBuilder.build();
        arrayList.add(label);
    }
    
    /**
     * Clean up resources when the service is shutdown
     */
    public void shutdown() {
        try {
            if (remoteWriteClient != null) {
                remoteWriteClient.close();
                LOG.info("RemoteWriteClient closed successfully");
            }
        } catch (IOException e) {
            LOG.error("Error closing RemoteWriteClient during shutdown", e);
        }
    }
}
