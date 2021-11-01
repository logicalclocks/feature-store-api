/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.engine.StreamFeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.StreamFeatureGroupOptions;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

    @Getter
    @Setter
    private Boolean onlineEnabled = true;

    @Getter
    @Setter
    private StorageConnector onlineStorageConnector;

    @Getter
    @Setter
    private StorageConnector offlineStorageConnector;

    @Getter
    @Setter
    private String type = "streamFeaturegroupDTO";

    @Getter
    @Setter
    private TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

    @Getter
    @Setter
    protected String location;

    @Getter
    @Setter
    private List<String> statisticColumns;

    @JsonIgnore
    // These are only used in the client. In the server they are aggregated in the `features` field
    private List<String> partitionKeys;

    @JsonIgnore
    // This is only used in the client. In the server they are aggregated in the `features` field
    private String hudiPrecombineKey;

    @JsonIgnore
    private String avroSchema;

    @Getter
    @Setter
    private String onlineTopicName;

    @Getter
    @Setter
    private List<StreamFeatureGroupOptions> options;

    private final StreamFeatureGroupEngine streamFeatureGroupEngine = new StreamFeatureGroupEngine();
    private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
    private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

    @Builder
    public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                        List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                              List<Feature> features,
                              StatisticsConfig statisticsConfig, ValidationType validationType,
                              scala.collection.Seq<Expectation> expectations, String onlineTopicName, String eventTime) {
        this.featureStore = featureStore;
        this.name = name;
        this.version = version;
        this.description = description;
        this.primaryKeys = primaryKeys != null
                ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
        this.partitionKeys = partitionKeys != null
                ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
        this.hudiPrecombineKey = timeTravelFormat == TimeTravelFormat.HUDI && hudiPrecombineKey != null
                ? hudiPrecombineKey.toLowerCase() : null;
        this.features = features;
        this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
        this.validationType = validationType != null ? validationType : ValidationType.NONE;
        if (expectations != null && !expectations.isEmpty()) {
            this.expectationsNames = new ArrayList<>();
            this.expectations = JavaConverters.seqAsJavaListConverter(expectations).asJava();
            this.expectations.forEach(expectation -> this.expectationsNames.add(expectation.getName()));
        }
        this.onlineTopicName = onlineTopicName;
        this.eventTime = eventTime;
    }

    public <T,C> C insertStream(T featureData)
            throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException {
        return insertStream(featureData, null);
    }

    public <T,C> C insertStream(T featureData, String queryName)
            throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException {
        return insertStream(featureData, queryName, "append");
    }

    public <T,C> C insertStream(T featureData, String queryName, String outputMode)
            throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException {
        return insertStream(featureData, queryName, outputMode, false, null);
    }

    public <T,C> C insertStream(T featureData, String queryName, String outputMode,
                                       boolean awaitTermination, Long timeout)
            throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException {
        return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null);
    }

    public <T,C> C insertStream(T featureData, String queryName, String outputMode,
                              boolean awaitTermination, Long timeout, Map<String, String> writeOptions)
            throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException {
        /*
        if (!featureData.isStreaming()) {
            throw new FeatureStoreException(
                    "Features have to be a streaming type spark dataframe. Use `insert()` method instead.");
        }
        LOGGER.info("StatisticsWarning: Stream ingestion for feature group `" + name + "`, with version `" + version
                + "` will not compute statistics.");
        */
        return streamFeatureGroupEngine.insertStream(this, featureData, queryName, outputMode,
                awaitTermination, timeout, writeOptions);
    }

    @JsonIgnore
    public String getAvroSchema() throws FeatureStoreException, IOException {
        if (avroSchema == null) {
            avroSchema = featureGroupEngine.getAvroSchema(this);
        }
        return avroSchema;
    }

    @JsonIgnore
    public List<String> getComplexFeatures() {
        return features.stream().filter(Feature::isComplex).map(Feature::getName).collect(Collectors.toList());
    }

    @JsonIgnore
    public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
        Schema schema = getDeserializedAvroSchema();
        Schema.Field complexField = schema.getFields().stream().filter(field ->
                field.name().equalsIgnoreCase(featureName)).findFirst().orElseThrow(() ->
                new FeatureStoreException(
                        "Complex feature `" + featureName + "` not found in AVRO schema of online feature group."));

        return complexField.schema().toString(true);
    }

    @JsonIgnore
    public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
        Schema schema = getDeserializedAvroSchema();
        List<Schema.Field> fields = schema.getFields().stream()
                .map(field -> getComplexFeatures().contains(field.name())
                        ? new Schema.Field(field.name(), SchemaBuilder.builder().nullable().bytesType(), null, null)
                        : new Schema.Field(field.name(), field.schema(), null, null))
                .collect(Collectors.toList());
        return Schema.createRecord(schema.getName(), null, schema.getNamespace(), schema.isError(), fields).toString(true);
    }

    @JsonIgnore
    public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
        try {
            return new Schema.Parser().parse(getAvroSchema());
        } catch (SchemaParseException e) {
            throw new FeatureStoreException("Failed to deserialize online feature group schema" + getAvroSchema() + ".");
        }
    }
}
