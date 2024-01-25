/*
 *  Copyright (c) 2020-2024. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.json.JSONObject;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class FeatureDescriptiveStatistics extends RestDto<FeatureDescriptiveStatistics> {
  private Integer id;
  private String featureType;
  private String featureName;

  // for any feature type
  private Long count;
  private Double completeness;
  private Long numNonNullValues;
  private Long numNullValues;
  private Long approxNumDistinctValues;

  // for numerical features
  private Double min;
  private Double max;
  private Double sum;
  private Double mean;
  private Double stddev;
  private Map<String, Double> percentiles;

  // with exact uniqueness
  private Double distinctness;
  private Double entropy;
  private Double uniqueness;
  private Long exactNumDistinctValues;

  // histogram, correlations, kll <- from hdfs file
  private String extendedStatistics;

  public static FeatureDescriptiveStatistics fromDeequStatisticsJson(JSONObject statsJson) {
    FeatureDescriptiveStatistics fds = new FeatureDescriptiveStatistics();
    fds.setFeatureName(statsJson.getString("column"));

    if (statsJson.has("dataType")) {
      fds.setFeatureType(statsJson.getString("dataType"));
    }

    if (statsJson.has("count") && statsJson.getLong("count") == 0) {
      // if empty data, ignore the rest of statistics
      fds.setCount(0L);
      return fds;
    }

    // common for all data types
    if (statsJson.has("numRecordsNull")) {
      fds.setNumNullValues(statsJson.getLong("numRecordsNull"));
    }
    if (statsJson.has("numRecordsNonNull")) {
      fds.setNumNonNullValues(statsJson.getLong("numRecordsNonNull"));
    }
    if (statsJson.has("numRecordsNull") && statsJson.has("numRecordsNonNull")) {
      fds.setCount(Long.valueOf(statsJson.getInt("numRecordsNull") + statsJson.getInt("numRecordsNonNull")));
    }
    if (statsJson.has("count")) {
      fds.setCount(statsJson.getLong("count"));
    }
    if (statsJson.has("completeness")) {
      fds.setCompleteness(statsJson.getDouble("completeness"));
    }
    if (statsJson.has("approximateNumDistinctValues")) {
      fds.setApproxNumDistinctValues(statsJson.getLong("approximateNumDistinctValues"));
    }

    // commmon for all data types if exact_uniqueness is enabled
    if (statsJson.has("uniqueness")) {
      fds.setUniqueness(statsJson.getDouble("uniqueness"));
    }
    if (statsJson.has("entropy")) {
      fds.setEntropy(statsJson.getDouble("entropy"));
    }
    if (statsJson.has("distinctness")) {
      fds.setDistinctness(statsJson.getDouble("distinctness"));
    }
    if (statsJson.has("exactNumDistinctValues")) {
      fds.setExactNumDistinctValues(statsJson.getLong("exactNumDistinctValues"));
    }

    // fractional / integral features
    if (statsJson.has("minimum")) {
      fds.setMin(statsJson.getDouble("minimum"));
    }
    if (statsJson.has("maximum")) {
      fds.setMax(statsJson.getDouble("maximum"));
    }
    if (statsJson.has("sum")) {
      fds.setSum(statsJson.getDouble("sum"));
    }
    if (statsJson.has("mean")) {
      fds.setMean(statsJson.getDouble("mean"));
    }
    if (statsJson.has("stdDev")) {
      fds.setStddev(statsJson.getDouble("stdDev"));
    }

    JSONObject extendedStatistics = new JSONObject();
    if (statsJson.has("correlations")) {
      extendedStatistics.put("correlations", statsJson.getJSONArray("correlations"));
    }
    if (statsJson.has("histogram")) {
      extendedStatistics.put("histogram", statsJson.getJSONArray("histogram"));
    }
    if (statsJson.has("kll")) {
      extendedStatistics.put("kll", statsJson.get("kll"));
    }
    if (statsJson.has("unique_values")) {
      extendedStatistics.put("unique_values", statsJson.getJSONArray("unique_values"));
    }
    if (extendedStatistics.length() > 0) {
      fds.setExtendedStatistics(extendedStatistics.toString());
    }

    return fds;
  }
}
