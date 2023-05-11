/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

    if (statsJson.has("count") && statsJson.getLong("count") == 0) {
      // if empty data, ignore the rest of statistics
      fds.setCount(0L);
      return fds;
    }

    if (statsJson.has("unique_values")) {
      // if stats for transformation function, save unique values as extended statistics
      JSONObject extStats = new JSONObject();
      extStats.append("unique_values", statsJson.get("unique_values"));
      fds.setExtendedStatistics(extStats.toString());
      return fds;
    }

    // common for all data types
    fds.setFeatureType(statsJson.getString("dataType"));
    fds.setCount(Long.valueOf(statsJson.getInt("numRecordsNull") + statsJson.getInt("numRecordsNonNull")));
    fds.setCompleteness(statsJson.getDouble("completeness"));
    fds.setNumNonNullValues(statsJson.getLong("numRecordsNonNull"));
    fds.setNumNullValues(statsJson.getLong("numRecordsNull"));
    fds.setApproxNumDistinctValues(statsJson.getLong("approximateNumDistinctValues"));

    if (statsJson.has("uniqueness")) {
      // commmon for all data types if exact_uniqueness is enabled
      fds.setUniqueness(statsJson.getDouble("uniqueness"));
      fds.setEntropy(statsJson.getDouble("entropy"));
      fds.setDistinctness(statsJson.getDouble("distinctness"));
      fds.setExactNumDistinctValues(statsJson.getLong("exactNumDistinctValues"));
    }

    if (statsJson.getString("dataType").equals("Fractional")
        || statsJson.getString("dataType").equals("Integral")) {
      fds.setMin(statsJson.getDouble("minimum"));
      fds.setMax(statsJson.getDouble("maximum"));
      fds.setSum(statsJson.getDouble("sum"));
      fds.setMean(statsJson.getDouble("mean"));
      fds.setStddev(statsJson.getDouble("stdDev"));
    }

    JSONObject extendedStatistics = new JSONObject();
    if (statsJson.has("correlations")) {
      extendedStatistics.append("correlations", statsJson.get("correlations"));
    }
    if (statsJson.has("histogram")) {
      extendedStatistics.append("histogram", statsJson.get("histogram"));
    }
    if (statsJson.has("kll")) {
      extendedStatistics.append("kll", statsJson.get("kll"));
    }
    if (extendedStatistics.length() > 0) {
      fds.setExtendedStatistics(extendedStatistics.toString());
    }

    return fds;
  }
}
