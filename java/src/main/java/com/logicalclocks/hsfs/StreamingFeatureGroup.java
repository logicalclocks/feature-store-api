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
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.StreamingFeatureGroupOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamingFeatureGroup  extends FeatureGroupBase {
    @Getter
    @Setter
    private StorageConnector onlineStorageConnector;

    @Getter
    @Setter
    private StorageConnector offlineStorageConnector;

    @Getter
    @Setter
    private String type = "streamingFeaturegroupDTO";

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
    private List<StreamingFeatureGroupOptions> options;
}
