/*
 * Copyright (c) 2021 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine.hudi;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;

import java.io.IOException;
import java.util.Objects;

public class InitialCheckpointFromAnotherHoodieTimelineProvider extends InitialCheckPointProvider {
  private HoodieTableMetaClient anotherDsHoodieMetaclient;

  public InitialCheckpointFromAnotherHoodieTimelineProvider(TypedProperties props) {
    super(props);
  }

  public void init(Configuration config) throws HoodieException {
    super.init(config);
    this.anotherDsHoodieMetaclient = HoodieTableMetaClient.builder()
        .setConf(config)
        .setBasePath(this.path.toString())
        .build();
  }

  @SneakyThrows
  public String getCheckpoint() throws HoodieException {
    Configuration conf = new Configuration();
    Path filepath = new Path(this.path.toString());
    FileSystem fileSystem = FileSystem.get(filepath.toUri(), conf);
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(
        fileSystem, this.path.toString());

    return this.anotherDsHoodieMetaclient.getCommitsTimeline().filterCompletedInstants()
        .getReverseOrderedInstants().map((instant) -> {
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                this.anotherDsHoodieMetaclient.getActiveTimeline().getInstantDetails(instant).get(),
                HoodieCommitMetadata.class);

            String checkpointStr =  commitMetadata.getMetadata(HudiEngine.DELTASTREAMER_CHECKPOINT_KEY);
            if (checkpointStr != null) {
              return checkpointStr;
            } else {
              return props.get(HudiEngine.HUDI_KAFKA_TOPIC) + "";
            }
          } catch (IOException var3) {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .findFirst()
        .get();
  }
}
