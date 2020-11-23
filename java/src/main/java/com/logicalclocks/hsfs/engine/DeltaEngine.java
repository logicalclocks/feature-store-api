/*
 * Copyright (c) 2020 Logical Clocks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.commit.DeltaLakeUpdate;
import com.logicalclocks.hsfs.commit.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.ActionType;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;

import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class DeltaEngine {

  private static final String DELTA_SPARK_FORMAT = "delta";
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaEngine.class);

  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();

  public void saveDeltaLakeFeatureGroup(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> dataset,
                                        SaveMode saveMode, ActionType operation,
                                        Map<String, String> writeOptions) throws FeatureStoreException, IOException {

    if (operation == ActionType.BULK_INSERT) {
      dataset.write()
          .format(DELTA_SPARK_FORMAT)
          .partitionBy(utils.getPartitionColumns(featureGroup))
          // write options cannot be null
          .options(writeOptions == null ? new HashMap<>() : writeOptions)
          .mode(saveMode)
          .saveAsTable(utils.getTableName(featureGroup));

      //dataset.write().format(DELTA_SPARK_FORMAT).saveAsTable(utils.getTableName(featureGroup));
      FeatureGroup apiFG = featureGroupApi.save(featureGroup);
      if (featureGroup.getVersion() == null) {
        LOGGER.info("VersionWarning: No version provided for creating feature group `" + featureGroup.getName()
                + "`, incremented version to `" + apiFG.getVersion() + "`.");
      }

      // Update the original object - Hopsworks returns the incremented version
      featureGroup.setId(apiFG.getId());
      featureGroup.setVersion(apiFG.getVersion());
      featureGroup.setLocation(apiFG.getLocation());
      featureGroup.setId(apiFG.getId());
      featureGroup.setCorrelations(apiFG.getCorrelations());
      featureGroup.setHistograms(apiFG.getHistograms());

    } else {
      DeltaLakeUpdate deltaLakeMetaData = new DeltaLakeUpdate();
      String newDataAlias = utils.getFgName(featureGroup) + "_newData";
      deltaLakeMetaData.setExistingAlias(utils.getFgName(featureGroup));
      deltaLakeMetaData.setNewDataAlias(newDataAlias);

      DeltaLakeUpdate updatedDeltaLakeMetaData = featureGroupApi.constructDeltaLakeUpdate(featureGroup,
          deltaLakeMetaData);
      DeltaMergeBuilder deltaMergeBuilder = DeltaTable.forPath(sparkSession, featureGroup.getLocation())
          .as(utils.getFgName(featureGroup))
          .merge(
              dataset.as(newDataAlias),
              updatedDeltaLakeMetaData.getMergeCondQuery());

      if (operation == ActionType.UPSERT) {
        deltaMergeBuilder
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute();
      } else if (operation == ActionType.INSERT) {
        deltaMergeBuilder
            .execute();
      }

      // TODO (davit): decide if we want to add more types of delta updates
      // https://docs.delta.io/latest/delta-update.html#language-java
      // https://docs.delta.io/0.7.0/delta-update.html#upsert-into-a-table-using-merge
    }

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup);
    featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
  }

  // TODO (davit):
  public FeatureGroupCommit deleteRecord() {
    return null;
  }

  public void registerTemporaryTable(SparkSession sparkSession, FeatureGroup featureGroup, String alias,
                                     Long timestampAsOf, Map<String, String> userReadOptions) {

    Map<String, String> readOptions = new HashMap<>();
    readOptions.put("timestampAsOf", new Timestamp(timestampAsOf).toString());
    if (userReadOptions != null) {
      readOptions.putAll(userReadOptions);
    }
    sparkSession.read()
        .format("delta")
        .options(readOptions)
        .load(featureGroup.getLocation())
        .createOrReplaceTempView(alias);

  }

  public FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, FeatureGroup featureGroup) {
    Row lastCommit = DeltaTable.forPath(sparkSession, featureGroup.getLocation()).history(1).first();
    Long version = lastCommit.getAs("version");
    String timestamp = lastCommit.getAs("timestamp").toString();
    fgCommitMetadata.setCommitDateString(timestamp);
    fgCommitMetadata.setDeltaCommitVersion(version);

    Map<String, String> operationMetrics = JavaConverters.mapAsJavaMapConverter(
        (scala.collection.immutable.Map<String, String>) lastCommit.getAs("operationMetrics")).asJava();
    if (version == 0) {
      fgCommitMetadata.setRowsUpdated(0L);
      fgCommitMetadata.setRowsInserted(Long.parseLong(operationMetrics.get("numOutputRows")));
      fgCommitMetadata.setRowsDeleted(0L);
    } else {
      fgCommitMetadata.setRowsUpdated(Long.parseLong(operationMetrics.get("numTargetRowsUpdated")));
      fgCommitMetadata.setRowsInserted(Long.parseLong(operationMetrics.get("numTargetRowsInserted")));
      fgCommitMetadata.setRowsDeleted(Long.parseLong(operationMetrics.get("numTargetRowsDeleted")));
    }

    return fgCommitMetadata;
  }
}
