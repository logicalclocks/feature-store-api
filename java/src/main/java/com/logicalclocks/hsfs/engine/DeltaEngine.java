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
import io.delta.tables.DeltaMergeMatchedActionBuilder;
import io.delta.tables.DeltaMergeNotMatchedActionBuilder;
import io.delta.tables.DeltaTable;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DeltaEngine {

  private static final String DELTA_SPARK_FORMAT = "delta";
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss.SSS");

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaEngine.class);

  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();

  public void saveDeltaLakeFeatureGroup(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> dataset,
                                        SaveMode saveMode, ActionType operation,
                                        Map<String, String> writeOptions, Map<String, Object> deltaCustomExpressions)
      throws FeatureStoreException, IOException {

    if (deltaCustomExpressions == null) {
      deltaCustomExpressions = new HashMap<>();
    }

    if (operation == ActionType.BULK_INSERT) {
      dataset.write()
          .format(DELTA_SPARK_FORMAT)
          .partitionBy(utils.getPartitionColumns(featureGroup))
          // write options cannot be null
          .options(writeOptions == null ? new HashMap<>() : writeOptions)
          .mode(saveMode)
          .save("hdfs:///apps/hive/warehouse/feltafs_featurestore.db/" + utils.getTableName(featureGroup));

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

    } else if (operation == ActionType.INSERT) {
      dataset.write()
          .format(DELTA_SPARK_FORMAT)
          // write options cannot be null
          .options(writeOptions == null ? new HashMap<>() : writeOptions)
          .mode(SaveMode.Append)
          .save(featureGroup.getLocation());
    } else if (operation == ActionType.UPSERT) {
      DeltaLakeUpdate deltaLakeMetaData = new DeltaLakeUpdate();
      String newDataAlias = utils.getFgName(featureGroup) + "_update";
      deltaLakeMetaData.setExistingAlias(utils.getFgName(featureGroup));
      deltaLakeMetaData.setNewDataAlias(newDataAlias);

      DeltaLakeUpdate updatedDeltaLakeMetaData = featureGroupApi.constructDeltaLakeUpdate(featureGroup,
            deltaLakeMetaData);

      DeltaMergeBuilder deltaMergeBuilder = DeltaTable.forPath(sparkSession, featureGroup.getLocation())
            .as(utils.getFgName(featureGroup))
            .merge(
                dataset.as(newDataAlias),
                deltaCustomExpressions.containsKey("mergeExp")
                    ? deltaCustomExpressions.get("mergeExp").toString()
                    : updatedDeltaLakeMetaData.getMergeCondQuery());

      DeltaMergeMatchedActionBuilder deltaMergeMatchedActionBuilder;
      if (writeOptions.containsKey("whenMatchedExp")) {
        deltaMergeMatchedActionBuilder = deltaMergeBuilder.whenMatched(
            deltaCustomExpressions.get("whenMatchedExp").toString());
      } else {
        deltaMergeMatchedActionBuilder = deltaMergeBuilder.whenMatched();
      }

      if (deltaCustomExpressions.containsKey("updateExp")) {
        deltaMergeBuilder = deltaMergeMatchedActionBuilder.updateExpr(
            (Map<String, String>) deltaCustomExpressions.get("updateExp"));
      } else {
        deltaMergeBuilder = deltaMergeMatchedActionBuilder.updateAll();
      }

      DeltaMergeNotMatchedActionBuilder deltaMergeNotMatchedActionBuilder;
      if (writeOptions.containsKey("whenMatchedExp")) {
        deltaMergeNotMatchedActionBuilder = deltaMergeBuilder.whenNotMatched(
            deltaCustomExpressions.get("whenMatchedExp").toString());
      } else {
        deltaMergeNotMatchedActionBuilder = deltaMergeBuilder.whenNotMatched();
      }


      if (deltaCustomExpressions.containsKey("insertExp")) {
        deltaMergeBuilder = deltaMergeNotMatchedActionBuilder.insertExpr(
            (Map<String, String>) deltaCustomExpressions.get("insertExp"));
      } else {
        deltaMergeBuilder = deltaMergeNotMatchedActionBuilder.insertAll();
      }

      deltaMergeBuilder.execute();
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
        .format(DELTA_SPARK_FORMAT)
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

  @SneakyThrows
  public String timeStampToCommitFormat(Long commitedOnTimeStamp) {
    Date commitedOnDate = new Timestamp(commitedOnTimeStamp);
    return dateFormat.format(commitedOnDate);
  }
}
