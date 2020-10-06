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

package com.logicalclocks.hsfs.util;

public class Constants {

  // env vars
  public static final String PROJECTNAME_ENV = "hopsworks.projectname";

  public static final String FEATURESTORE_SUFFIX = "_featurestore";

  public static final String HIVE_FORMAT = "hive";
  public static final String JDBC_FORMAT = "jdbc";

  // Spark options
  public static final String DELIMITER = "delimiter";
  public static final String HEADER = "header";
  public static final String INFER_SCHEMA = "inferSchema";
  public static final String JDBC_USER = "user";
  public static final String JDBC_PWD = "password";
  public static final String JDBC_URL = "url";
  public static final String JDBC_TABLE = "dbtable";

  public static final String TF_CONNECTOR_RECORD_TYPE = "recordType";


  public static final String S3_SCHEME = "s3://";
  public static final String S3_SPARK_SCHEME = "s3a://";

  // hudi options
  public static final String HUDI_SPARK_FORMAT = "org.apache.hudi";
  public static final String HUDI_TABLE_NAME = "hoodie.table.name";
  public static final String HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type";
  public static final String HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation";
  public static final String HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field";
  public static final String HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field";
  public static final String HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field";
  public static final String HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable";
  public static final String HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table";
  public static final String HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database";
  public static final String HUDI_HIVE_SYNC_JDBC_URL = "hoodie.datasource.hive_sync.jdbcurl";
  public static final String HUDI_HIVE_SYNC_PARTITION_FIELDS = "hoodie.datasource.hive_sync.partition_fields";
  public static final String HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class";
  public static final String HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator";
  public static final String HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY =
          "hoodie.datasource.hive_sync.partition_extractor_class";
  public static final String DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
          "org.apache.hudi.hive.MultiPartKeysValueExtractor";
  public static final String HUDI_COPY_ON_WRITE = "COPY_ON_WRITE";
  public static final String HUDI_BULK_INSERT = "bulk_insert";
  public static final String HUDI_INSERT = "insert";
  public static final String HUDI_UPSERT = "upsert";
  public static final String HUDI_DELETE = "delete";
  public static final String HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type";
  public static final String HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot";
  public static final String HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental";
  public static final String HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime";
  public static final String HUDI_END_INSTANTTIME_OPT_KEY  = "hoodie.datasource.read.end.instanttime";
  public static final String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  public static final String PAYLOAD_CLASS_OPT_VAL =  "org.apache.hudi.common.model.EmptyHoodieRecordPayload";

}
