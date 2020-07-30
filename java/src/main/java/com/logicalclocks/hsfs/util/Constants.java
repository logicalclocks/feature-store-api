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
  public static final String S3_SCHEME_ROOT_REGEX = "s3:/";
}
