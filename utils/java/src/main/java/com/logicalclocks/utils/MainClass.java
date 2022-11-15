/*
 * Copyright (c) 2022 Logical Clocks AB
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

package com.logicalclocks.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.SparkEngine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MainClass {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

  private static Map<String, Object> readJobConf(String path) throws Exception {
    Configuration conf = new Configuration();
    Path filepath = new Path(path);
    FileSystem fileSystem = FileSystem.get(filepath.toUri(), conf);
    InputStream jobConfigInStream = fileSystem.open(filepath);

    Map<String, Object> jobConfig =
        new ObjectMapper().readValue(jobConfigInStream, HashMap.class);
    jobConfigInStream.close();

    return jobConfig;
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options();

    options.addOption(Option.builder("op")
        .argName("op")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("path")
        .argName("path")
        .required(true)
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    String op = commandLine.getOptionValue("op");
    String path = commandLine.getOptionValue("path");

    // read jobs config
    Map<String, Object> jobConf = readJobConf(path);

    // get feature store handle
    HopsworksConnection connection = HopsworksConnection.builder().build();
    FeatureStore fs = connection.getFeatureStore((String) jobConf.get("feature_store"));

    // get feature group handle
    StreamFeatureGroup streamFeatureGroup = fs.getStreamFeatureGroup((String) jobConf.get("name"),
        Integer.parseInt((String)
        jobConf.get("version")));

    Map<String, String> writeOptions = (Map<String, String>) jobConf.get("write_options");

    if (op.equals("offline_fg_backfill")) {
      SparkEngine.getInstance().streamToHudiTable(streamFeatureGroup, writeOptions);
    }
  }
}
