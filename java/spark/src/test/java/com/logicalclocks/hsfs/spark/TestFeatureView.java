/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureGroupBaseForApi;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

class TestFeatureView {

  @Test
  void testParsingJson() throws JsonProcessingException {
    // Arrange
    Logger logger = Logger.getRootLogger();
    Appender appender = Mockito.mock(Appender.class);
    logger.addAppender(appender);

    ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);

    ObjectMapper objectMapper = new ObjectMapper();
    String json = "{\"name\":\"test_name\",\"query\":{\"leftFeatureGroup\":{\"name\":\"test_fg\",\"version\":1,\"deprecated\":false}}}";

    // Act
    FeatureView fv = objectMapper.readValue(json, FeatureView.class);

    // Assert
    Mockito.verify(appender, Mockito.times(0)).doAppend(argument.capture());
  }

  @Test
  void testParsingJsonWhenDeprecatedFeatureGroup() throws JsonProcessingException {
    // Arrange
    Logger logger = Logger.getRootLogger();
    Appender appender = Mockito.mock(Appender.class);
    logger.addAppender(appender);

    ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);

    ObjectMapper objectMapper = new ObjectMapper();
    String json = "{\"name\":\"test_name\",\"query\":{\"leftFeatureGroup\":{\"name\":\"test_fg\",\"version\":1,\"deprecated\":true}}}";

    // Act
    FeatureView fv = objectMapper.readValue(json, FeatureView.class);

    // Assert
    Mockito.verify(appender, Mockito.times(1)).doAppend(argument.capture());
    Assert.assertEquals(Level.WARN, argument.getValue().getLevel());
    Assert.assertEquals("Feature Group `test_fg`, version `1` is deprecated", argument.getValue().getMessage());
    Assert.assertEquals("com.logicalclocks.hsfs.FeatureGroupBase", argument.getValue().getLoggerName());
  }
}
