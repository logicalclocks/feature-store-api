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
package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.EntityEndpointType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class TestTagsApi {
  @Test
  public void testDoubleValueRead() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, 4.2d);
    Assert.assertTrue(obj instanceof Double);
  }

  @Test
  public void testDoubleValueWrite() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
//    objectMapper.writeValue(System.out, "test");
    System.out.println(objectMapper.writeValueAsString(4.2d));
  }

  @Test
  public void testIntegerValue() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, 4);
    Assert.assertTrue(obj instanceof Integer);
  }

  @Test
  public void testStringValue() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, "test");
    Assert.assertTrue(obj instanceof String);
  }

  @Test
  public void testMapValue() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, "{\"key\":\"value\"}");
    Assert.assertTrue(obj instanceof Map);
  }

  @Test
  public void testArrayValue() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]");
    Assert.assertTrue(obj.getClass().isArray());
  }

  @Test
  public void testInnerPrimitiveTypes() throws IOException {
    TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = tagsApi.parseTagValue(objectMapper, "{\"key1\":\"value\", \"key2\":4.2, \"key3\":4}");
    Assert.assertTrue(obj instanceof Map);
    Assert.assertTrue(((Map) obj).get("key1") instanceof String);
    Assert.assertTrue(((Map) obj).get("key2") instanceof Double);
    Assert.assertTrue(((Map) obj).get("key3") instanceof Integer);
  }
}
