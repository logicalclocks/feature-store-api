/*
 * Copyright (c) 2022 Hopsworks AB
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

import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class TestFeatureViewEngine {

  Query queryLabelInLeftFg;
  Query queryMultipleLabels;
  Query queryLabelInRightFgOnly;
  FeatureGroup fg1 = new FeatureGroup();
  FeatureGroup fg2 = new FeatureGroup();
  FeatureGroup fg3 = new FeatureGroup();
  String label = "label";

  @BeforeEach
  public void before() {
    setFeatureGroup(fg1, 1);
    setFeatureGroup(fg2, 2);
    setFeatureGroup(fg3, 3);
    queryLabelInLeftFg = fg1.selectAll().join(fg2.selectAll(), "fg2_");
    queryMultipleLabels =
        fg1.selectExcept(Lists.newArrayList(label)).join(fg2.selectAll(), "fg2_").join(fg3.selectAll(), "fg3_");
    queryLabelInRightFgOnly = fg1.selectExcept(Lists.newArrayList(label)).join(fg2.selectAll(), "fg2_");
  }

  private void setFeatureGroup(FeatureGroup fg, int id) {
    fg.setId(id);
    fg.setFeatures(Lists.newArrayList(new Feature("id", id), new Feature(label, id)));
  }

  @Test
  void makeLabelFeatures_queryLabelInLeftFg_leftLabel() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryLabelInLeftFg,
        Lists.newArrayList(label));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg1.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryLabelInLeftFg_rightLabel() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryLabelInLeftFg,
        Lists.newArrayList("fg2_label"));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg2.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryMultipleLabels_rightLabel1() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryMultipleLabels,
        Lists.newArrayList("fg2_label"));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg2.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryMultipleLabels_rightLabel2() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryMultipleLabels,
        Lists.newArrayList("fg3_label"));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg3.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryMultipleLabels_ambiguousLabel() throws Exception {
    Exception exception = Assertions.assertThrows(FeatureStoreException.class, () ->
        FeatureViewEngine.makeLabelFeatures(queryMultipleLabels, Lists.newArrayList(label)));
    String expectedMessage = String.format(FeatureViewEngine.AMBIGUOUS_LABEL_ERROR, label);
    String actualMessage = exception.getMessage();

    Assertions.assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  void makeLabelFeatures_queryLabelInRightFgOnly_rightLabel() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryLabelInRightFgOnly,
        Lists.newArrayList("fg2_label"));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg2.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryLabelInRightFgOnly_rightLabelWithoutPrefix() throws Exception {
    List<TrainingDatasetFeature> tdFeatures = FeatureViewEngine.makeLabelFeatures(queryLabelInRightFgOnly,
        Lists.newArrayList(label));
    Assertions.assertEquals(tdFeatures.size(), 1);
    TrainingDatasetFeature labelFeature = tdFeatures.get(0);
    Assertions.assertTrue(labelFeature.getLabel());
    Assertions.assertEquals(labelFeature.getFeaturegroup().getId(), fg2.getId());
    Assertions.assertEquals(labelFeature.getName(), label);
  }

  @Test
  void makeLabelFeatures_queryLabelInRightFgOnly_labelNotExist() throws Exception {
    Exception exception = Assertions.assertThrows(FeatureStoreException.class, () ->
        FeatureViewEngine.makeLabelFeatures(queryLabelInRightFgOnly, Lists.newArrayList("none")));
    String expectedMessage = String.format(FeatureViewEngine.LABEL_NOT_EXIST_ERROR, "none");
    String actualMessage = exception.getMessage();

    Assertions.assertTrue(actualMessage.contains(expectedMessage));
  }

}