/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.engine.VectorServer;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public abstract class FeatureViewBase<T extends FeatureViewBase, T3 extends FeatureStoreBase<T4>,
    T4 extends QueryBase, T5> {

  @Getter
  @Setter
  @JsonIgnore
  protected T3 featureStore;

  @Getter
  @Setter
  @JsonIgnore
  protected Integer id;

  @Getter
  @Setter
  protected String name;

  @Getter
  @Setter
  protected Integer version;

  @Getter
  @Setter
  protected String description;

  @Getter
  @Setter
  protected List<TrainingDatasetFeature> features;

  @Getter
  @Setter
  protected T4 query;

  @Getter
  @Setter
  @JsonIgnore
  protected List<String> labels;

  @Getter
  @Setter
  protected String type = "featureViewDTO";

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewBase.class);

  protected VectorServer vectorServer = new VectorServer();
  protected Integer extraFilterVersion = null;

  /**
   * Initialise feature view to retrieve feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Initialise feature view serving
   *        fv.initServing();
   * }
   * </pre>
   *
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  public void initServing() throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, false);
  }

  /**
   * Initialise feature view to retrieve feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Initialise feature view batch serving
   *        fv.initServing(true, false);
   * }
   * </pre>
   *
   * @param batch Whether to initialise feature view to retrieve feature vector from offline feature store.
   * @param external If set to `true`, the connection to the online feature store is established using the same host as
   *                 for the `host` parameter in the connection object.
   *                 If set to False, the online feature store storage connector is used which relies on the private IP.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @Deprecated
  public void initServing(Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, batch, external);
  }

  protected void validateTrainTestSplit(Float testSize, String trainEnd, String testStart)
      throws FeatureStoreException {
    if (!((testSize != null && testSize > 0 && testSize < 1)
        || (!Strings.isNullOrEmpty(trainEnd) || !Strings.isNullOrEmpty(testStart)))) {
      throw new FeatureStoreException(
          "Invalid split input."
              + "You should specify either `testSize` or (`trainEnd` or `testStart`)."
              + " `testSize` should be between 0 and 1 if specified."
      );
    }
  }

  protected void validateTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainEnd, String validationStart, String validationEnd,
      String testStart)
      throws FeatureStoreException {
    if (!((validationSize != null && validationSize > 0 && validationSize < 1
        && testSize != null && testSize > 0 && testSize < 1
        && validationSize + testSize < 1)
        || ((!Strings.isNullOrEmpty(trainEnd) || !Strings.isNullOrEmpty(validationStart))
        && (!Strings.isNullOrEmpty(validationEnd) || !Strings.isNullOrEmpty(testStart))))) {
      throw new FeatureStoreException(
          "Invalid split input."
              + " You should specify either (`validationSize` and `testSize`) or "
              + "((`trainEnd` or `validationStart`) and (`validationEnd` "
              + "or `testStart`))."
              + "`validationSize`, `testSize` and sum of `validationSize` and `testSize` should be between 0 and 1 "
              + "if specified."
      );
    }
  }

  /**
   * Initialise feature view to retrieve feature vector from offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Initialise feature view batch scoring
   *        fv.initBatchScoring(1);
   * }
   * </pre>
   *
   * @param trainingDatasetVersion Version of training dataset to identify additional filters attached to the training
   *                               dataset and statistics to use for transformation functions.
   */
  public void initBatchScoring(Integer trainingDatasetVersion) {
    this.extraFilterVersion = trainingDatasetVersion;
  }

  /**
   * Returns assembled feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define primary key values to fetch data from online feature store
   *        Map<String, Object> pkMap = new HashMap<String, Object>() {
   *               {put("customer_id", 1);
   *                put("contract_id" , 100);
   *                }
   *        };
   *        // get feature vector
   *        fv.getFeatureVector(entry);
   * }
   * </pre>
   *
   * @param entry Fictionary of feature group primary key and values provided by serving application.
   * @return List of feature values related to provided primary keys, ordered according to positions of the features
   *         in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry);
  }

  /**
   * Returns assembled feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define primary key values to fetch data from online feature store
   *        Map<String, Object> pkMap = new HashMap<String, Object>() {
   *               {put("customer_id", 1);
   *                put("contract_id" , 100);
   *                }
   *        };
   *        // get feature vector
   *        fv.getFeatureVector(entry, false);
   * }
   * </pre>
   *
   * @param entry Dictionary of feature group primary key and values provided by serving application.
   * @param external If set to true, the connection to the online feature store is established using the same host as
   *                 for the `host` parameter in the connection object.
   *                 If set to false, the online feature store storage connector is used which relies on the private IP.
   *                 Defaults to True if connection to Hopsworks is established from external environment
   * @return List of feature values related to provided primary keys, ordered according to positions of the features
   *         in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, external);
  }

  /**
   * Returns assembled feature vectors in batches from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define primary key values to fetch data from online feature store
   *        Map<String, List<Long>> entry = ...;
   *        // get feature vector
   *        fv.getFeatureVector(entry);
   * }
   * </pre>
   *
   * @param entry A list of dictionaries of feature group primary key and values provided by serving application.
   * @return List of lists of feature values related to provided primary keys, ordered according to
   *         positions of the features in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry);
  }

  /**
   *Returns assembled feature vectors in batches from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define primary key values to fetch data from online feature store
   *        Map<String, List<Long>> entry = ...;
   *        // get feature vector
   *        fv.getFeatureVectors(entry, false);
   * }
   * </pre>
   *
   * @param entry A list of dictionaries of feature group primary key and values provided by serving application.
   * @param external If set to `true`, the connection to the  online feature store is established using the same host as
   *                 for the `host` parameter in the connection object.
   *                 If set to False, the online feature store storage connector is used which relies on the private IP.
   * @return List of lists of feature values related to provided primary keys, ordered according to
   *         positions of this features in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry, external);
  }

  /**
   * Add name/value tag to the feature view.
   *
   * @param name
   *     name of the tag
   * @param value
   *     value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void addTag(String name, Object value) throws FeatureStoreException, IOException;

  /**
   * Get all tags of the feature view.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public abstract Map<String, Object> getTags() throws FeatureStoreException, IOException;

  /**
   * Get a single tag value of the feature view.
   *
   * @param name
   *     name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public abstract Object getTag(String name) throws FeatureStoreException, IOException;

  /**
   * Delete a tag of the feature view.
   *
   * @param name
   *     name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void deleteTag(String name) throws FeatureStoreException, IOException;

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name
   *     name of the tag
   * @param value
   *     value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void addTrainingDatasetTag(Integer version, String name, Object value) throws FeatureStoreException,
      IOException;

  /**
   * Get all tags of the training dataset.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public abstract Map<String, Object> getTrainingDatasetTags(Integer version) throws FeatureStoreException, IOException;

  /**
   * Get a single tag value of the training dataset.
   *
   * @param name
   *     name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public abstract Object getTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException;

  /**
   * Delete a tag of the training dataset.
   *
   * @param name
   *     name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void deleteTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException;

  /**
   * Set of primary key names that is used as keys in input dict object for `get_serving_vector` method.
   *
   * @return Set of serving keys
   * @throws SQLException
   * @throws IOException
   * @throws FeatureStoreException
   * @throws ClassNotFoundException
   */
  @JsonIgnore
  public HashSet<String> getPrimaryKeys()
      throws SQLException, IOException, FeatureStoreException, ClassNotFoundException {
    if (vectorServer.getServingKeys().isEmpty()) {
      initServing();
    }
    return vectorServer.getServingKeys();
  }

  public abstract void delete() throws FeatureStoreException, IOException;

  public abstract void clean(T3 featureStore, String featureViewName, Integer featureViewVersion)
      throws FeatureStoreException, IOException;

  public abstract T update(T other) throws FeatureStoreException, IOException;

  public abstract String getBatchQuery() throws FeatureStoreException, IOException, ParseException;

  public abstract String getBatchQuery(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException;

  public abstract T5 getBatchData() throws FeatureStoreException, IOException, ParseException;

  public abstract T5 getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException;

  public abstract T5 getBatchData(String startTime, String endTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException;

  public abstract Object getTrainingData(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object getTrainTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object getTrainValidationTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract void purgeTrainingData(Integer version) throws FeatureStoreException, IOException;

  public abstract void purgeAllTrainingData() throws FeatureStoreException, IOException;

  public abstract void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException;

  public abstract void deleteAllTrainingDatasets() throws FeatureStoreException, IOException;
}
