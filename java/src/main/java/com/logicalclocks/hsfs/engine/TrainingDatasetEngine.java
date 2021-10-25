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

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.sql.DriverManager;
import java.util.TreeMap;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.TRAINING_DATASET);
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Utils utils = new Utils();
  private Schema.Parser parser = new Schema.Parser();
  private BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetEngine.class);

  /**
   * Make a REST call to Hopsworks to create the metadata and write the data on the File System.
   *
   * @param trainingDataset
   * @param dataset
   * @param userWriteOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset save(TrainingDataset trainingDataset, Dataset<Row> dataset,
                              Map<String, String> userWriteOptions, List<String> label)
      throws FeatureStoreException, IOException {

    trainingDataset.setFeatures(utils.parseTrainingDatasetSchema(dataset));

    // set label features
    if (label != null && !label.isEmpty()) {
      for (String l : label) {
        Optional<TrainingDatasetFeature> feature =
            trainingDataset.getFeatures().stream().filter(f -> f.getName().equals(l)).findFirst();
        if (feature.isPresent()) {
          feature.get().setLabel(true);
        } else {
          throw new FeatureStoreException("The specified label `" + l + "` could not be found among the features: "
              + trainingDataset.getFeatures().stream().map(TrainingDatasetFeature::getName) + ".");
        }
      }
    }

    // Make the rest call to create the training dataset metadata
    TrainingDataset apiTD = trainingDatasetApi.createTrainingDataset(trainingDataset);

    if (trainingDataset.getVersion() == null) {
      LOGGER.info("VersionWarning: No version provided for creating training dataset `" + trainingDataset.getName()
          + "`, incremented version to `" + apiTD.getVersion() + "`.");
    }

    // Update the original object - Hopsworks returns the full location and incremented version
    trainingDataset.setLocation(apiTD.getLocation());
    trainingDataset.setVersion(apiTD.getVersion());
    trainingDataset.setId(apiTD.getId());
    trainingDataset.setStorageConnector(apiTD.getStorageConnector());

    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, SaveMode.Overwrite);

    return trainingDataset;
  }

  /**
   * Insert (append or overwrite) data on a training dataset.
   *
   * @param trainingDataset
   * @param dataset
   * @param providedOptions
   * @param saveMode
   * @throws FeatureStoreException
   */
  public void insert(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> providedOptions, SaveMode saveMode)
      throws FeatureStoreException, IOException {
    // validate that the schema matches
    utils.trainingDatasetSchemaMatch(dataset, trainingDataset.getFeatures());

    // check if this training dataset has transformation functions attached and throw exception if any
    if (trainingDatasetApi.getTransformationFunctions(trainingDataset).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "insert operation must be performed from a PySpark application");
    }

    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(providedOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, saveMode);
  }

  public Dataset<Row> read(TrainingDataset trainingDataset, String split, Map<String, String> providedOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> readOptions =
        SparkEngine.getInstance().getReadOptions(providedOptions, trainingDataset.getDataFormat());

    String path = trainingDataset.getLocation();
    if (!com.google.common.base.Strings.isNullOrEmpty(split)) {
      path = new Path(trainingDataset.getLocation(), split).toString();
    }
    return trainingDataset.getStorageConnector()
        .read(null, trainingDataset.getDataFormat().toString(), readOptions, path);
  }

  public void addTag(TrainingDataset trainingDataset, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(trainingDataset, name, value);
  }

  public Map<String, Object> getTags(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset);
  }

  public Object getTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset, name);
  }

  public void deleteTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(trainingDataset, name);
  }

  public String getQuery(TrainingDataset trainingDataset, Storage storage, boolean withLabel)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.getQuery(trainingDataset, withLabel).getStorageQuery(storage);
  }

  public void updateStatisticsConfig(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    TrainingDataset apiTD = trainingDatasetApi.updateMetadata(trainingDataset, "updateStatsConfig");
    trainingDataset.getStatisticsConfig().setCorrelations(apiTD.getStatisticsConfig().getCorrelations());
    trainingDataset.getStatisticsConfig().setHistograms(apiTD.getStatisticsConfig().getHistograms());
    trainingDataset.getStatisticsConfig().setExactUniqueness(apiTD.getStatisticsConfig().getExactUniqueness());
  }

  public void initPreparedStatement(TrainingDataset trainingDataset, Integer batchSize, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");

    boolean batch;
    if (batchSize != null && batchSize > 1) {
      batch = true;
      trainingDataset.setServingBatchSize(batchSize);
    } else {
      batch = false;
    }

    // check if this training dataset has transformation functions attached and throw exception if any
    if (trainingDatasetApi.getTransformationFunctions(trainingDataset).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }

    StorageConnector storageConnector =
        storageConnectorApi.getOnlineStorageConnector(trainingDataset.getFeatureStore());
    Map<String, String> jdbcOptions = storageConnector.sparkOptions();
    String url = jdbcOptions.get(Constants.JDBC_URL);
    if (external) {
      // if external is true, replace the IP coming from the storage connector with the host
      // used during the connection setup
      url = url.replaceAll("/[0-9.]+:", "/" + HopsworksClient.getInstance().getHost() + ":");
    }
    Connection jdbcConnection =
        DriverManager.getConnection(url, jdbcOptions.get(Constants.JDBC_USER), jdbcOptions.get(Constants.JDBC_PWD));
    jdbcConnection.setAutoCommit(false);
    trainingDataset.setPreparedStatementConnection(jdbcConnection);

    List<ServingPreparedStatement> servingPreparedStatements =
        trainingDatasetApi.getServingPreparedStatement(trainingDataset, batch);
    // map of prepared statement index and its corresponding parameter indices
    Map<Integer, Map<String, Integer>> preparedStatementParameters = new HashMap<>();
    // save map of fg index and its prepared statement
    TreeMap<Integer, PreparedStatement> preparedStatements = new TreeMap<>();
    // save unique primary key names that will be used by user to retrieve serving vector
    HashSet<String> servingVectorKeys = new HashSet<>();
    for (ServingPreparedStatement servingPreparedStatement: servingPreparedStatements) {
      preparedStatements.put(servingPreparedStatement.getPreparedStatementIndex(),
          jdbcConnection.prepareStatement(prepareParametersIfBatch(servingPreparedStatement.getQueryOnline(), batch,
              batchSize)));
      HashMap<String, Integer> parameterIndices = new HashMap<>();
      servingPreparedStatement.getPreparedStatementParameters().forEach(preparedStatementParameter -> {
        servingVectorKeys.add(preparedStatementParameter.getName());
        parameterIndices.put(preparedStatementParameter.getName(), preparedStatementParameter.getIndex());
      });
      preparedStatementParameters.put(servingPreparedStatement.getPreparedStatementIndex(), parameterIndices);
    }
    trainingDataset.setServingKeys(servingVectorKeys);
    trainingDataset.setPreparedStatementParameters(preparedStatementParameters);
    trainingDataset.setPreparedStatements(preparedStatements);
  }

  public List<Object> getServingVector(TrainingDataset trainingDataset, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {

    // init prepared statement if it has not already
    if (trainingDataset.getPreparedStatements() == null) {
      initPreparedStatement(trainingDataset, null, external);
    }
    //check if primary key map correspond to serving_keys.
    if (!trainingDataset.getServingKeys().equals(entry.keySet())) {
      throw new IllegalArgumentException("Provided primary key map doesn't correspond to serving_keys");
    }

    Map<Integer, Map<String, Integer>> preparedStatementParameters = trainingDataset.getPreparedStatementParameters();
    TreeMap<Integer, PreparedStatement> preparedStatements = trainingDataset.getPreparedStatements();
    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(trainingDataset);

    // Iterate over entry map of preparedStatements and set values to them
    for (Integer fgId : preparedStatements.keySet()) {
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(fgId);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatements.get(fgId).setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }
    }

    // construct serving vector
    ArrayList<Object> servingVector = new ArrayList<>();
    for (Integer preparedStatementIndex : preparedStatements.keySet()) {
      ResultSet results = preparedStatements.get(preparedStatementIndex).executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store using input " + entry);
      }
      //Get column count
      int columnCount = results.getMetaData().getColumnCount();
      //append results to servingVector
      while (results.next()) {
        int index = 1;
        while (index <= columnCount) {
          if (complexFeatureSchemas.containsKey(results.getMetaData().getColumnName(index))) {
            servingVector.add(deserializeComplexFeature(complexFeatureSchemas, results, index));
          } else {
            servingVector.add(results.getObject(index));
          }
          index++;
        }
      }
      results.close();
    }
    trainingDataset.getPreparedStatementConnection().commit();
    return servingVector;
  }

  public List<List<Object>> getServingVectors(TrainingDataset trainingDataset, Map<String, List<Object>> entry,
                                        boolean external) throws SQLException, FeatureStoreException, IOException,
          ClassNotFoundException {

    // init prepared statement if it has not already
    if (trainingDataset.getPreparedStatements() == null) {
      // size of batch of primary keys are required to be equal. Thus we take size of batch for the 1st primary key if
      // it was not initialized from initPreparedStatement(batchSize)
      initPreparedStatement(trainingDataset, entry.get(0).size(), external);
    }

    //check if primary key map correspond to serving_keys.
    if (!trainingDataset.getServingKeys().equals(entry.keySet())) {
      throw new IllegalArgumentException("Provided primary key map doesn't correspond to serving_keys");
    }

    Map<Integer, Map<String, Integer>> preparedStatementParameters = trainingDataset.getPreparedStatementParameters();
    TreeMap<Integer, PreparedStatement> preparedStatements = trainingDataset.getPreparedStatements();
    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(trainingDataset);

    // Iterate over entry map of preparedStatements and set values to them
    for (Integer fgId : trainingDataset.getPreparedStatements().keySet()) {
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(fgId);
      for (Object name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          //check if size of batch of keys corresponds to one that was used during initialisation
          if (entry.get(name).size() != trainingDataset.getServingBatchSize()) {
            throw new IllegalArgumentException("Size of provided batch of primary keys doesn't correspond to "
                + "size used during initialisation");
          }
          // As MySQL doesn't support setting array type on prepared statement we will iterate over batch and set
          // values individually. each values parameter position will be fg index + position index in batch array
          for (int i = 0; i < entry.get(name).size(); i++) {
            preparedStatements.get(fgId).setObject(parameterIndexInStatement.get(name) + i,
                entry.get(name).get(i));
          }
        }
      }
    }

    // construct batch of serving vectors

    // Create map object that will have of order of the vector as key and values as
    // vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
    // expect that backend will return correctly ordered vectors.
    Map<Integer, List<Object>> servingVectorsMap = new HashMap<>();

    for (Integer preparedStatementIndex : preparedStatements.keySet()) {
      int orderInBatch = 0;
      ArrayList<Object> servingVector = new ArrayList<>();
      ResultSet results = preparedStatements.get(preparedStatementIndex).executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store using input " + entry);
      }
      //Get column count
      int columnCount = results.getMetaData().getColumnCount();
      //append results to servingVector
      while (results.next()) {
        int index = 1;
        while (index <= columnCount) {
          if (complexFeatureSchemas.containsKey(results.getMetaData().getColumnName(index))) {
            servingVector.add(deserializeComplexFeature(complexFeatureSchemas, results, index));
          } else {
            servingVector.add(results.getObject(index));
          }
          index++;
        }
        // get vector by order and update with vector from other feature group(s)
        if (servingVectorsMap.containsKey(orderInBatch)) {
          servingVectorsMap.get(orderInBatch).addAll(servingVector);
        } else {
          servingVectorsMap.put(orderInBatch, servingVector);
        }
        // empty servingVector for new primary key
        servingVector = new ArrayList<>();
        orderInBatch++;
      }
      results.close();
    }
    trainingDataset.getPreparedStatementConnection().commit();
    return new ArrayList<List<Object>>(servingVectorsMap.values());
  }

  private Object deserializeComplexFeature(Map<String, DatumReader<Object>> complexFeatureSchemas, ResultSet results,
      int index) throws SQLException, IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(results.getBytes(index), binaryDecoder);
    return complexFeatureSchemas.get(results.getMetaData().getColumnName(index)).read(null, decoder);
  }

  private Map<String, DatumReader<Object>> getComplexFeatureSchemas(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    Map<String, DatumReader<Object>> featureSchemaMap = new HashMap<>();
    for (TrainingDatasetFeature f : trainingDataset.getFeatures()) {
      if (f.isComplex()) {
        DatumReader<Object> datumReader =
            new GenericDatumReader<>(parser.parse(f.getFeaturegroup().getFeatureAvroSchema(f.getName())));
        featureSchemaMap.put(f.getName(), datumReader);
      }
    }
    return featureSchemaMap;
  }

  public void delete(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    trainingDatasetApi.delete(trainingDataset);
  }

  private String prepareParametersIfBatch(String query, boolean batch, Integer batchSize) {
    // MySQL doesn't support setting array type on prepared statement. This is the hack to replace
    // replace the ? with ? times batchSize.
    if (batch) {
      String inParameters = "(" + String.join(", ", Collections.nCopies(batchSize, "?")) + ")";
      return query.replaceAll("\\?", inParameters);
    } else {
      return query;
    }
  }
}
