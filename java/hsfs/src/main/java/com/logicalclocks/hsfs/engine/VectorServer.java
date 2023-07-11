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

package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksExternalClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.metadata.Variable;
import com.logicalclocks.hsfs.metadata.VariablesApi;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@NoArgsConstructor
public class VectorServer {

  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Schema.Parser parser = new Schema.Parser();
  private BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private FeatureViewApi featureViewApi = new FeatureViewApi();

  private Connection preparedStatementConnection;
  private Map<Integer, TreeMap<String, Integer>> preparedStatementParameters;
  private TreeMap<Integer, PreparedStatement> preparedStatements;
  private TreeMap<Integer, String> preparedQueryString;
  @Getter
  private HashSet<String> servingKeys;
  private boolean isBatch = false;
  private VariablesApi variablesApi = new VariablesApi();

  public VectorServer(boolean isBatch) {
    this.isBatch = isBatch;
  }

  public List<Object> getFeatureVector(TrainingDatasetBase trainingDatasetBase, Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return getFeatureVector(trainingDatasetBase, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<Object> getFeatureVector(TrainingDatasetBase trainingDatasetBase,
                                       Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    // init prepared statement if it has not been already set. Or if a prepare statement for serving
    // single vector is needed.
    if (preparedStatements == null || isBatch) {
      initPreparedStatement(trainingDatasetBase, false, external);
    }
    return getFeatureVector(trainingDatasetBase.getFeatureStore(),
        trainingDatasetBase.getFeatures(), entry, external);
  }

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry)
      throws FeatureStoreException, SQLException, IOException, ClassNotFoundException {
    return getFeatureVector(featureViewBase, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    if (preparedStatements == null || isBatch) {
      initPreparedStatement(featureViewBase, false, external);
    }
    return getFeatureVector(featureViewBase.getFeatureStore(), featureViewBase.getFeatures(), entry, external);
  }

  private List<Object> getFeatureVector(FeatureStoreBase featureStoreBase, List<TrainingDatasetFeature> features,
                                        Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    refreshJdbcConnection(featureStoreBase, external);
    // Iterate over entry map of preparedStatements and set values to them
    for (Integer fgId : preparedStatements.keySet()) {
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(fgId);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatements.get(fgId).setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }
    }
    return getFeatureVector(features);
  }

  private List<Object> getFeatureVector(List<TrainingDatasetFeature> features)
      throws SQLException, FeatureStoreException, IOException {
    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(features);
    // construct serving vector
    ArrayList<Object> servingVector = new ArrayList<>();
    for (Integer preparedStatementIndex : preparedStatements.keySet()) {
      ResultSet results = preparedStatements.get(preparedStatementIndex).executeQuery();
      // check if results contain any data at all and throw exception if not
      if (!results.isBeforeFirst()) {
        throw new FeatureStoreException("No data was retrieved from online feature store.");
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
    return servingVector;
  }

  public List<List<Object>> getFeatureVectors(TrainingDatasetBase trainingDatasetBase, Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {

    return getFeatureVectors(trainingDatasetBase, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<List<Object>> getFeatureVectors(TrainingDatasetBase trainingDatasetBase, Map<String, List<Object>> entry,
                                              boolean external) throws SQLException, FeatureStoreException, IOException,
      ClassNotFoundException {
    // init prepared statement if it has not already
    if (preparedStatements == null || !isBatch) {
      // size of batch of primary keys are required to be equal. Thus, we take size of batch for the 1st primary key if
      // it was not initialized from initPreparedStatement(batchSize)
      initPreparedStatement(trainingDatasetBase, true, external);
    }
    return getFeatureVectors(trainingDatasetBase.getFeatureStore(), trainingDatasetBase.getFeatures(),
        entry, external);
  }

  public List<List<Object>> getFeatureVectors(FeatureViewBase featureViewBase, Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return getFeatureVectors(featureViewBase.getFeatureStore(), featureViewBase.getFeatures(), entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<List<Object>> getFeatureVectors(FeatureViewBase featureViewBase, Map<String, List<Object>> entry,
                                              boolean external) throws SQLException, FeatureStoreException, IOException,
      ClassNotFoundException {
    if (preparedStatements == null || !isBatch) {
      initPreparedStatement(featureViewBase, true, external);
    }
    return getFeatureVectors(featureViewBase.getFeatureStore(), featureViewBase.getFeatures(), entry, external);
  }

  private List<List<Object>> getFeatureVectors(FeatureStoreBase featureStoreBase, List<TrainingDatasetFeature> features,
                                               Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException {
    return getFeatureVectors(featureStoreBase, features, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  private List<List<Object>> getFeatureVectors(FeatureStoreBase featureStoreBase, List<TrainingDatasetFeature> features,
                                               Map<String, List<Object>> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    List<String> queries = Lists.newArrayList();
    for (Integer fgId : preparedQueryString.keySet()) {
      String query = preparedQueryString.get(fgId);
      String zippedTupleString =
          zipArraysToTupleString(preparedStatementParameters.get(fgId).keySet().stream().map(entry::get)
              .collect(Collectors.toList()));
      queries.add(query.replaceFirst("\\?", zippedTupleString));
    }
    return getFeatureVectors(featureStoreBase, features, queries, external);
  }

  private List<List<Object>> getFeatureVectors(FeatureStoreBase featureStoreBase, List<TrainingDatasetFeature> features,
                                               List<String> queries, boolean external)
      throws SQLException, FeatureStoreException, IOException {
    refreshJdbcConnection(featureStoreBase, external);
    ArrayList<Object> servingVector = new ArrayList<>();

    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(features);

    // construct batch of serving vectors
    // Create map object that will have of order of the vector as key and values as
    // vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
    // expect that backend will return correctly ordered vectors.
    Map<Integer, List<Object>> servingVectorsMap = new HashMap<>();

    try (Statement stmt = preparedStatementConnection.createStatement()) {
      for (String query : queries) {
        int orderInBatch = 0;

        // MySQL doesn't support setting array type on prepared statement. This is the hack to replace
        // the ? with array joined as comma separated array.
        try (ResultSet results = stmt.executeQuery(query)) {

          // check if results contain any data at all and throw exception if not
          if (!results.isBeforeFirst()) {
            throw new FeatureStoreException("No data was retrieved from online feature store.");
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
        }
      }
    }
    return new ArrayList<List<Object>>(servingVectorsMap.values());
  }

  public void initServing(TrainingDatasetBase trainingDatasetBase, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(trainingDatasetBase, batch);
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch);
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch, external);
  }

  public void initPreparedStatement(TrainingDatasetBase trainingDatasetBase, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(trainingDatasetBase, batch,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public void initPreparedStatement(TrainingDatasetBase trainingDatasetBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    // check if this training dataset has transformation functions attached and throw exception if any
    if (trainingDatasetApi.getTransformationFunctions(trainingDatasetBase).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }
    List<ServingPreparedStatement> servingPreparedStatements =
        trainingDatasetApi.getServingPreparedStatement(trainingDatasetBase, batch);
    initPreparedStatement(trainingDatasetBase.getFeatureStore(), servingPreparedStatements, batch, external);
  }

  public void initPreparedStatement(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch, HopsworksClient.getInstance().getHopsworksHttpClient()
        instanceof HopsworksExternalClient);
  }

  public void initPreparedStatement(FeatureViewBase featureViewBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    // check if this training dataset has transformation functions attached and throw exception if any
    if (featureViewApi.getTransformationFunctions(featureViewBase).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }
    List<ServingPreparedStatement> servingPreparedStatements =
        featureViewApi.getServingPreparedStatement(featureViewBase, batch);
    initPreparedStatement(featureViewBase.getFeatureStore(), servingPreparedStatements, batch, external);
  }

  private void initPreparedStatement(FeatureStoreBase featureStoreBase,
                                     List<ServingPreparedStatement> servingPreparedStatements, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureStoreBase, servingPreparedStatements, batch,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  private void initPreparedStatement(FeatureStoreBase featureStoreBase,
                                     List<ServingPreparedStatement> servingPreparedStatements, boolean batch,
                                     boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");

    this.isBatch = batch;
    setupJdbcConnection(featureStoreBase, external);
    // map of prepared statement index and its corresponding parameter indices
    Map<Integer, TreeMap<String, Integer>> preparedStatementParameters = new HashMap<>();
    // save map of fg index and its prepared statement
    TreeMap<Integer, PreparedStatement> preparedStatements = new TreeMap<>();

    // in case its batch serving then we need to save sql string only
    TreeMap<Integer, String> preparedQueryString = new TreeMap<>();

    // save unique primary key names that will be used by user to retrieve serving vector
    HashSet<String> servingVectorKeys = new HashSet<>();
    for (ServingPreparedStatement servingPreparedStatement : servingPreparedStatements) {
      if (batch) {
        preparedQueryString.put(servingPreparedStatement.getPreparedStatementIndex(),
            servingPreparedStatement.getQueryOnline());
      } else {
        preparedStatements.put(servingPreparedStatement.getPreparedStatementIndex(),
            preparedStatementConnection
                .prepareStatement(servingPreparedStatement.getQueryOnline()));
      }
      TreeMap<String, Integer> parameterIndices = new TreeMap<>();
      servingPreparedStatement.getPreparedStatementParameters().forEach(preparedStatementParameter -> {
        servingVectorKeys.add(preparedStatementParameter.getName());
        parameterIndices.put(preparedStatementParameter.getName(), preparedStatementParameter.getIndex());
      });
      preparedStatementParameters.put(servingPreparedStatement.getPreparedStatementIndex(), parameterIndices);
    }
    this.servingKeys = servingVectorKeys;
    this.preparedStatementParameters = preparedStatementParameters;
    this.preparedStatements = preparedStatements;
    this.preparedQueryString = preparedQueryString;
  }

  private void setupJdbcConnection(FeatureStoreBase featureStoreBase, Boolean external) throws FeatureStoreException,
      IOException, SQLException {

    StorageConnector.JdbcConnector storageConnectorBase =
        storageConnectorApi.getOnlineStorageConnector(featureStoreBase, StorageConnector.JdbcConnector.class);
    Map<String, String> jdbcOptions = storageConnectorBase.sparkOptions();
    String url = jdbcOptions.get(Constants.JDBC_URL);
    if (external) {
      // if external is true, replace the IP coming from the storage connector with the host
      // used during the connection setup
      String host;
      Optional<Variable> loadbalancerVariable = variablesApi.get(VariablesApi.LOADBALANCER_EXTERNAL_DOMAIN);
      if (loadbalancerVariable.isPresent() && !Strings.isNullOrEmpty(loadbalancerVariable.get().getValue())) {
        host = loadbalancerVariable.get().getValue();
      } else {
        // Fall back to the mysql server on the head node
        host = HopsworksClient.getInstance().getHost();
      }

      url = url.replaceAll("/[0-9.]+:", "/" + host + ":");
    }
    preparedStatementConnection =
        DriverManager.getConnection(url, jdbcOptions.get(Constants.JDBC_USER), jdbcOptions.get(Constants.JDBC_PWD));
  }

  private String zipArraysToTupleString(List<List<Object>> lists) {
    List<String> zippedTuples = new ArrayList<>();
    for (int i = 0; i < lists.get(0).size(); i++) {
      List<String> zippedArray = new ArrayList<String>();
      for (List<Object> in : lists) {
        zippedArray.add(in.get(i).toString());
      }
      zippedTuples.add("(" + String.join(",", zippedArray) + ")");
    }
    return "(" + String.join(",", zippedTuples) + ")";
  }

  private void refreshJdbcConnection(FeatureStoreBase featureStoreBase, Boolean external) throws FeatureStoreException,
      IOException, SQLException {
    if (!preparedStatementConnection.isValid(1)) {
      setupJdbcConnection(featureStoreBase, external);
    }
  }

  private Object deserializeComplexFeature(Map<String, DatumReader<Object>> complexFeatureSchemas, ResultSet results,
                                           int index) throws SQLException, IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(results.getBytes(index), binaryDecoder);
    return complexFeatureSchemas.get(results.getMetaData().getColumnName(index)).read(null, decoder);
  }

  private Map<String, DatumReader<Object>> getComplexFeatureSchemas(List<TrainingDatasetFeature> features)
      throws FeatureStoreException, IOException {
    Map<String, DatumReader<Object>> featureSchemaMap = new HashMap<>();
    for (TrainingDatasetFeature f : features) {
      if (f.isComplex()) {
        DatumReader<Object> datumReader =
            new GenericDatumReader<>(parser.parse(f.getFeatureGroup().getFeatureAvroSchema(f.getName())));
        featureSchemaMap.put(f.getName(), datumReader);
      }
    }
    return featureSchemaMap;
  }

  private void checkPrimaryKeys(Set<String> primaryKeys) {
    //check if primary key map correspond to serving_keys.
    if (!servingKeys.equals(primaryKeys)) {
      throw new IllegalArgumentException("Provided primary key map doesn't correspond to serving_keys");
    }
  }
}
