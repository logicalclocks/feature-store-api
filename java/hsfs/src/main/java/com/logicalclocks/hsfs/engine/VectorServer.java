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
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.metadata.Variable;
import com.logicalclocks.hsfs.metadata.VariablesApi;
import com.logicalclocks.hsfs.util.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@NoArgsConstructor
public class VectorServer {

  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Schema.Parser parser = new Schema.Parser();
  private FeatureViewApi featureViewApi = new FeatureViewApi();

  private HikariDataSource hikariDataSource = null;
  private Map<Integer, TreeMap<String, Integer>> preparedStatementParameters;
  private TreeMap<Integer, String> preparedQueryString;
  private Map<String, DatumReader<Object>> datumReadersComplexFeatures;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  @Getter
  private HashSet<String> servingKeys;
  private boolean isBatch = false;
  private VariablesApi variablesApi = new VariablesApi();

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorServer.class);

  public VectorServer(boolean isBatch) {
    this.isBatch = isBatch;
  }

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry)
      throws FeatureStoreException, SQLException, IOException, ClassNotFoundException {
    return getFeatureVector(featureViewBase, entry,
        HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient);
  }

  public List<Object> getFeatureVector(FeatureViewBase featureViewBase, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    if (hikariDataSource == null || isBatch) {
      initPreparedStatement(featureViewBase, false, external);
    }
    return getFeatureVector(entry);
  }

  private List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException {
    // construct serving vector
    List<Object> servingVector = new ArrayList<>();
    List<Future<List<Object>>> queryFutures = new ArrayList<>();

    for (Integer preparedStatementIndex : preparedQueryString.keySet()) {
      queryFutures.add(executorService.submit(() -> {
        try {
          return processQuery(entry, preparedStatementIndex);
        } catch (SQLException | FeatureStoreException | IOException e) {
          throw new RuntimeException(e);
        }
      }));
    }

    for (Future<List<Object>> queryFuture : queryFutures) {
      try {
        servingVector.add(queryFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new FeatureStoreException("Error retrieving query statement result", e);
      }
    }

    return servingVector;
  }

  private List<Object> processQuery(Map<String, Object> entry, int preparedStatementIndex)
      throws SQLException, FeatureStoreException, IOException {
    List<Object> servingVector = new ArrayList<>();
    try (Connection connection = hikariDataSource.getConnection()) {
      // Create the prepared statement
      PreparedStatement preparedStatement =
          connection.prepareStatement(preparedQueryString.get(preparedStatementIndex));

      // Set the parameters base do the entry object
      Map<String, Integer> parameterIndexInStatement = preparedStatementParameters.get(preparedStatementIndex);
      for (String name : entry.keySet()) {
        if (parameterIndexInStatement.containsKey(name)) {
          preparedStatement.setObject(parameterIndexInStatement.get(name), entry.get(name));
        }
      }

      ResultSet results = preparedStatement.executeQuery();
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
          if (datumReadersComplexFeatures.containsKey(results.getMetaData().getColumnName(index))) {
            servingVector.add(deserializeComplexFeature(datumReadersComplexFeatures, results, index));
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

  public List<List<Object>> getFeatureVectors(FeatureViewBase featureViewBase, Map<String, List<Object>> entry,
                                              boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    if (hikariDataSource == null || !isBatch) {
      initPreparedStatement(featureViewBase, true, external);
    }
    return getFeatureVectors(entry);
  }

  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    List<String> queries = Lists.newArrayList();
    for (Integer fgId : preparedQueryString.keySet()) {
      String query = preparedQueryString.get(fgId);
      String zippedTupleString =
          zipArraysToTupleString(preparedStatementParameters.get(fgId)
            .entrySet()
            .stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(e -> entry.get(e.getKey()))
            .collect(Collectors.toList()));
      queries.add(query.replaceFirst("\\?", zippedTupleString));
    }
    return getFeatureVectors(queries);
  }

  private List<List<Object>> getFeatureVectors(List<String> queries)
      throws SQLException, FeatureStoreException, IOException {
    ArrayList<Object> servingVector = new ArrayList<>();

    // construct batch of serving vectors
    // Create map object that will have of order of the vector as key and values as
    // vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
    // expect that backend will return correctly ordered vectors.
    Map<Integer, List<Object>> servingVectorsMap = new HashMap<>();

    try (Connection connection = hikariDataSource.getConnection()) {
      try (Statement stmt = connection.createStatement()) {
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
                if (datumReadersComplexFeatures.containsKey(results.getMetaData().getColumnName(index))) {
                  servingVector.add(deserializeComplexFeature(datumReadersComplexFeatures, results, index));
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
    }
    return new ArrayList<List<Object>>(servingVectorsMap.values());
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch);
  }

  public void initServing(FeatureViewBase featureViewBase, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureViewBase, batch, external);
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
      throw new FeatureStoreException("This feature view has transformation functions attached and "
          + "serving must performed from a Python application");
    }
    List<ServingPreparedStatement> servingPreparedStatements =
        featureViewApi.getServingPreparedStatement(featureViewBase, batch);
    initPreparedStatement(featureViewBase.getFeatureStore(), featureViewBase.getFeatures(),
        servingPreparedStatements, batch, external);
  }

  private void initPreparedStatement(FeatureStoreBase featureStoreBase,
                                     List<TrainingDatasetFeature> features,
                                     List<ServingPreparedStatement> servingPreparedStatements,
                                     boolean batch,
                                     boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {

    this.isBatch = batch;
    setupHikariPool(featureStoreBase, external);
    // map of prepared statement index and its corresponding parameter indices
    Map<Integer, TreeMap<String, Integer>> preparedStatementParameters = new HashMap<>();

    // in case its batch serving then we need to save sql string only
    TreeMap<Integer, String> preparedQueryString = new TreeMap<>();

    // save unique primary key names that will be used by user to retrieve serving vector
    HashSet<String> servingVectorKeys = new HashSet<>();
    for (ServingPreparedStatement servingPreparedStatement : servingPreparedStatements) {
      preparedQueryString.put(servingPreparedStatement.getPreparedStatementIndex(),
          servingPreparedStatement.getQueryOnline());
      TreeMap<String, Integer> parameterIndices = new TreeMap<>();
      servingPreparedStatement.getPreparedStatementParameters().forEach(preparedStatementParameter -> {
        servingVectorKeys.add(preparedStatementParameter.getName());
        parameterIndices.put(preparedStatementParameter.getName(), preparedStatementParameter.getIndex());
      });
      preparedStatementParameters.put(servingPreparedStatement.getPreparedStatementIndex(), parameterIndices);
    }
    this.servingKeys = servingVectorKeys;

    this.preparedStatementParameters = preparedStatementParameters;
    System.out.println(preparedQueryString);
    this.preparedQueryString = preparedQueryString;
    this.datumReadersComplexFeatures = getComplexFeatureSchemas(features);
  }

  private void setupHikariPool(FeatureStoreBase featureStoreBase, Boolean external) throws FeatureStoreException,
      IOException, SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");

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

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(jdbcOptions.get(Constants.JDBC_USER));
    config.setPassword(jdbcOptions.get(Constants.JDBC_PWD));
    hikariDataSource = new HikariDataSource(config);
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

  private Object deserializeComplexFeature(Map<String, DatumReader<Object>> complexFeatureSchemas, ResultSet results,
                                           int index) throws SQLException, IOException {
    if (results.getBytes(index) != null) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(results.getBytes(index), null);
      return complexFeatureSchemas.get(results.getMetaData().getColumnName(index)).read(null, decoder);
    } else {
      return null;
    }
  }

  private Map<String, DatumReader<Object>> getComplexFeatureSchemas(List<TrainingDatasetFeature> features)
      throws FeatureStoreException, IOException {
    Map<String, DatumReader<Object>> featureSchemaMap = new HashMap<>();
    for (TrainingDatasetFeature f : features) {
      if (f.isComplex()) {
        DatumReader<Object> datumReader =
            new GenericDatumReader<>(parser.parse(f.getFeaturegroup().getFeatureAvroSchema(f.getName())));
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

  public void close() {
    hikariDataSource.close();
    executorService.shutdown();
  }
}
