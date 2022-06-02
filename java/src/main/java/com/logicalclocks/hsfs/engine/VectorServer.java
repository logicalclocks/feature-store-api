package com.logicalclocks.hsfs.engine;

import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import com.logicalclocks.hsfs.util.Constants;
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
  private HashSet<String> servingKeys;
  private boolean isBatch = false;

  public VectorServer(boolean isBatch) {
    this.isBatch = isBatch;
  }

  public List<Object> getFeatureVector(TrainingDataset trainingDataset, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    // init prepared statement if it has not been already set. Or if a prepare statement for serving
    // single vector is needed.
    if (preparedStatements == null || isBatch) {
      initPreparedStatement(trainingDataset, false, external);
    }
    return getFeatureVector(trainingDataset.getFeatureStore(), trainingDataset.getFeatures(), entry, external);
  }

  public List<Object> getFeatureVector(FeatureView featureView, Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    if (preparedStatements == null || isBatch) {
      initPreparedStatement(featureView, false, external);
    }
    return getFeatureVector(featureView.getFeatureStore(), featureView.getFeatures(), entry, external);
  }

  private List<Object> getFeatureVector(FeatureStore featureStore, List<TrainingDatasetFeature> features,
      Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    refreshJdbcConnection(featureStore, external);
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

  public List<List<Object>> getFeatureVectors(TrainingDataset trainingDataset, Map<String, List<Object>> entry,
      boolean external) throws SQLException, FeatureStoreException, IOException,
      ClassNotFoundException {
    // init prepared statement if it has not already
    if (preparedStatements == null || !isBatch) {
      // size of batch of primary keys are required to be equal. Thus, we take size of batch for the 1st primary key if
      // it was not initialized from initPreparedStatement(batchSize)
      initPreparedStatement(trainingDataset, true, external);
    }
    return getFeatureVectors(trainingDataset.getFeatureStore(), trainingDataset.getFeatures(), entry, external);
  }

  public List<List<Object>> getFeatureVectors(FeatureView featureView, Map<String, List<Object>> entry,
      boolean external) throws SQLException, FeatureStoreException, IOException,
      ClassNotFoundException {
    if (preparedStatements == null || !isBatch) {
      initPreparedStatement(featureView, true, external);
    }
    return getFeatureVectors(featureView.getFeatureStore(), featureView.getFeatures(), entry, external);
  }

  private List<List<Object>> getFeatureVectors(FeatureStore featureStore, List<TrainingDatasetFeature> features,
      Map<String, List<Object>> entry, boolean external) throws SQLException, FeatureStoreException, IOException {
    checkPrimaryKeys(entry.keySet());
    List<String> queries = Lists.newArrayList();
    for (Integer fgId : preparedQueryString.keySet()) {
      String query = preparedQueryString.get(fgId);
      String zippedTupleString =
          zipArraysToTupleString(preparedStatementParameters.get(fgId).keySet().stream().map(entry::get)
              .collect(Collectors.toList()));
      queries.add(query.replaceFirst("\\?", zippedTupleString));
    }
    return getFeatureVectors(featureStore, features, queries, external);
  }

  private List<List<Object>> getFeatureVectors(FeatureStore featureStore, List<TrainingDatasetFeature> features,
      List<String> queries, boolean external) throws SQLException, FeatureStoreException, IOException {
    refreshJdbcConnection(featureStore, external);
    ArrayList<Object> servingVector = new ArrayList<>();

    Map<String, DatumReader<Object>> complexFeatureSchemas = getComplexFeatureSchemas(features);

    // construct batch of serving vectors
    // Create map object that will have of order of the vector as key and values as
    // vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
    // expect that backend will return correctly ordered vectors.
    Map<Integer, List<Object>> servingVectorsMap = new HashMap<>();

    Statement stmt = preparedStatementConnection.createStatement();
    for (String query : queries) {
      int orderInBatch = 0;

      // MySQL doesn't support setting array type on prepared statement. This is the hack to replace
      // the ? with array joined as comma separated array.
      ResultSet results = stmt.executeQuery(query);

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
      results.close();
    }
    return new ArrayList<List<Object>>(servingVectorsMap.values());
  }

  public List<List<Object>> previewFeatureVectors(FeatureView featureView,
      boolean external, int size) throws SQLException, FeatureStoreException, IOException,
      ClassNotFoundException {
    if (preparedStatements == null || !isBatch) {
      initPreparedStatement(featureView, true, external);
    }
    List<String> queries = Lists.newArrayList();
    for (Integer fgId : preparedQueryString.keySet()) {
      String query = preparedQueryString.get(fgId);
      queries.add(query.substring(0, query.indexOf("WHERE ")) + "LIMIT " + size);
    }
    return getFeatureVectors(featureView.getFeatureStore(), featureView.getFeatures(), queries, external);
  }

  public void initServing(TrainingDataset trainingDataset, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(trainingDataset, batch, external);
  }

  public void initServing(FeatureView featureView, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    initPreparedStatement(featureView, batch, external);
  }

  public void initPreparedStatement(TrainingDataset trainingDataset, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    // check if this training dataset has transformation functions attached and throw exception if any
    if (trainingDatasetApi.getTransformationFunctions(trainingDataset).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }
    List<ServingPreparedStatement> servingPreparedStatements =
        trainingDatasetApi.getServingPreparedStatement(trainingDataset, batch);
    initPreparedStatement(trainingDataset.getFeatureStore(), servingPreparedStatements, batch, external);
  }

  public void initPreparedStatement(FeatureView featureView, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    // check if this training dataset has transformation functions attached and throw exception if any
    if (featureViewApi.getTransformationFunctions(featureView).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "serving must performed from a Python application");
    }
    List<ServingPreparedStatement> servingPreparedStatements =
        featureViewApi.getServingPreparedStatement(featureView, batch);
    initPreparedStatement(featureView.getFeatureStore(), servingPreparedStatements, batch, external);
  }

  private void initPreparedStatement(FeatureStore featureStore,
      List<ServingPreparedStatement> servingPreparedStatements, boolean batch, boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");
    this.isBatch = batch;
    setupJdbcConnection(featureStore, external);
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

  private void setupJdbcConnection(FeatureStore featureStore, Boolean external) throws FeatureStoreException,
      IOException, SQLException {
    StorageConnector storageConnector =
        storageConnectorApi.getOnlineStorageConnector(featureStore);
    Map<String, String> jdbcOptions = storageConnector.sparkOptions();
    String url = jdbcOptions.get(Constants.JDBC_URL);
    if (external) {
      // if external is true, replace the IP coming from the storage connector with the host
      // used during the connection setup
      url = url.replaceAll("/[0-9.]+:", "/" + HopsworksClient.getInstance().getHost() + ":");
    }
    Connection jdbcConnection =
        DriverManager.getConnection(url, jdbcOptions.get(Constants.JDBC_USER), jdbcOptions.get(Constants.JDBC_PWD));
    preparedStatementConnection = jdbcConnection;
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

  private void refreshJdbcConnection(FeatureStore featureStore, Boolean external) throws FeatureStoreException,
      IOException, SQLException {
    if (!preparedStatementConnection.isValid(1)) {
      setupJdbcConnection(featureStore, external);
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
}
