# Feature

*Features* are the most granular entity in the [feature store](feature_store.md) and are
logically grouped by [feature groups](feature_group.md).
Features in the same feature groups are semantically related to the feature groups primary keys.

The storage location of a single feature is determined by the
[feature group](feature_group.md). Hence, enabling a [feature group](feature_group.md)
for online storage will make a feature available as an online feature.

## Features Taxonomy

Within a [feature group](feature_group.md) there are different categories of features:

* **Primary Keys**: These features describe the entity contained in a feature group (e.g. customer features, user features, ...). Within a feature group there could be more than one feature describing the primary key.
* **Event Time**: One feature within a feature group can be selected to identify the time at which the event for the given record has happened. The event time feature is going to be used to enforce Point-in-time correctness when joining features from different feature groups together. When storing feature data on the offline feature store, each record is uniquely identified by the union of the primary key features and the even time feature.
* **Partition Keys**: These features describe the storage layout of the feature group data on the offline feature store. Partition keys allow to divide the feature group data into different subdirectory. Partitioning can help to query more efficiently the data from the offline feature store.
* **Normal Features**: These features represent attributes of related to the primary keys.

## Feature Types

Each features has two different data types:

* **Offline type**: The data type of the feature when stored on the offline feature store
* **Online type**: The data type of the feature when stored on the online feature store.

The offline type is always required, even if the feature group is stored only online. On the other hand, if the feature group is not *online_enabled*, its features will not have an online type.

### Offline Storage

The offline feature store is based on Apache Hudi and Hive Metastore, as such, any
[Hive Data Type](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types)
can be leveraged.

Potential *offline* types are:

```SQL
"TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
"DECIMAL", "TIMESTAMP", "DATE", "STRING", "BOOLEAN", "BINARY",
"ARRAY <TINYINT>", "ARRAY <SMALLINT>", "ARRAY <INT>", "ARRAY <BIGINT>",
"ARRAY <FLOAT>", "ARRAY <DOUBLE>", "ARRAY <DECIMAL>", "ARRAY <TIMESTAMP>",
"ARRAY <DATE>", "ARRAY <STRING>",
"ARRAY <BOOLEAN>", "ARRAY <BINARY>", "ARRAY <ARRAY <FLOAT> >",
"ARRAY <ARRAY <INT> >", "ARRAY <ARRAY <STRING> >",
"MAP <FLOAT, FLOAT>", "MAP <FLOAT, STRING>", "MAP <FLOAT, INT>",
"MAP <FLOAT, BINARY>", "MAP <INT, INT>", "MAP <INT, STRING>",
"MAP <INT, BINARY>", "MAP <INT, FLOAT>", "MAP <INT, ARRAY <FLOAT> >",
"STRUCT < label: STRING, index: INT >", "UNIONTYPE < STRING, INT>"
```

### Online Storage

The online storage is based on RonDB and hence, any
[RonDB Data Type](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)
can be leveraged.

Potential *online* types are:

```SQL
"None", "INT(11)", "TINYINT(1)", "SMALLINT(5)", "MEDIUMINT(7)", "BIGINT(20)",
"FLOAT", "DOUBLE", "DECIMAL", "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR",
"CHAR", "VARCHAR(n)", "BINARY", "VARBINARY(n)", "BLOB", "TEXT", "TINYBLOB",
"TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT", "JSON"
```

#### Complex online types

Additionally to the *online* types above, Hopsworks allows users to store complex types (e.g. *ARRAY<INT>*) on the online feature store.
Hopsworks will take care of transparently serializing the complex features and storing them as *VARBINARY* on the online feature store. The serialization happens when calling the [save()](../api/feature_group_api/#save), [insert()](../api/feature_group_api/#insert) or [insert_stream()](../api/feature_group_api/#insert_stream) methods. The deserialization will be executed when calling the [get_serving_vector()](../api/training_dataset_api/#get_serving_vector) method to retrieve data from the online feature store.
If users query directly the online feature store, for instance using the `fs.sql("SELECT ...", online=True)` statement, they will receive back a binary blob.

On the feature store UI, the online feature type for complex features will be reported as *VARBINARY*.

#### Online restrictions for primary key types:

When a feature is being used as a primary key, certain types are not allowed. Examples of such types are *Float*, *Double*, *Date*, *Text*, *Blob* and *Complex Types*  (e.g. Array<>). Additionally the size of the sum of the primary key online types storage requirements should not exceed 3KB.

### Type Inference

The offline and online types for each feature are inferred automatically when calling the [save()](../api/feature_group_api/#save) method to create a feature group. The types will be inferred based on the types of the Spark or Pandas DataFrame.

In the case of Spark DataFrame, the [Spark types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) will be converted in the equivalent Hive Metastore type and used as offline feature store type. If the feature group is online enabled, Hopsworks will then match the offline type to the corresponding online type. The matching is performed as following:

* If the offline type is supported also on the online feature store (e.g. INT, FLOAT, DATE, TIMESTAMP), the online type will be equivalent to the offline type
* If the offline type is *boolean*, the online type is going to be set as *tinyint*
* If the offline type is *string*, the online type is going to be set as *varchar(100)*
* If the offline type is not supported by the online feature store and it is not one of the above exception, the online type will be set as *varbinary(100)* to handle complex types.

#### Pandas Conversion

When registering a [Pandas](https://pandas.pydata.org/) DataFrame as feature group, the following conversion is applied:

| Pandas Type        | Offline Feature Type|
| ------------------ | ------------------- |
| int32              | INT                 |
| int64              | BIGINT              |
| float32            | FLOAT               |
| float64            | DOUBLE              |
| datetime64[ns]     | TIMESTAMP           |
| object             | STRING              |


### Type Overwrite

When creating a feature group it is possible for the user to control both the offline and online type of each feature. If users manually specify a schema for the feature group, Hopsworks is going to use it to create the feature group, without performing any type inference.
Users can manually define the feature group schema as follow:

```python
from hsfs.feature import Feature

features = [
    Feature(name="id",type="int",online_type="int"),
    Feature(name="name",type="string",online_type="varchar(20)")
]

fg = fs.create_feature_group(name="fg_manual_schema",
                             features=features,
                             online_enabled=True)
fg.save(df)
```

{{feature}}

## Properties

{{feature_properties}}

## Methods

{{feature_methods}}
